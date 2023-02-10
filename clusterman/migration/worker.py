# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import signal
import time
from functools import partial
from multiprocessing import Process
from multiprocessing.synchronize import Lock as LockBase
from statistics import mean
from typing import Any
from typing import Callable
from typing import cast
from typing import Collection
from typing import Tuple

import colorlog

from clusterman.autoscaler.offset import remove_capacity_offset
from clusterman.autoscaler.offset import set_capacity_offset
from clusterman.autoscaler.pool_manager import AWS_RUNNING_STATES
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.autoscaler.toggle import disable_autoscaling
from clusterman.autoscaler.toggle import enable_autoscaling
from clusterman.draining.queue import TerminationReason
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.migration.constants import INITIAL_POOL_HEALTH_TIMEOUT_SECONDS
from clusterman.migration.constants import SFX_DRAINED_NODE_UPTIME
from clusterman.migration.constants import SFX_MIGRATION_JOB_DURATION
from clusterman.migration.constants import SFX_NODE_DRAIN_COUNT
from clusterman.migration.constants import SUPPORTED_POOL_SCHEDULER
from clusterman.migration.constants import UPTIME_CHECK_INTERVAL_SECONDS
from clusterman.migration.event import MigrationEvent
from clusterman.migration.settings import WorkerSetup
from clusterman.monitoring_lib import get_monitoring_client
from clusterman.util import limit_function_runtime


module_logger = colorlog.getLogger(__name__)


class RestartableDaemonProcess:
    def __init__(self, target, args, kwargs, initial_restart_count: int = 0) -> None:
        self.__target = partial(self._sigterm_wrapper, target)
        self.__args = args
        self.__kwargs = kwargs
        self.restart_count = initial_restart_count
        self._init_proc_handle()

    @staticmethod
    def _sigterm_wrapper(target: Callable, *args, **kwargs) -> Any:
        signal.signal(signal.SIGTERM, RestartableDaemonProcess._sigterm_handler)
        return target(*args, **kwargs)

    @staticmethod
    def _sigterm_handler(sig, frame):
        raise JobTerminationSignal()

    def _init_proc_handle(self):
        self.process_handle = Process(target=self.__target, args=self.__args, kwargs=self.__kwargs)
        self.process_handle.daemon = True

    def restart(self):
        if self.process_handle.is_alive():
            self.process_handle.kill()
        self._init_proc_handle()
        self.process_handle.start()
        self.restart_count += 1

    def __getattr__(self, attr):
        return getattr(self.process_handle, attr)


class NodeMigrationError(Exception):
    pass


class JobTerminationSignal(RuntimeError):
    pass


def _monitor_pool_health(
    manager: PoolManager,
    timeout: float,
    drained: Collection[ClusterNodeMetadata],
    health_check_interval_seconds: int,
    ignore_pod_health: bool = False,
    orphan_capacity_tollerance: float = 0,
) -> Tuple[bool, Collection[ClusterNodeMetadata]]:
    """Monitor pool health after nodes were submitted for draining

    :param PoolManager manager: pool manager instance
    :param float timeout: timestamp after which giving up
    :param Collection[ClusterNodeMetadata] drained: nodes which were submitted for draining
    :param int health_check_interval_seconds: how often to iterate the check
    :param bool ignore_pod_health: If set, do not check that pods can successfully be scheduled
    :param float orphan_capacity_tollerance: acceptable ratio of orphan capacity to still consider check satisfied
    :return: tuple of health status, and nodes failing to drain
    """
    still_to_drain = []
    logger = module_logger.getChild(manager.pool)
    draining_happened, capacity_satisfied, pods_healthy = False, False, False
    connector = cast(KubernetesClusterConnector, manager.cluster_connector)
    logger.info(f"Monitoring health for {manager.cluster}:{manager.pool}")
    while time.time() < timeout:
        manager.reload_state(load_pods_info=not ignore_pod_health)
        still_to_drain = (
            [node for node in drained if manager.is_node_still_in_pool(node)] if not draining_happened else []
        )
        draining_happened = draining_happened or not bool(still_to_drain)
        # TODO: replace these with use of walrus operator in if-statement once on py38
        capacity_satisfied = capacity_satisfied or (
            draining_happened and manager.is_capacity_satisfied(orphan_capacity_tollerance)
        )
        pods_healthy = pods_healthy or (
            draining_happened and (ignore_pod_health or connector.has_enough_capacity_for_pods())
        )
        if draining_happened and capacity_satisfied and pods_healthy:
            return True, still_to_drain
        else:
            logger.info(
                f"Pool {manager.cluster}:{manager.pool} not healthy yet"
                f" (drain_ok={draining_happened}, capacity_ok={capacity_satisfied}, pods_ok={pods_healthy})"
            )
        time.sleep(health_check_interval_seconds)
    return False, still_to_drain


def _drain_node_selection(
    manager: PoolManager, selector: Callable[[ClusterNodeMetadata], bool], worker_setup: WorkerSetup
) -> bool:
    """Drain nodes in pool according to selection criteria

    :param PoolManager manager: pool manager instance
    :param Callable[[ClusterNodeMetadata], bool] selector: selection filter
    :param WorkerSetup worker_setup: node migration setup
    :return: true if completed
    """
    logger = module_logger.getChild(manager.pool)
    nodes = manager.get_node_metadatas(AWS_RUNNING_STATES)
    selected = sorted(filter(selector, nodes), key=worker_setup.precedence.sort_key)
    if not selected:
        return True
    monitoring_info = {"cluster": manager.cluster, "pool": manager.pool}
    node_drain_counter = get_monitoring_client().create_counter(SFX_NODE_DRAIN_COUNT, monitoring_info)
    job_timer = get_monitoring_client().create_timer(SFX_MIGRATION_JOB_DURATION, monitoring_info)
    node_uptime_gauge = get_monitoring_client().create_gauge(SFX_DRAINED_NODE_UPTIME, monitoring_info)
    chunk = worker_setup.rate.of(len(nodes))
    n_requeued_nodes, i, selection_size = 0, 0, len(selected)
    logger.info(f"{selection_size} nodes of {manager.cluster}:{manager.pool} will be recycled")
    job_timer.start()
    while i < len(selected):
        start_time = time.time()
        selection_chunk = selected[i : i + chunk]
        for node in selection_chunk:
            logger.info(f"Recycling node {node.instance.instance_id}")
            manager.submit_for_draining(node, TerminationReason.NODE_MIGRATION)
            node_uptime_gauge.set(node.instance.uptime.total_seconds())
            node_drain_counter.count()
        time.sleep(worker_setup.bootstrap_wait)
        is_healthy, still_to_drain = _monitor_pool_health(
            manager=manager,
            timeout=start_time + worker_setup.bootstrap_timeout,
            drained=selection_chunk,
            health_check_interval_seconds=worker_setup.health_check_interval,
            ignore_pod_health=worker_setup.ignore_pod_health,
            orphan_capacity_tollerance=worker_setup.orphan_capacity_tollerance,
        )
        if not is_healthy:
            if still_to_drain and len(still_to_drain) + n_requeued_nodes <= worker_setup.allowed_failed_drains:
                n_requeued_nodes += len(still_to_drain)
                selected.extend(still_to_drain)
            else:
                logger.warning(
                    f"Pool {manager.cluster}:{manager.pool} did not come back"
                    " to desired capacity, stopping selection draining"
                )
                job_timer.stop()
                return False
        logger.info(
            f"Recycled {min(i + chunk - n_requeued_nodes, selection_size)} nodes out of {selection_size} selected"
        )
        i += len(selection_chunk)
    logger.info(f"Completed recycling node selection from {manager.cluster}:{manager.pool}")
    job_timer.stop()
    return True


def uptime_migration_worker(
    cluster: str, pool: str, uptime_seconds: int, worker_setup: WorkerSetup, pool_lock: LockBase
) -> None:
    """Worker monitoring and migrating nodes according to uptime

    :parma str cluster: cluster name
    :param str pool: pool name
    :param int uptime_seconds: uptime threshold
    :param WorkerSetup worker_setup: migration setup
    """
    skipped_executions = 0
    logger = module_logger.getChild(pool)
    manager = PoolManager(cluster, pool, SUPPORTED_POOL_SCHEDULER, fetch_state=False)
    node_selector = lambda node: node.instance.uptime.total_seconds() > uptime_seconds  # noqa
    if not manager.draining_client:
        logger.warning(f"Draining client not set up for {cluster}:{pool}, giving up")
        return
    try:
        while True:
            manager.reload_state(load_pods_info=not worker_setup.ignore_pod_health)
            if (
                worker_setup.max_uptime_worker_skips > 0 and skipped_executions > worker_setup.max_uptime_worker_skips
            ) or manager.is_capacity_satisfied(worker_setup.orphan_capacity_tollerance):
                with pool_lock:
                    _drain_node_selection(manager, node_selector, worker_setup)
                skipped_executions = 0
            else:
                skipped_executions += 1
                logger.warning(
                    f"Pool {cluster}:{pool} is currently underprovisioned, skipping uptime migration iteration"
                )
            time.sleep(UPTIME_CHECK_INTERVAL_SECONDS)
    except JobTerminationSignal:
        logger.warning("Received termination signal")
    except Exception as e:
        logger.error(f"Issue while running uptime worker: {e}")
        raise


def event_migration_worker(migration_event: MigrationEvent, worker_setup: WorkerSetup, pool_lock: LockBase) -> None:
    """Worker migrating nodes according to event configuration

    :param MigrationEvent migration_event: event instance
    :param WorkerSetup worker_setup: migration setup
    """
    pool_lock_acquired = False
    logger = module_logger.getChild(migration_event.pool)
    manager = PoolManager(migration_event.cluster, migration_event.pool, SUPPORTED_POOL_SCHEDULER, fetch_state=False)
    connector = cast(KubernetesClusterConnector, manager.cluster_connector)
    connector.set_label_selectors(migration_event.label_selectors, add_to_existing=True)
    manager.reload_state(load_pods_info=not worker_setup.ignore_pod_health)
    try:
        pool_lock.acquire(timeout=worker_setup.expected_duration)
        pool_lock_acquired = True
        if worker_setup.disable_autoscaling:
            logger.info(f"Disabling autoscaling for {migration_event.cluster}:{migration_event.pool}")
            disable_autoscaling(
                migration_event.cluster,
                migration_event.pool,
                SUPPORTED_POOL_SCHEDULER,
                time.time() + worker_setup.expected_duration,
            )
        if worker_setup.prescaling:
            nodes = manager.get_node_metadatas(AWS_RUNNING_STATES)
            offset = worker_setup.prescaling.of(len(nodes))
            logger.info(f"Applying pre-scaling of {offset} node to {migration_event.cluster}:{migration_event.pool}")
            capacity_offset = offset * mean(node.instance.weight for node in nodes)
            if worker_setup.disable_autoscaling:
                prescaled_capacity = round(manager.target_capacity + capacity_offset)
                manager.modify_target_capacity(prescaled_capacity)
            else:
                set_capacity_offset(
                    migration_event.cluster,
                    migration_event.pool,
                    SUPPORTED_POOL_SCHEDULER,
                    time.time() + worker_setup.expected_duration,
                    capacity_offset,
                )
        if not _monitor_pool_health(
            manager=manager,
            timeout=time.time() + INITIAL_POOL_HEALTH_TIMEOUT_SECONDS,
            drained=[],
            health_check_interval_seconds=worker_setup.health_check_interval,
            ignore_pod_health=True,
            orphan_capacity_tollerance=worker_setup.orphan_capacity_tollerance,
        ):
            raise NodeMigrationError(f"Pool {migration_event.cluster}:{migration_event.pool} is not healthy")
        node_selector = lambda node: node.agent.agent_id and not migration_event.matches(node)  # noqa
        migration_routine = partial(_drain_node_selection, manager, node_selector, worker_setup)
        if not limit_function_runtime(migration_routine, worker_setup.expected_duration):
            raise NodeMigrationError(f"Failed migrating nodes for event {migration_event}")
    except JobTerminationSignal:
        logger.warning("Received termination signal")
    except Exception as e:
        logger.error(f"Issue while processing migration event {migration_event}: {e}")
        raise
    finally:
        if pool_lock_acquired:
            pool_lock.release()
        # we do not reset the pool target capacity in case of direct capacity changes
        # as we trust the autoscaler to readjust that in a short time eventually
        if worker_setup.disable_autoscaling:
            logger.info(f"Re-enabling autoscaling for {migration_event.cluster}:{migration_event.pool}")
            enable_autoscaling(migration_event.cluster, migration_event.pool, SUPPORTED_POOL_SCHEDULER)
        elif worker_setup.prescaling:
            remove_capacity_offset(
                migration_event.cluster,
                migration_event.pool,
                SUPPORTED_POOL_SCHEDULER,
            )
