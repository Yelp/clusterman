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
import time
from multiprocessing import Process
from typing import Callable
from typing import cast
from typing import Collection

import colorlog

from clusterman.autoscaler.pool_manager import AWS_RUNNING_STATES
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.migration.event import MigrationEvent
from clusterman.migration.settings import WorkerSetup


logger = colorlog.getLogger(__name__)
UPTIME_CHECK_INTERVAL_SECONDS = 60 * 60  # 1 hour
HEALTH_CHECK_INTERVAL_SECONDS = 60
SUPPORTED_POOL_SCHEDULER = "kubernetes"


class RestartableDaemonProcess:
    def __init__(self, target, args, kwargs) -> None:
        self.__target = target
        self.__args = args
        self.__kwargs = kwargs
        self._init_proc_handle()

    def _init_proc_handle(self):
        self.process_handle = Process(target=self.__target, args=self.__args, kwargs=self.__kwargs)
        self.process_handle.daemon = True

    def restart(self):
        if self.process_handle.is_alive():
            self.process_handle.kill()
        self._init_proc_handle()
        self.process_handle.start()

    def __getattr__(self, attr):
        return getattr(self.process_handle, attr)


def _monitor_pool_health(manager: PoolManager, timeout: float, drained: Collection[ClusterNodeMetadata]) -> bool:
    """Monitor pool health after nodes were submitted for draining

    :param PoolManager manager: pool manager instance
    :param float timeout: timestamp after which giving up
    :param Collection[ClusterNodeMetadata] drained: nodes which were submitted for draining
    :return: true if capacity is fulfilled
    """
    draining_happened = False
    connector = cast(KubernetesClusterConnector, manager.cluster_connector)
    while time.time() < timeout:
        manager.reload_state()
        draining_happened = draining_happened or not any(
            node.agent.agent_id == connector.get_agent_metadata(node.instance.ip_address).agent_id for node in drained
        )
        if draining_happened and manager.is_capacity_satisfied() and not connector.get_unschedulable_pods():
            return True
        time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)
    return False


def _drain_node_selection(
    manager: PoolManager, selector: Callable[[ClusterNodeMetadata], bool], worker_setup: WorkerSetup
) -> bool:
    """Drain nodes in pool according to selection criteria

    :param PoolManager manager: pool manager instance
    :param Callable[[ClusterNodeMetadata], bool] selector: selection filter
    :param WorkerSetup worker_setup: node migration setup
    :return: true if completed
    """
    nodes = manager.get_node_metadatas(AWS_RUNNING_STATES)
    selected = sorted(filter(selector, nodes), key=worker_setup.precedence.sort_key)
    chunk = worker_setup.rate.of(len(nodes))
    for i in range(0, len(selected), chunk):
        start_time = time.time()
        selection_chunk = selected[i : i + chunk]
        for node in selection_chunk:
            manager.submit_for_draining(node)
        time.sleep(worker_setup.bootstrap_wait)
        if not _monitor_pool_health(manager, start_time + worker_setup.bootstrap_timeout, selection_chunk):
            logger.warning(
                f"Pool {manager.cluster}:{manager.pool} did not come back"
                " to desired capacity, stopping selection draining"
            )
            return False
    return True


def uptime_migration_worker(cluster: str, pool: str, uptime_seconds: int, worker_setup: WorkerSetup) -> None:
    """Worker monitoring and migrating nodes according to uptime

    :parma str cluster: cluster name
    :param str pool: pool name
    :param int uptime_seconds: uptime threshold
    :param WorkerSetup worker_setup: migration setup
    """
    manager = PoolManager(cluster, pool, SUPPORTED_POOL_SCHEDULER)
    node_selector = lambda node: node.instance.uptime.total_seconds() > uptime_seconds  # noqa
    if not manager.draining_client:
        logger.warning(f"Draining client not set up for {cluster}:{pool}, giving up")
        return
    while True:
        if manager.is_capacity_satisfied():
            _drain_node_selection(manager, node_selector, worker_setup)
        else:
            logger.warning(f"Pool {cluster}:{pool} is currently underprovisioned, skipping uptime migration iteration")
        time.sleep(UPTIME_CHECK_INTERVAL_SECONDS)
        manager.reload_state()


def event_migration_worker(migration_event: MigrationEvent, worker_setup: WorkerSetup) -> None:
    """Worker migrating nodes according to event configuration

    :param MigrationEvent migration_event: event instance
    :param WorkerSetup worker_setup: migration setup
    """
    pass  # TODO
