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
import argparse
import logging
import time
from collections import defaultdict
from functools import lru_cache
from multiprocessing import Lock
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

import arrow
import colorlog
import staticconf
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.config import get_pool_config_path
from clusterman.config import load_cluster_pool_config
from clusterman.config import POOL_NAMESPACE
from clusterman.config import setup_config
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.migration.constants import SUPPORTED_POOL_SCHEDULER
from clusterman.migration.event import load_timespan_target
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import MigrationStatus
from clusterman.migration.settings import WorkerSetup
from clusterman.migration.worker import event_migration_worker
from clusterman.migration.worker import RestartableDaemonProcess
from clusterman.migration.worker import uptime_migration_worker
from clusterman.util import get_pool_name_list
from clusterman.util import setup_logging


class NodeMigration(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ["compute-infra@yelp.com"]

    POOL_SETTINGS_PARENT = "node_migration"
    MIN_UPTIME_CHURNING_SECONDS = 60 * 60 * 24  # 1 day
    DEFAULT_MAX_WORKER_PROCESSES = 6
    DEFAULT_RUN_INTERVAL_SECONDS = 60
    DEFAULT_TERMINATION_TIMEOUT_SECONDS = 10
    DEFAULT_FAILED_ATTEMPTS_MARGIN = 5

    WORKER_LABEL_SEPARATOR = ":"
    EVENT_WORKER_LABEL_PREFIX = "event"
    UPTIME_WORKER_LABEL_PREFIX = "uptime"

    @batch_command_line_arguments
    def parse_args(self, parser: argparse.ArgumentParser):
        arg_group = parser.add_argument_group("NodeMigration batch options")
        add_env_config_path_arg(arg_group)
        add_cluster_arg(arg_group, required=True)
        parser.add_argument(
            "--extra-logs",
            action="store_true",
            help="Show all logs from pool manager, cluster connector and resource groups",
        )

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self._silence_unneeded_logging()
        self.logger = colorlog.getLogger(__name__)
        self.migration_workers: Dict[str, RestartableDaemonProcess] = {}
        self.migration_configs: Dict[str, dict] = {}
        self.events_in_progress: Dict[str, MigrationEvent] = {}
        self.pools_accepting_events: Set[str] = set()
        self.worker_locks = defaultdict(Lock)
        self.cluster_connector = KubernetesClusterConnector(self.options.cluster, None, init_crd=True)
        self.run_interval = staticconf.read_int(
            "batches.node_migration.run_interval_seconds", self.DEFAULT_RUN_INTERVAL_SECONDS
        )
        self.available_worker_slots = staticconf.read_float(
            "batches.node_migration.max_worker_processes", self.DEFAULT_MAX_WORKER_PROCESSES
        )
        self.worker_termination_timeout = staticconf.read_int(
            "batches.node_migration.worker_termination_timeout_seconds", self.DEFAULT_TERMINATION_TIMEOUT_SECONDS
        )
        self.failed_attemps_margin = staticconf.read_int(
            "batches.node_migration.failed_attemps_margin",
            self.DEFAULT_FAILED_ATTEMPTS_MARGIN,
        )
        for pool in get_pool_name_list(self.options.cluster, SUPPORTED_POOL_SCHEDULER):
            self.add_watcher({pool: get_pool_config_path(self.options.cluster, pool, SUPPORTED_POOL_SCHEDULER)})
            load_cluster_pool_config(self.options.cluster, pool, SUPPORTED_POOL_SCHEDULER, None)
            pool_config_namespace = POOL_NAMESPACE.format(pool=pool, scheduler=SUPPORTED_POOL_SCHEDULER)
            pool_config = staticconf.config.get_namespace(pool_config_namespace).get_config_dict()
            if self.POOL_SETTINGS_PARENT in pool_config:
                self.migration_configs[pool] = pool_config[self.POOL_SETTINGS_PARENT]
                if self.migration_configs[pool]["trigger"].get("event", False):
                    self.pools_accepting_events.add(pool)
        self.logger.info(f"Found node migration configs for pools: {list(self.migration_configs.keys())}")

    def _silence_unneeded_logging(self):
        """Set higher level for a selection of log handler to reduce overall noise"""
        modules_to_silence = (
            [
                "clusterman.autoscaler.pool_manager",
                "clusterman.aws.spot_fleet_resource_group",
                "clusterman.aws.auto_scaling_resource_group",
                "clusterman.kubernetes.kubernetes_cluster_connector",
            ]
            if not self.options.extra_logs
            else []
        )
        for module in modules_to_silence:
            colorlog.getLogger(module).setLevel(logging.WARNING)

    @lru_cache()
    def _get_worker_setup(self, pool: str) -> Optional[WorkerSetup]:
        """Build worker setup for

        :param str pool: name of the pool
        :return: migration worker setup object
        """
        try:
            if pool in self.migration_configs:
                return WorkerSetup.from_config(self.migration_configs[pool])
        except Exception as e:
            self.logger.exception(f"Bad migration configuration for pool {pool}: {e}")
        return None

    def _build_worker_label(self, pool: Optional[str] = None, event: Optional[MigrationEvent] = None) -> str:
        """Composes label for worker process

        :param Optional[str] pool: pool name in case of uptime worker label
        :param Optional[MigrationEvent] event: event instance in case of event worker label
        :return: label string
        """
        if event:
            prefix, cluster, pool = self.EVENT_WORKER_LABEL_PREFIX, event.cluster, event.pool
        elif pool:
            prefix, cluster = self.UPTIME_WORKER_LABEL_PREFIX, self.options.cluster
        else:
            raise ValueError("Either 'pool' or 'event' must be provided as parameter")
        return self.WORKER_LABEL_SEPARATOR.join((prefix, cluster, pool))

    def _is_event_worker_label(self, label: str) -> bool:
        """Check if process label is for an event worker"""
        return label.startswith(self.EVENT_WORKER_LABEL_PREFIX)

    def _get_worker_lock_key(self, label: str) -> str:
        """Extract process lock key from process label"""
        return label.split(self.WORKER_LABEL_SEPARATOR, 1)[1]

    def _spawn_worker(self, label: str, routine: Callable, *args, **kwargs) -> bool:
        """Start worker process

        :param str label: label for the worker process
        :param Callable routine: worker method
        :param int initial_restart_count: initial number of attempts this event already had (kwargs only)
        :param *args: method positional argument
        :param **kwargs: method keyword arguments
        :return: whether the worker process was spawned
        """
        if label in self.migration_workers and self.migration_workers[label].is_alive():
            self.logger.warning(f"Worker labelled {label} already running, skipping")
            return False
        running_workers = sum(proc.is_alive() for proc in self.migration_workers.values())
        if self._is_event_worker_label(label) and running_workers >= self.available_worker_slots:
            # uptime workers are prioritized skipping this check
            self.logger.warning(f"Too many worker processes running already ({running_workers}), skipping")
            return False
        kwargs["pool_lock"] = self.worker_locks[self._get_worker_lock_key(label)]
        initial_restart_count = kwargs.pop("initial_restart_count", 0)
        proc = RestartableDaemonProcess(
            target=routine, args=args, kwargs=kwargs, initial_restart_count=initial_restart_count
        )
        self.migration_workers[label] = proc
        proc.start()
        return True

    def _is_event_past_adjusted_time_estimate(self, event: MigrationEvent) -> bool:
        """Check if duration of migration event job is higher than its time estimate
        multiplied by the margin for failed attempts

        :param MigrationEvent event: migration event to be checked
        :return: boolean check outcome
        """
        worker_settings = self._get_worker_setup(event.pool)
        return (
            # let check pass if some information is missing
            not (event.created and worker_settings)
            or (
                (arrow.now() - event.created).total_seconds()
                > (self.failed_attemps_margin * worker_settings.expected_duration)
            )
        )

    def fetch_event_crd(self, statuses: List[MigrationStatus]) -> Collection[MigrationEvent]:
        """Fetch migration events from Kubernetes CRDs

        :param List[MigrationStatus] statuses: event status to look for
        :return: collection of migration events
        """
        self.cluster_connector.reload_client()
        return self.cluster_connector.list_node_migration_resources(statuses, self.failed_attemps_margin)

    def fetch_events_to_process(self) -> Collection[MigrationEvent]:
        """Fetch migration events needing to be processed"""
        events = self.fetch_event_crd([MigrationStatus.PENDING, MigrationStatus.INPROGRESS])
        return set(events) - set(self.events_in_progress.values())

    def mark_event(
        self, event: MigrationEvent, status: MigrationStatus = MigrationStatus.COMPLETED, attempts: Optional[int] = None
    ) -> None:
        """Set status for CRD event resource

        :param MigrationEvent event: event to be marked
        :param MigrationStatus status: status to be set
        """
        self.cluster_connector.mark_node_migration_resource(event.resource_name, status, attempts)

    def spawn_event_worker(self, event: MigrationEvent):
        """Start process recycling nodes in a pool accordingly to some event parameters

        :param MigrationEvent event: Event data
        """
        if event.pool not in self.pools_accepting_events:
            self.logger.warning(f"Pool {event.pool} not configured to accept migration trigger event, skipping")
            self.mark_event(event, MigrationStatus.SKIPPED)
            return
        worker_setup = self._get_worker_setup(event.pool)
        if not worker_setup or event.cluster != self.options.cluster:
            self.logger.warning(f"Event not processable by this batch instance, skipping: {event}")
            self.mark_event(event, MigrationStatus.SKIPPED)
            return
        self.logger.info(f"Spawning migration worker for event: {event}")
        worker_label = self._build_worker_label(event=event)
        if self._spawn_worker(
            label=worker_label,
            routine=event_migration_worker,
            initial_restart_count=event.previous_attempts,
            migration_event=event,
            worker_setup=worker_setup,
        ):
            self.mark_event(event, MigrationStatus.INPROGRESS)
            self.events_in_progress[worker_label] = event

    def spawn_uptime_worker(self, pool: str, uptime: Union[int, str]):
        """Start process monitoring pool node uptime, and recycling nodes accordingly

        :param str pool: name of the pool
        """
        uptime_seconds = load_timespan_target(str(uptime))
        if uptime_seconds < self.MIN_UPTIME_CHURNING_SECONDS:
            self.logger.warning(
                f"Node migration uptime trigger too low (<{self.MIN_UPTIME_CHURNING_SECONDS} seconds)."
                " Skipping worker bootstrapping."
            )
            return
        worker_setup = self._get_worker_setup(pool)
        if not worker_setup:
            # this can only happen with bad config, which gets logged already
            return
        self.logger.info(f"Spawning uptime migration worker for {pool} pool")
        self._spawn_worker(
            label=self._build_worker_label(pool=pool),
            routine=uptime_migration_worker,
            cluster=self.options.cluster,
            pool=pool,
            uptime_seconds=uptime_seconds,
            worker_setup=worker_setup,
        )

    def monitor_workers(self):
        """Check health of migration worker processes"""
        completed, torestart = [], []
        for label, proc in self.migration_workers.items():
            if not proc.is_alive():
                if proc.exitcode == 0:
                    completed.append(label)
                else:
                    torestart.append((label, proc.exitcode))
        for label in completed:
            self.logger.info(f"Worker process with label {label} completed")
            if self._is_event_worker_label(label):
                event = self.events_in_progress.pop(label)
                self.mark_event(event, MigrationStatus.COMPLETED)
            del self.migration_workers[label]
        for label, exitcode in torestart:
            if self._is_event_worker_label(label):
                event = self.events_in_progress[label]
                attempts = self.migration_workers[label].restart_count + 1
                # for job to be marked as failed, both the number of attempts and the multiplied
                # time estimate for it should be surpassed.
                if attempts >= self.failed_attemps_margin and self._is_event_past_adjusted_time_estimate(event):
                    self.mark_event(event, MigrationStatus.FAILED, attempts)
                    del self.migration_workers[label]
                    del self.events_in_progress[label]
                    continue
                self.mark_event(event, MigrationStatus.INPROGRESS, attempts)
            self.logger.info(f"Restarting worker process with label {label} (exit code: {exitcode})")
            self.migration_workers[label].restart()

    def terminate_workers(self, labels: Optional[Set[str]] = None):
        """Stop worker processes

        :param Optional[Set[str]] labels: list of process labels to select for termination (all by default)
        """
        self.logger.info(f"Terminating {labels or 'all'} worker processes")
        selected_workers = [entry for entry in self.migration_workers.items() if labels is None or entry[0] in labels]
        for _, proc in selected_workers:
            proc.terminate()
        for label, proc in selected_workers:
            proc.join(self.worker_termination_timeout)
            if proc.exitcode is None:
                self.logger.warning(f"Timed out terminating worker {label}, sending kill signal")
                proc.kill()
            del self.migration_workers[label]
            try:
                self.worker_locks[self._get_worker_lock_key(label)].release()
            except ValueError:
                # may have been released already, hence raising an exception, we just want to be sure
                pass

    def handle_stopped_jobs(self):
        """Stops workers for events requesting to do so"""
        for event in self.fetch_event_crd([MigrationStatus.STOP]):
            label = self._build_worker_label(event=event)
            if label in self.events_in_progress:
                self.terminate_workers({label})
                del self.events_in_progress[label]
            self.mark_event(event, MigrationStatus.SKIPPED)

    def run(self):
        for pool, config in self.migration_configs.items():
            if "max_uptime" in config["trigger"]:
                self.spawn_uptime_worker(pool, config["trigger"]["max_uptime"])
        while self.running:
            events = self.fetch_events_to_process()
            for event in events:
                self.spawn_event_worker(event)
            self.handle_stopped_jobs()
            time.sleep(self.run_interval)
            self.monitor_workers()
        self.terminate_workers()


if __name__ == "__main__":
    setup_logging()
    NodeMigration().start()
