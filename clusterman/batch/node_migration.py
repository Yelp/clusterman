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
from time import time
from typing import Collection
from typing import Optional

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
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import MigrationStatus
from clusterman.migration.settings import WorkerSetup
from clusterman.util import get_pool_name_list
from clusterman.util import setup_logging


class NodeMigration(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ["compute-infra@yelp.com"]

    POOL_SCHEDULER = "kubernetes"
    POOL_SETTINGS_PARENT = "node_migration"

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group("NodeMigration batch options")
        add_env_config_path_arg(arg_group)
        add_cluster_arg(arg_group, required=True)

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self.logger = colorlog.getLogger(__name__)
        self.migration_workers = {}
        self.migration_configs = {}
        self.events_in_progress = set()
        self.pools_accepting_events = set()
        self.cluster_connector = KubernetesClusterConnector(self.options.cluster, init_crd=True)
        self.run_interval = staticconf.read_int("batches.node_migration.run_interval_seconds", 60)
        self.event_visibilty_timeout = staticconf.read_int(
            "batches.node_migration.event_visibilty_timeout_seconds", 15 * 60
        )
        for pool in get_pool_name_list(self.options.cluster, self.POOL_SCHEDULER):
            load_cluster_pool_config(self.options.cluster, pool, self.POOL_SCHEDULER, None)
            pool_config_namespace = POOL_NAMESPACE.format(pool=pool, scheduler=self.POOL_SCHEDULER)
            pool_config = staticconf.config.get_namespace(pool_config_namespace).get_config_dict()
            if self.POOL_SETTINGS_PARENT in pool_config:
                self.migration_configs[pool] = pool_config[self.POOL_SETTINGS_PARENT]
                self.add_watcher({pool: get_pool_config_path(self.options.cluster, pool, self.POOL_SCHEDULER)})
                if self.migration_configs[pool]["trigger"].get("event", False):
                    self.pools_accepting_events.add(pool)
        self.logger.info(f"Found node migration configs for pools: {list(self.migration_configs.keys())}")

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

    def fetch_event_crd(self) -> Collection[MigrationEvent]:
        """Fetch migration events from Kubernetes CRDs"""
        self.cluster_connector.reload_client()
        events = self.cluster_connector.list_node_migration_resources(
            [MigrationStatus.PENDING, MigrationStatus.INPROGRESS]
        )
        return set(events) - self.events_in_progress

    def mark_event(self, event: MigrationEvent, status: MigrationStatus = MigrationStatus.COMPLETED) -> None:
        """Set status for CRD event resource

        :param MigrationEvent event: event to be marked
        :param MigrationStatus status: status to be set
        """
        self.cluster_connector.mark_node_migration_resource(event.resource_name, status)

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
        # TODO: everything

    def spawn_uptime_worker(self, pool: str, uptime: str):
        """Start process monitoring pool node uptime, and recycling nodes accordingly

        :param str pool: name of the pool
        """
        worker_setup = self._get_worker_setup(pool)
        if not worker_setup:
            # this can only happen with bad config, which gets logged already
            return
        self.logger.info(f"Spawning uptime migration worker for {pool} pool")
        # TODO: everything

    def monitor_workers(self):
        """Check health of migration worker processes"""
        # TODO: everything
        pass

    def run(self):
        for pool, config in self.migration_configs.items():
            if "max_uptime" in config["trigger"]:
                self.spawn_uptime_worker(pool, config["trigger"]["max_uptime"])
        while self.running:
            events = self.fetch_event_crd()
            for event in events:
                self.spawn_event_worker(event)
            time.sleep(self.run_interval)
            self.monitor_workers()


if __name__ == "__main__":
    setup_logging()
    NodeMigration().start()
