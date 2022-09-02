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
from typing import Any
from typing import Collection

import colorlog
import staticconf
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.aws.client import sqs
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.config import get_pool_config_path
from clusterman.config import load_cluster_pool_config
from clusterman.config import POOL_NAMESPACE
from clusterman.config import setup_config
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
        add_cluster_arg(arg_group)

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self.logger = colorlog.getLogger(__name__)
        self.sqs_client = sqs()
        self.migration_workers = {}
        self.migration_configs = {}
        self.run_interval = staticconf.read_int("batches.node_migration.run_interval_seconds", 60)
        for pool in get_pool_name_list(self.options.cluster, self.POOL_SCHEDULER):
            load_cluster_pool_config(self.options.cluster, pool, self.POOL_SCHEDULER, None)
            pool_config_namespace = POOL_NAMESPACE.format(pool=pool, scheduler=self.POOL_SCHEDULER)
            pool_config = staticconf.config.get_namespace(pool_config_namespace).get_config_dict()
            if self.POOL_SETTINGS_PARENT in pool_config:
                self.migration_configs[pool] = pool_config[self.POOL_SETTINGS_PARENT]
                self.add_watcher({pool: get_pool_config_path(self.options.cluster, pool, self.POOL_SCHEDULER)})
        self.logger.info(f"Found node migration configs for pools: {list(self.migration_configs.keys())}")

    def fetch_event_queues(self, queues: Collection[str]):
        """Tail a collection of SQS queue for migration trigger events

        :param Collection[str] queues: SQS queue URLs
        """
        # TODO: everything

    def spawn_event_worker(self, event: Any):
        """Start process recycling nodes in a pool accordingly to some event parameters

        :param Any event: Event data (TODO: define an actual data structure)
        """
        self.logger.info(f"Spawning migration worker: {event}")
        # TODO: everything

    def spawn_uptime_worker(self, pool: str):
        """Start process monitoring pool node uptime, and recycling nodes accordingly

        :param str pool: name of the pool
        """
        self.logger.info(f"Spawning uptime migration worker for {pool} pool")
        # TODO: everything

    def monitor_workers(self):
        """Check health of migration worker processes"""
        # TODO: everything
        pass

    def run(self):
        event_queues = set()
        for pool, config in self.migration_configs.items():
            if "event_queue" in config["trigger"]:
                event_queues.add(config["trigger"]["event_queue"])
            if "max_uptime" in config["trigger"]:
                self.spawn_uptime_worker(pool)
        while self.running:
            events = self.fetch_event_queues(event_queues)
            for event in events:
                self.spawn_event_worker(event)
            time.sleep(self.run_interval)
            self.monitor_workers()


if __name__ == "__main__":
    setup_logging()
    NodeMigration().start()
