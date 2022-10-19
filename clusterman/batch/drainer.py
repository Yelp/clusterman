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
import sys
import time
from typing import Optional

import colorlog
import staticconf

try:
    from yelp_batch.batch import batch_command_line_arguments
    from yelp_batch.batch import batch_configure
    from yelp_batch.batch_daemon import BatchDaemon
    from clusterman.batch.util import BatchLoggingMixin
    from clusterman.batch.util import BatchRunningSentinelMixin
except ImportError:
    colorlog.warning("Drainer functionality only available with internal libraries")
    identity_func = lambda x: x  # noqa
    batch_command_line_arguments = identity_func
    batch_configure = identity_func
    BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin = (  # type: ignore
        type(f"MockBatch{i}", (object,), {}) for i in range(3)  # type: ignore
    )

from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.args import subparser

from clusterman.config import get_pool_config_path
from clusterman.config import load_cluster_pool_config
from clusterman.config import setup_config
from clusterman.draining.mesos import operator_api
from clusterman.draining.queue import DrainingClient
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.util import get_pool_name_list
from clusterman.util import setup_logging


class NodeDrainerBatch(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ["compute-infra@yelp.com"]

    CLI_SUBCOMMAND = "drain"
    DEFAULT_RUN_INTERVAL_SECONDS = 5

    @batch_command_line_arguments
    def parse_args(self, parser: argparse.ArgumentParser):
        arg_group = parser.add_argument_group("NodeDrainer batch options")
        cli_entrypoint(None, arg_group, None)

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self.run_interval = staticconf.read_int(
            "batches.drainer.run_interval_seconds", self.DEFAULT_RUN_INTERVAL_SECONDS
        )
        for scheduler in ("mesos", "kubernetes"):
            for pool in get_pool_name_list(self.options.cluster, scheduler):
                load_cluster_pool_config(self.options.cluster, pool, scheduler, None)
                self.add_watcher({f"{pool}.{scheduler}": get_pool_config_path(self.options.cluster, pool, scheduler)})
        self.logger = colorlog.getLogger(__name__)

    def run(self):
        cluster_name = self.options.cluster
        draining_client = DrainingClient(cluster_name)
        cluster_manager_name = staticconf.read_string(f"clusters.{cluster_name}.cluster_manager")
        always_delay_drain_processing = staticconf.read_bool(
            f"clusters.{cluster_name}.always_delay_drain_processing", True
        )
        mesos_operator_client = kube_operator_client = None

        try:
            kube_operator_client = KubernetesClusterConnector(cluster_name, None)
        except Exception:
            self.logger.error("Cluster specified is mesos specific. Skipping kubernetes operator")
        if cluster_manager_name == "mesos":
            try:
                mesos_master_url = staticconf.read_string(f"clusters.{cluster_name}.mesos_master_fqdn")
                mesos_secret_path = staticconf.read_string("mesos.mesos_agent_secret_path", default=None)
                mesos_operator_client = operator_api(mesos_master_url, mesos_secret_path)
            except Exception:
                self.logger.error("Cluster specified is kubernetes specific. Skipping mesos operator")

        self.logger.info("Polling SQS for messages every 5s")
        while self.running:
            if kube_operator_client:
                kube_operator_client.reload_client()
            draining_client.clean_processing_hosts_cache()
            warning_result = draining_client.process_warning_queue()
            draining_result = draining_client.process_drain_queue(
                mesos_operator_client=mesos_operator_client,
                kube_operator_client=kube_operator_client,
            )
            termination_result = draining_client.process_termination_queue(
                mesos_operator_client=mesos_operator_client,
                kube_operator_client=kube_operator_client,
            )
            # sleep five seconds only if all queues are empty OR feature flag is enabled
            if always_delay_drain_processing or (not warning_result and not draining_result and not termination_result):
                time.sleep(self.run_interval)


def main(args: Optional[argparse.ArgumentParser] = None):
    if args:
        # clean sub-command when invoked from CLI interface
        sys.argv.pop(sys.argv.index(NodeDrainerBatch.CLI_SUBCOMMAND))
    NodeDrainerBatch().start()


@subparser(NodeDrainerBatch.CLI_SUBCOMMAND, "Drains and terminates instances submitted to SQS by clusterman", main)
def cli_entrypoint(
    subparser: argparse.ArgumentParser,
    required_named_args: argparse.Namespace,
    optional_named_args: argparse.Namespace,
) -> None:
    add_env_config_path_arg(required_named_args)
    add_cluster_arg(required_named_args, required=True)


if __name__ == "__main__":
    setup_logging()
    main()
