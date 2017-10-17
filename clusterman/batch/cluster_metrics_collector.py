import time

import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch import batch_context
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.mesos.mesos_role_manager import get_roles_in_cluster
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import setup_config


class ClusterMetricsCollector(BatchDaemon):
    notify_emails = ['distsys-processing@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('ClusterMetricsCollector options')
        add_cluster_arg(arg_group, required=True)
        add_env_config_path_arg(arg_group)

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)

        self.region = staticconf.read_string(f'mesos_clusters.{self.options.cluster}.aws_region')
        self.run_interval = staticconf.read_int('batches.cluster_metrics.run_interval_seconds')

        roles = get_roles_in_cluster(self.options.cluster)
        self.mesos_managers = {
            role: MesosRoleManager(role, self.options.cluster)
            for role in roles
        }

    @batch_context
    def get_writer(self):
        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)
        with self.metrics_client.get_writer(SYSTEM_METRICS) as writer:
            self.writer = writer
            yield

    def write_metrics(self):
        for role, manager in self.mesos_managers.items():
            average_cpu = manager.get_average_resource_utilization('cpus')
            metric_name = generate_key_with_dimensions('cpu_allocation', {'cluster': self.options.cluster, 'role': role})
            data = (metric_name, int(time.time()), average_cpu)
            self.writer.send(data)

    def run(self):
        while self.running:
            time.sleep(self.run_interval - time.time() % self.run_interval)
            self.write_metrics()


if __name__ == '__main__':
    ClusterMetricsCollector().start()
