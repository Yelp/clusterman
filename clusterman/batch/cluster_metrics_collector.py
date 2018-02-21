import socket
import time
from traceback import format_exc

import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_disable_sensu_arg
from clusterman.args import add_env_config_path_arg
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import sensu_checkin
from clusterman.config import get_role_config_path
from clusterman.config import setup_config
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
METRICS_TO_WRITE = {
    SYSTEM_METRICS: [
        ('cpus_allocated', lambda manager: manager.get_resource_allocation('cpus')),
        ('mem_allocated', lambda manager: manager.get_resource_allocation('mem')),
        ('disk_allocated', lambda manager: manager.get_resource_allocation('disk')),
    ],
    METADATA: [
        ('cpus_total', lambda manager: manager.get_resource_total('cpus')),
        ('mem_total', lambda manager: manager.get_resource_total('mem')),
        ('disk_total', lambda manager: manager.get_resource_total('disk')),
        ('target_capacity', lambda manager: manager.target_capacity),
        ('fulfilled_capacity', lambda manager: {str(market): value for market,
                                                value in manager._get_market_capacities().items()}),
    ],
}


class ClusterMetricsCollector(BatchDaemon, BatchLoggingMixin):
    notify_emails = ['distsys-compute@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('ClusterMetricsCollector options')
        add_cluster_arg(arg_group, required=True)
        add_env_config_path_arg(arg_group)
        add_disable_sensu_arg(arg_group)

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)

        self.region = staticconf.read_string('aws.region')
        self.run_interval = staticconf.read_int('batches.cluster_metrics.run_interval_seconds')
        self.logger = logger

        self.roles = staticconf.read_list('cluster_roles')
        for role in self.roles:
            self.config.watchers.append({role: get_role_config_path(role)})
        self.mesos_managers = {
            role: MesosRoleManager(self.options.cluster, role)
            for role in self.roles
        }
        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)

    def write_metrics(self, writer, metrics_to_write):
        for metric, value_method in metrics_to_write:
            for role, manager in self.mesos_managers.items():
                value = value_method(manager)
                metric_name = generate_key_with_dimensions(metric, {'cluster': self.options.cluster, 'role': role})
                data = (metric_name, int(time.time()), value)
                writer.send(data)

    def run(self):
        while self.running:
            time.sleep(self.run_interval - time.time() % self.run_interval)

            successful = True
            for metric_type, metrics in METRICS_TO_WRITE.items():
                with self.metrics_client.get_writer(metric_type) as writer:
                    try:
                        self.write_metrics(writer, metrics)
                    except socket.timeout:
                        logger.warn(f'Timed out getting spot prices:\n\n{format_exc()}')
                        successful = False
                        continue

            # Report successful run to Sensu.
            if successful:
                sensu_checkin(
                    check_name='check_clusterman_cluster_metrics_running',
                    output='OK: clusterman cluster_metrics was successful',
                    check_every='1m',
                    source=self.options.cluster,
                    ttl='5m',
                    page=False,
                    noop=self.options.disable_sensu,
                )


if __name__ == '__main__':
    ClusterMetricsCollector().start()
