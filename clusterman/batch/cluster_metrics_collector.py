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
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_disable_sensu_arg
from clusterman.args import add_env_config_path_arg
from clusterman.args import add_healthcheck_only_arg
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.batch.util import suppress_request_limit_exceeded
from clusterman.config import get_pool_config_path
from clusterman.config import load_cluster_pool_config
from clusterman.config import setup_config
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.util import get_pool_name_list
from clusterman.util import get_clusterman_logger
from clusterman.util import sensu_checkin
from clusterman.util import splay_event_time

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


class ClusterMetricsCollector(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ['distsys-compute@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('ClusterMetricsCollector options')
        add_cluster_arg(arg_group, required=True)
        add_env_config_path_arg(arg_group)
        add_disable_sensu_arg(arg_group)
        add_healthcheck_only_arg(arg_group)
        add_cluster_config_directory_arg(arg_group)

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)

        # Since we want to collect metrics for all the pools, we need to call setup_config
        # first to load the cluster config path, and then read all the entries in that directory
        self.pools = get_pool_name_list(self.options.cluster) if not self.options.healthcheck_only else []
        for pool in self.pools:
            self.config.watchers.append({pool: get_pool_config_path(self.options.cluster, pool)})
            load_cluster_pool_config(self.options.cluster, pool, None)

        self.region = staticconf.read_string('aws.region')
        self.run_interval = staticconf.read_int('batches.cluster_metrics.run_interval_seconds')
        self.logger = logger

        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)

    def load_mesos_managers(self):
        logger.info('Reloading all MesosPoolManagers')
        self.mesos_managers = {}
        for pool in self.pools:
            logger.info(f'Loading resource groups for {pool} on {self.options.cluster}')
            self.mesos_managers[pool] = MesosPoolManager(self.options.cluster, pool)

    def write_metrics(self, writer, metrics_to_write):
        for metric, value_method in metrics_to_write:
            for pool, manager in self.mesos_managers.items():
                value = value_method(manager)
                metric_name = generate_key_with_dimensions(metric, {'cluster': self.options.cluster, 'pool': pool})
                logger.info(f'Writing {metric_name} to metric store')
                data = (metric_name, int(time.time()), value)
                writer.send(data)

    @suppress_request_limit_exceeded()
    def run(self):
        self.load_mesos_managers()  # Load the pools on the first run; do it here so we get logging

        while self.running:
            time.sleep(splay_event_time(
                self.run_interval,
                self.get_name() + self.options.cluster,
            ))
            if self.options.healthcheck_only:
                continue

            successful = True

            for manager in self.mesos_managers.values():
                manager.reload_state()

            for metric_type, metrics in METRICS_TO_WRITE.items():
                with self.metrics_client.get_writer(metric_type) as writer:
                    try:
                        self.write_metrics(writer, metrics)
                    except socket.timeout:
                        # Try to get metrics for the rest of the clusters, but make sure we know this failed
                        logger.warn(f'Timed out getting cluster metric data:\n\n{format_exc()}')
                        successful = False
                        continue

            # Report successful run to Sensu.
            if successful:
                sensu_args = dict(
                    check_name='check_clusterman_cluster_metrics_running',
                    output='OK: clusterman cluster_metrics was successful',
                    check_every='1m',
                    source=self.options.cluster,
                    ttl='5m',
                    noop=self.options.disable_sensu,
                )
                sensu_checkin(**sensu_args)


if __name__ == '__main__':
    ClusterMetricsCollector().start()
