import socket
import time
from collections import namedtuple
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
from clusterman.mesos.metrics_generators import generate_framework_metadata
from clusterman.mesos.metrics_generators import generate_simple_metadata
from clusterman.mesos.metrics_generators import generate_system_metrics
from clusterman.mesos.util import get_pool_name_list
from clusterman.util import get_clusterman_logger
from clusterman.util import sensu_checkin
from clusterman.util import splay_event_time

logger = get_clusterman_logger(__name__)

MetricToWrite = namedtuple('MetricToWrite', ['generator', 'type', 'aggregate_meteorite_dims'])

METRICS_TO_WRITE = [
    MetricToWrite(generate_system_metrics, SYSTEM_METRICS, aggregate_meteorite_dims=False),
    MetricToWrite(generate_simple_metadata, METADATA, aggregate_meteorite_dims=False),
    MetricToWrite(generate_framework_metadata, METADATA, aggregate_meteorite_dims=True),
]


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

            for manager in self.mesos_managers.values():
                manager.reload_state()

            successful = self.write_all_metrics()

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

    def write_all_metrics(self):
        successful = True

        for metric_to_write in METRICS_TO_WRITE:
            with self.metrics_client.get_writer(
                metric_to_write.type,
                metric_to_write.aggregate_meteorite_dims
            ) as writer:
                try:
                    self.write_metrics(writer, metric_to_write.generator)
                except socket.timeout:
                    # Try to get metrics for the rest of the clusters, but make sure we know this failed
                    logger.warn(f'Timed out getting cluster metric data:\n\n{format_exc()}')
                    successful = False
                    continue

        return successful

    def write_metrics(self, writer, metric_generator):
        for pool, manager in self.mesos_managers.items():
            base_dimensions = {'cluster': self.options.cluster, 'pool': pool}

            for cluster_metric in metric_generator(manager):
                dimensions = dict(**base_dimensions, **cluster_metric.dimensions)
                metric_name = generate_key_with_dimensions(cluster_metric.metric_name, dimensions)
                logger.info(f'Writing {metric_name} to metric store')
                data = (metric_name, int(time.time()), cluster_metric.value)

                writer.send(data)


if __name__ == '__main__':
    ClusterMetricsCollector().start()
