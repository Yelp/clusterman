import time

import staticconf
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.config import get_roles_watcher
from clusterman.config import get_service_watcher
from clusterman.config import setup_config
from clusterman.util import get_clusterman_logger
from clusterman.util import sensu_checkin

logger = get_clusterman_logger(__name__)


class AutoscalerBatch(BatchDaemon):
    notify_emails = ['distsys-compute@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('AutoscalerBatch options')
        add_cluster_arg(arg_group, required=True)
        add_env_config_path_arg(arg_group)
        arg_group.add_argument(
            '--dry-run',
            default=False,
            action='store_true',
            help='If true, will only log autoscaling decisions instead of modifying capacities.',
        )

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self.service_config_watcher = get_service_watcher(self.options, using_roles=True)
        self.role_configs_watcher = get_roles_watcher(self.options.cluster)

    def get_name(self):
        # Overrides the yelp_batch default, which is the name of the file (autoscaler in this case).
        # This controls the name of the scribe log for this batch. Without this, the log
        # conflicts with other batches (like the Kew autoscaler).
        return 'clusterman_autoscaler'

    def run(self):
        roles = staticconf.read_list('cluster_roles')

        if not roles:
            raise Exception('No roles are configured to be managed by Clusterman in this cluster')

        # TODO: handle multiple roles in the autoscaler (CLUSTERMAN-126)
        if len(roles) > 1:
            raise NotImplementedError('Scaling multiple roles in a cluster is not yet supported')

        self.autoscaler = Autoscaler(self.options.cluster, roles[0])
        while self.running:
            time.sleep(self.autoscaler.time_to_next_activation())

            reload_config = self.service_config_watcher.reload_if_changed()
            if reload_config is not None:
                logger.info('Service config changed, reloading')
                # Role directory may have changed, so we need to get new role watchers.
                self.role_configs_watcher = get_roles_watcher(self.options.cluster)
                self.autoscaler.load_signal()
            elif self.role_configs_watcher.reload_if_changed() is not None:
                logger.info('Role config changed, reloading')
                self.autoscaler.load_signal()

            if staticconf.read_list('cluster_roles') != roles:
                logger.info('Roles configured for cluster have changed. Stopping.')
                self.stop()
            else:
                self.autoscaler.run(dry_run=self.options.dry_run)

                # Report successful run to Sensu.
                sensu_checkin(
                    check_name='check_clusterman_autoscaler_running',
                    output='OK: clusterman autoscaler is running',
                    check_every='10m',
                    source=self.options.cluster,
                    ttl='25m',
                    page=False,
                    noop=self.options.dry_run,
                )


if __name__ == '__main__':
    AutoscalerBatch().start()
