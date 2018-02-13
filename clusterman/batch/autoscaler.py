import time

import staticconf
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_branch_or_tag_arg
from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.util import LOG_STREAM_NAME
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.batch.util import sensu_checkin
from clusterman.config import get_role_config_path
from clusterman.config import setup_config
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


class AutoscalerBatch(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ['distsys-compute@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('AutoscalerBatch options')
        add_cluster_arg(arg_group, required=True)
        add_env_config_path_arg(arg_group)
        add_branch_or_tag_arg(arg_group)
        arg_group.add_argument(
            '--dry-run',
            default=False,
            action='store_true',
            help='If true, will only log autoscaling decisions instead of modifying capacities',
        )

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self.roles = staticconf.read_list('cluster_roles')
        for role in self.roles:
            self.config.watchers.append({role: get_role_config_path(role)})
        self.logger = logger
        if not self.roles:
            raise Exception('No roles are configured to be managed by Clusterman in this cluster')

        # TODO: handle multiple roles in the autoscaler (CLUSTERMAN-126)
        if len(self.roles) > 1:
            raise NotImplementedError('Scaling multiple roles in a cluster is not yet supported')

        self.autoscaler = Autoscaler(self.options.cluster, self.roles[0])

    def _get_local_log_stream(self, clog_prefix=None):
        # Overrides the yelp_batch default, which is tmp_batch_<filename> (autoscaler in this case)

        # This controls the name of the scribe log for this batch. Without this, the log
        # conflicts with other batches (like the Kew autoscaler).
        return LOG_STREAM_NAME

    def run(self):
        while self.running:
            time.sleep(self.autoscaler.time_to_next_activation())
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
