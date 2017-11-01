import time

import staticconf
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_env_config_path_arg
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.mesos.mesos_role_manager import get_roles_in_cluster
from clusterman.util import get_clusterman_logger
from clusterman.util import setup_config


logger = get_clusterman_logger(__name__)


class AutoscalerBatch(BatchDaemon):
    notify_emails = ['distsys-processing@yelp.com']

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
        self.run_interval = staticconf.read_int('batches.autoscaler.run_interval_seconds')

    def get_name(self):
        # Overrides the yelp_batch default, which is the name of the file (autoscaler in this case).
        # This controls the name of the scribe log for this batch. Without this, the log
        # conflicts with other batches (like the Kew autoscaler).
        return 'clusterman_autoscaler'

    def run(self):
        roles = get_roles_in_cluster(self.options.cluster)
        if not roles:
            raise Exception('No roles are configured to be managed by Clusterman in this cluster')

        # TODO: handle multiple roles in the autoscaler (CLUSTERMAN-126)
        if len(roles) > 1:
            raise NotImplementedError('Scaling multiple roles in a cluster is not yet supported')
        self.autoscaler = Autoscaler(self.options.cluster, roles[0])

        while self.running:
            time.sleep(self.run_interval - time.time() % self.run_interval)
            self.autoscaler.run(dry_run=self.options.dry_run)


if __name__ == '__main__':
    AutoscalerBatch().start()
