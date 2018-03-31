import time

import staticconf
from pysensu_yelp import Status
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
from clusterman.exceptions import ClustermanSignalError
from clusterman.util import get_clusterman_logger
from clusterman.util import splay_time_start

logger = get_clusterman_logger(__name__)
SIGNAL_CHECK_NAME = 'check_clusterman_autoscaler_signal'
SERVICE_CHECK_NAME = 'check_clusterman_autoscaler_service'


def sensu_alert_triage(fn):
    def wrapper(self):
        msg = ''
        signal_failed, service_failed = False, False
        try:
            fn(self)
        except ClustermanSignalError as e:
            msg = str(e)
            signal_failed = True
        except Exception as e:
            msg = str(e)
            service_failed = True
        self._do_sensu_checkins(signal_failed, service_failed, msg)
    return wrapper


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
    @sensu_alert_triage
    def configure_initial(self):
        setup_config(self.options)
        self.roles = staticconf.read_list('cluster_roles')
        for role in self.roles:
            self.config.watchers.append({role: get_role_config_path(self.options.cluster, role)})
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

    @sensu_alert_triage
    def _autoscale(self):
        time.sleep(splay_time_start(
            self.autoscaler.run_frequency,
            self.get_simple_name(),
            staticconf.read_string('aws.region'),
        ))
        self.autoscaler.run(dry_run=self.options.dry_run)

    def run(self):
        while self.running:
            self._autoscale()

    def _do_sensu_checkins(self, signal_failed, service_failed, msg):
        signal_owner = 'releng'  # TODO (CLUSTERMAN-199)
        check_every = '10m'

        # Check in for the signal
        if signal_failed:
            sensu_checkin(
                check_name=SIGNAL_CHECK_NAME,
                output=f'FAILED: clusterman autoscaler signal failed ({msg})',
                status=Status.CRITICAL,
                owner=signal_owner,
                check_every=check_every,
                source=self.options.cluster,
                page=False,
                ttl='25m',
                noop=self.options.dry_run,
            )
        else:
            sensu_checkin(
                check_name=SIGNAL_CHECK_NAME,
                output=f'OK: clusterman autoscaler signal is fine',
                owner=signal_owner,
                check_every=check_every,
                source=self.options.cluster,
                page=False,
                ttl='25m',
                noop=self.options.dry_run,
            )

        # Check in for the service
        if service_failed:
            sensu_checkin(
                check_name=SERVICE_CHECK_NAME,
                output=f'FAILED: clusterman autoscaler failed ({msg})',
                status=Status.CRITICAL,
                check_every=check_every,
                source=self.options.cluster,
                page=False,
                ttl='25m',
                noop=self.options.dry_run,
            )
        else:
            sensu_checkin(
                check_name=SERVICE_CHECK_NAME,
                output=f'OK: clusterman autoscaler is fine',
                check_every=check_every,
                source=self.options.cluster,
                page=False,
                ttl='25m',
                noop=self.options.dry_run,
            )


if __name__ == '__main__':
    AutoscalerBatch().start()
