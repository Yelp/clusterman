import time

import staticconf
from pysensu_yelp import Status
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_branch_or_tag_arg
from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_env_config_path_arg
from clusterman.args import add_pool_arg
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.util import LOG_STREAM_NAME
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.config import get_pool_config_path
from clusterman.config import setup_config
from clusterman.exceptions import AutoscalerError
from clusterman.exceptions import ClustermanSignalError
from clusterman.util import get_clusterman_logger
from clusterman.util import sensu_checkin
from clusterman.util import splay_time_start

logger = get_clusterman_logger(__name__)
SIGNAL_CHECK_NAME = 'check_clusterman_autoscaler_signal'
SERVICE_CHECK_NAME = 'check_clusterman_autoscaler_service'
DEFAULT_TTL = '25m'
DEFAULT_CHECK_EVERY = '10m'


def sensu_alert_triage(fail=False):
    def decorator(fn):
        def wrapper(self):
            msg = ''
            signal_failed, service_failed = False, False
            error = None
            try:
                fn(self)
            except ClustermanSignalError as e:
                msg = str(e)
                logger.exception(f'Autoscaler signal failed: {msg}')
                signal_failed = True
                error = e
            except Exception as e:
                msg = str(e)
                logger.exception(f'Autoscaler service failed: {msg}')
                service_failed = True
                error = e
            self._do_sensu_checkins(signal_failed, service_failed, msg)
            if fail and error:
                raise AutoscalerError from error
        return wrapper
    return decorator


class AutoscalerBatch(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ['distsys-compute@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('AutoscalerBatch options')
        add_cluster_arg(arg_group, required=True)
        add_pool_arg(arg_group, required=False)
        add_cluster_config_directory_arg(arg_group)
        add_env_config_path_arg(arg_group)
        add_branch_or_tag_arg(arg_group)
        arg_group.add_argument(
            '--dry-run',
            default=False,
            action='store_true',
            help='If true, will only log autoscaling decisions instead of modifying capacities',
        )

    @batch_configure
    @sensu_alert_triage(fail=True)
    def configure_initial(self):
        setup_config(self.options)
        self.autoscaler = None
        self.pool = staticconf.read_string('scaling_pool')
        self.config.watchers.append({self.pool: get_pool_config_path(self.options.cluster, self.pool)})
        self.logger = logger
        if not self.pool:
            raise Exception('No pool is configured to be managed by Clusterman in this cluster')

        self.apps = [self.pool]  # TODO (CLUSTERMAN-126) somday these should not be the same thing
        self.autoscaler = Autoscaler(self.options.cluster, self.pool, self.apps)

    def _get_local_log_stream(self, clog_prefix=None):
        # Overrides the yelp_batch default, which is tmp_batch_<filename> (autoscaler in this case)

        # This controls the name of the scribe log for this batch. Without this, the log
        # conflicts with other batches (like the Kew autoscaler).
        return LOG_STREAM_NAME

    @sensu_alert_triage()
    def _autoscale(self):
        time.sleep(splay_time_start(
            self.autoscaler.run_frequency,
            self.get_name(),
            staticconf.read_string('aws.region'),
        ))
        self.autoscaler.run(dry_run=self.options.dry_run)

    def run(self):
        while self.running:
            self._autoscale()

    def _do_sensu_checkins(self, signal_failed, service_failed, msg):
        check_every = ('{minutes}m'.format(minutes=int(self.autoscaler.run_frequency // 60))
                       if self.autoscaler else DEFAULT_CHECK_EVERY)
        # magic-y numbers here; an alert will time out after two autoscaler run periods plus a five minute buffer
        ttl = ('{minutes}m'.format(minutes=int(self.autoscaler.run_frequency // 60) * 2 + 5)
               if self.autoscaler else DEFAULT_TTL)

        # Check in for the signal
        signal_sensu_args = dict(
            check_name=SIGNAL_CHECK_NAME,
            check_every=check_every,
            source=self.options.cluster,
            ttl=ttl,
            app_name=self.apps[0],
            noop=self.options.dry_run,
        )

        if signal_failed:
            signal_sensu_args['output'] = f'FAILED: clusterman autoscaler signal failed ({msg})'
            signal_sensu_args['status'] = Status.CRITICAL
        else:
            signal_sensu_args['output'] = f'OK: clusterman autoscaler signal is fine'
        sensu_checkin(**signal_sensu_args)

        # Check in for the service
        service_sensu_args = dict(
            check_name=SERVICE_CHECK_NAME,
            check_every=check_every,
            source=self.options.cluster,
            ttl=ttl,
            noop=self.options.dry_run,
        )

        if service_failed:
            service_sensu_args['output'] = f'FAILED: clusterman autoscaler failed ({msg})'
            service_sensu_args['status'] = Status.CRITICAL
        else:
            service_sensu_args['output'] = f'OK: clusterman autoscaler is fine'
        sensu_checkin(**service_sensu_args)


if __name__ == '__main__':
    AutoscalerBatch().start()
