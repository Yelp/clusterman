import arrow
import simplejson as json
import staticconf
import yelp_meteorite
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS
from pysensu_yelp import Status
from simplejson.errors import JSONDecodeError
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.util import evaluate_signal
from clusterman.autoscaler.util import get_metrics_index_from_s3
from clusterman.autoscaler.util import load_signal_connection
from clusterman.autoscaler.util import read_signal_config
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.util import get_clusterman_logger
from clusterman.util import sensu_checkin

SIGNAL_LOAD_CHECK_NAME = 'signal_configuration_failed'
CAPACITY_GAUGE_NAME = 'clusterman.autoscaler.target_capacity'
logger = get_clusterman_logger(__name__)


class Autoscaler:
    def __init__(self, cluster, pool, apps, *, pool_manager=None, metrics_client=None):
        self.cluster = cluster
        self.pool = pool
        self.apps = apps

        # TODO: handle multiple apps in the autoscaler (CLUSTERMAN-126)
        if len(self.apps) > 1:
            raise NotImplementedError('Scaling multiple apps in a cluster is not yet supported')

        logger.info(f'Initializing autoscaler engine for {self.pool} in {self.cluster}...')
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool))
        self.capacity_gauge = yelp_meteorite.create_gauge(CAPACITY_GAUGE_NAME, {'cluster': cluster, 'pool': pool})

        self.mesos_pool_manager = pool_manager or MesosPoolManager(self.cluster, self.pool)

        self.mesos_region = staticconf.read_string('aws.region')
        self.metrics_client = metrics_client or ClustermanMetricsBotoClient(self.mesos_region, app_identifier=self.apps[0])
        for app in self.apps:
            self.load_signal_for_app(app)
        self._last_signal_traceback = None

        logger.info('Initialization complete')

    @property
    def run_frequency(self):
        return self.signal_config.period_minutes * 60

    def run(self, dry_run=False, timestamp=None):
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: Don't actually modify the fleet size, just print what would happen
        """
        timestamp = timestamp or arrow.utcnow()
        logger.info(f'Autoscaling run starting at {timestamp}')
        new_target_capacity = self._compute_target_capacity(timestamp)
        self.capacity_gauge.set(new_target_capacity, {'dry_run': dry_run})
        self.mesos_pool_manager.modify_target_capacity(new_target_capacity, dry_run=dry_run)

    def load_signal_for_app(self, app):
        """Load the signal object to use for autoscaling."""
        logger.info(f'Loading autoscaling signal for {self.apps[0]} on {self.pool} in {self.cluster}')

        # TODO (CLUSTERMAN-126, CLUSTERMAN-195) apps will eventually have separate namespaces from pools
        pool_namespace = POOL_NAMESPACE.format(pool=app)
        try:
            metrics_index_bucket = staticconf.read_string('aws.s3_metrics_index_bucket')
            metrics_index = get_metrics_index_from_s3(metrics_index_bucket, self.mesos_region)
        except staticconf.errors.ConfigurationError:
            metrics_index = None
            logger.warning('no metrics_index_bucket is configured.')
        self.signal_config = read_signal_config(DEFAULT_NAMESPACE, metrics_index)
        use_default = True
        try:
            # see if the pool has set up a custom signal correctly; if not, fall back to the default
            # signal configuration (preloaded above)
            self.signal_config = read_signal_config(pool_namespace, metrics_index)
            use_default = False
        except NoSignalConfiguredException:
            logger.info(f'No signal configured for {self.pool}, falling back to default')
        except Exception:
            msg = f'WARNING: loading signal for {self.pool} failed, falling back to default'
            logger.exception(msg)
            sensu_checkin(
                check_name=SIGNAL_LOAD_CHECK_NAME,
                status=Status.WARNING,
                output=msg,
                source=self.cluster,
                page=False,
                ttl=None,
                app=app,
            )

        try:
            self._init_signal_connection(app, use_default)
        except Exception as e:
            raise ClustermanSignalError('Signal connection initialization failed') from e

    def _init_signal_connection(self, app, use_default):
        """ Initialize the signal socket connection/communication layer.

        :param use_default: use the default signal with whatever parameters are stored in self.signal_config
        """
        if not use_default:
            # Try to set up the (non-default) signal specified in the signal_config
            #
            # If it fails, it might be because the signal_config is requesting a different signal (or different
            # configuration) for a signal in the default role, so we fall back to the default in that case
            try:
                # Look for the signal name under the "pool" directory in clusterman_signals
                self.signal_conn = load_signal_connection(
                    self.signal_config.branch_or_tag,
                    app,
                    self.signal_config.name,
                )
            except SignalConnectionError:
                # If it's not there, see if the signal is one of our default signals
                logger.info(f'Signal {self.signal_config.name} not found in {self.pool}.yaml, checking default signals')
                use_default = True

        # This is not an "else" because the value of use_default may have changed in the above block
        if use_default:
            default_role = staticconf.read_string('autoscaling.default_signal_role')
            self.signal_conn = load_signal_connection(
                self.signal_config.branch_or_tag,
                default_role,
                self.signal_config.name,
            )

        signal_kwargs = json.dumps({'parameters': self.signal_config.parameters})
        self.signal_conn.send(signal_kwargs.encode())
        logger.info(f'Loaded signal {self.signal_config.name}')

    def _get_metrics(self, end_time):
        metrics = {}
        for metric in self.signal_config.required_metrics:
            start_time = end_time.shift(minutes=-metric.minute_range)
            metric_key = (
                generate_key_with_dimensions(metric.name, {'cluster': self.cluster, 'pool': self.pool})
                if metric.type == SYSTEM_METRICS
                else metric.name
            )
            metrics[metric.name] = self.metrics_client.get_metric_values(
                metric_key,
                metric.type,
                start_time.timestamp,
                end_time.timestamp
            )[1]
        return metrics

    def _compute_target_capacity(self, timestamp):
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :returns: the new target capacity we should scale to
        """
        # TODO (CLUSTERMAN-201) support other types of resource requests
        metrics = self._get_metrics(timestamp)
        try:
            resource_request = evaluate_signal(metrics, timestamp, self.signal_conn)

        # JSONDecodeError means that the signal returned something unexpected, so store the result
        # from the signal and alert the signal owner.  Similarly, BrokenPipeError means that the
        # signal process crashed (probably because of a JSONDecodeError earlier); so we print the
        # last signal traceback in addition to the BrokenPipe traceback for ease of debugging
        #
        # Other errors will propagate upwards and notify the service owner
        except JSONDecodeError as e:
            self._last_signal_traceback = e.doc
            logger.error(self._last_signal_traceback)
            raise ClustermanSignalError('Signal evaluation failed') from e
        except BrokenPipeError as e:
            if self._last_signal_traceback:
                logger.error('The most recent error from the signal was:\n' + self._last_signal_traceback)
            raise ClustermanSignalError('The signal is down') from e

        if resource_request['cpus'] is None:
            logger.info(f'No data from signal, not changing capacity')
            return self.mesos_pool_manager.target_capacity
        signal_cpus = float(resource_request['cpus'])

        # Get autoscaling settings.
        setpoint = staticconf.read_float('autoscaling.setpoint')
        setpoint_margin = staticconf.read_float('autoscaling.setpoint_margin')
        cpus_per_weight = staticconf.read_int('autoscaling.cpus_per_weight')

        # If the percentage allocated differs by more than the allowable margin from the setpoint,
        # we scale up/down to reach the setpoint.  We want to use target_capacity here instead of
        # get_resource_total to protect against short-term fluctuations in the cluster.
        total_cpus = self.mesos_pool_manager.target_capacity * cpus_per_weight
        setpoint_cpus = setpoint * total_cpus
        cpus_difference_from_setpoint = signal_cpus - setpoint_cpus

        # Note that the setpoint window is based on the value of total_cpus, not setpoint_cpus
        # This is so that, if you have a setpoint of 70% and a margin of 10%, you know that the
        # window is going to be between 60% and 80%, not 63% and 77%.
        window_size = setpoint_margin * total_cpus
        lb, ub = setpoint_cpus - window_size, setpoint_cpus + window_size
        logger.info(f'Current CPU total is {total_cpus} (setpoint={setpoint_cpus}); setpoint window is [{lb}, {ub}]')
        logger.info(f'Signal {self.signal_config.name} requested {signal_cpus} CPUs')
        if abs(cpus_difference_from_setpoint / total_cpus) >= setpoint_margin:
            # We want signal_cpus / new_total_cpus = setpoint.
            # So new_total_cpus should be signal_cpus / setpoint.
            new_target_cpus = signal_cpus / setpoint

            # Finally, convert CPUs to capacity units.
            new_target_capacity = new_target_cpus / cpus_per_weight
            logger.info(f'Computed target capacity is {new_target_capacity} units ({new_target_cpus} CPUs)')
        else:
            logger.info('Requested CPUs within setpoint margin, not changing target capacity')
            new_target_capacity = self.mesos_pool_manager.target_capacity
        return new_target_capacity
