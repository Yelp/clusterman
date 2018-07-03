import traceback

import arrow
import staticconf
import yelp_meteorite
from clusterman_metrics import ClustermanMetricsBotoClient
from pysensu_yelp import Status
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.config import get_autoscaling_config
from clusterman.autoscaler.signals import Signal
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.util import get_clusterman_logger
from clusterman.util import sensu_checkin

SIGNAL_LOAD_CHECK_NAME = 'signal_configuration_failed'
CAPACITY_GAUGE_NAME = 'clusterman.autoscaler.target_capacity'
logger = get_clusterman_logger(__name__)


class Autoscaler:
    def __init__(self, cluster, pool, apps, *, pool_manager=None, metrics_client=None):
        """ Class containing the core logic for autoscaling a cluster

        :param cluster: the name of the cluster to autoscale
        :param pool: the name of the pool to autoscale
        :param apps: a list of apps running on the pool
        :param pool_manager: a MesosPoolManager object (used for simulations)
        :param metrics_client: a ClustermanMetricsBotoClient object (used for simulations)
        """
        self.cluster = cluster
        self.pool = pool
        self.apps = apps

        # TODO: handle multiple apps in the autoscaler (CLUSTERMAN-126)
        if len(self.apps) > 1:
            raise NotImplementedError('Scaling multiple apps in a cluster is not yet supported')

        logger.info(f'Initializing autoscaler engine for {self.pool} in {self.cluster}...')
        self.capacity_gauge = yelp_meteorite.create_gauge(CAPACITY_GAUGE_NAME, {'cluster': cluster, 'pool': pool})

        self.autoscaling_config = get_autoscaling_config(POOL_NAMESPACE.format(pool=self.pool))
        self.mesos_pool_manager = pool_manager or MesosPoolManager(self.cluster, self.pool)

        self.mesos_region = staticconf.read_string('aws.region')
        self.metrics_client = metrics_client or ClustermanMetricsBotoClient(self.mesos_region)
        self.default_signal = Signal(
            self.cluster,
            self.pool,
            None,  # the default signal is not specific to any app
            DEFAULT_NAMESPACE,
            self.metrics_client,
            signal_namespace=staticconf.read_string('autoscaling.default_signal_role'),
        )
        self.signal = self._get_signal_for_app(self.apps[0])
        logger.info('Initialization complete')

    @property
    def run_frequency(self):
        return self.signal.config.period_minutes * 60

    def run(self, dry_run=False, timestamp=None):
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: boolean; if True, don't modify the pool size, just print what would happen
        :param timestamp: an arrow object indicating the current time
        """
        timestamp = timestamp or arrow.utcnow()
        logger.info(f'Autoscaling run starting at {timestamp}')

        try:
            signal_name = self.signal.config.name
            resource_request = self.signal.evaluate(timestamp)
            exception = None
        except Exception as e:
            logger.error(f'Client signal {self.signal.config.name} failed; using default signal')
            signal_name = self.default_signal.config.name
            resource_request = self.default_signal.evaluate(timestamp)
            exception, tb = e, traceback.format_exc()

        logger.info(f'Signal {signal_name} requested {resource_request["cpus"]} CPUs')
        self.mesos_pool_manager.reload_state()
        new_target_capacity = self._compute_target_capacity(resource_request)
        self.capacity_gauge.set(new_target_capacity, {'dry_run': dry_run})
        self.mesos_pool_manager.modify_target_capacity(new_target_capacity, dry_run=dry_run)

        if exception:
            logger.error(f'The client signal failed with:\n{tb}')
            raise exception

    def _get_signal_for_app(self, app):
        """Load the signal object to use for autoscaling for a particular app

        :param app: the name of the app to load a Signal for
        :returns: the configured app signal, or the default signal in case of an error
        """
        logger.info(f'Loading autoscaling signal for {app} on {self.pool} in {self.cluster}')

        # TODO (CLUSTERMAN-126, CLUSTERMAN-195) apps will eventually have separate namespaces from pools
        pool_namespace = POOL_NAMESPACE.format(pool=app)
        signal_namespace = staticconf.get_string('autoscale_signal.namespace', None, namespace=pool_namespace)

        try:
            # see if the pool has set up a custom signal correctly; if not, fall back to the default signal
            return Signal(self.cluster, self.pool, app, pool_namespace, self.metrics_client, signal_namespace)
        except NoSignalConfiguredException:
            logger.info(f'No signal configured for {app}, falling back to default')
            return self.default_signal
        except Exception:
            msg = f'WARNING: loading signal for {app} failed, falling back to default'
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
            return self.default_signal

    def _compute_target_capacity(self, resource_request):
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :param resource_request: a resource_request object from the signal evaluation
        :returns: the new target capacity we should scale to
        """
        # TODO (CLUSTERMAN-201) support other types of resource requests

        if resource_request['cpus'] is None:
            logger.info('No data from signal, not changing capacity')
            return self.mesos_pool_manager.target_capacity
        signal_cpus = float(resource_request['cpus'])

        # If the percentage allocated differs by more than the allowable margin from the setpoint,
        # we scale up/down to reach the setpoint.  We want to use target_capacity here instead of
        # get_resource_total to protect against short-term fluctuations in the cluster.
        total_cpus = self.mesos_pool_manager.target_capacity * self.autoscaling_config.cpus_per_weight
        setpoint_cpus = self.autoscaling_config.setpoint * total_cpus
        cpus_difference_from_setpoint = signal_cpus - setpoint_cpus

        # Note that the setpoint window is based on the value of total_cpus, not setpoint_cpus
        # This is so that, if you have a setpoint of 70% and a margin of 10%, you know that the
        # window is going to be between 60% and 80%, not 63% and 77%.
        window_size = self.autoscaling_config.setpoint_margin * total_cpus
        lb, ub = setpoint_cpus - window_size, setpoint_cpus + window_size
        logger.info(f'Current CPU total is {total_cpus} (setpoint={setpoint_cpus}); setpoint window is [{lb}, {ub}]')
        if abs(cpus_difference_from_setpoint / total_cpus) >= self.autoscaling_config.setpoint_margin:
            # We want signal_cpus / new_total_cpus = setpoint.
            # So new_total_cpus should be signal_cpus / setpoint.
            new_target_cpus = signal_cpus / self.autoscaling_config.setpoint

            # Finally, convert CPUs to capacity units.
            new_target_capacity = new_target_cpus / self.autoscaling_config.cpus_per_weight
            logger.info(f'Computed target capacity is {new_target_capacity} units ({new_target_cpus} CPUs)')
        else:
            logger.info('Requested CPUs within setpoint margin, not changing target capacity')
            new_target_capacity = self.mesos_pool_manager.target_capacity

        return new_target_capacity
