import time
from datetime import timedelta
from importlib import import_module

import staticconf
import yelp_meteorite
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.util import read_signal_config
from clusterman.mesos.constants import ROLE_NAMESPACE
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import get_clusterman_logger


DELTA_GAUGE_NAME = 'clusterman.autoscaler.delta'
logger = get_clusterman_logger(__name__)


class Autoscaler:
    def __init__(self, cluster, role, role_manager=None, metrics_client=None):
        self.cluster = cluster
        self.role = role

        logger.info(f'Initializing autoscaler engine for {self.role} in {self.cluster}...')
        self.role_config = staticconf.NamespaceReaders(ROLE_NAMESPACE.format(role=self.role))
        self.delta_gauge = yelp_meteorite.create_gauge(DELTA_GAUGE_NAME, {'cluster': cluster, 'role': role})

        self.mesos_role_manager = role_manager or MesosRoleManager(self.cluster, self.role)

        self.metrics_client = metrics_client
        self.load_signal()

        logger.info('Initialization complete')

    def time_to_next_activation(self, timestamp=None):
        timestamp = timestamp or time.time()
        period_seconds = self.signal.period_minutes * 60
        return timedelta(seconds=period_seconds - timestamp % period_seconds)

    def run(self, dry_run=False, timestamp=None):
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: Don't actually modify the fleet size, just print what would happen
        """
        # TODO (CLUSTERMAN-122): reload signal if signals code or configs change
        logger.info(f'Autoscaling run starting at {timestamp}')
        delta = self._compute_cluster_delta(timestamp)
        self.delta_gauge.set(delta, {'dry_run': dry_run})
        new_target_capacity = self.mesos_role_manager.target_capacity + delta
        if dry_run:
            logger.warn('This is a dry run: cluster size will not change.')
        else:
            self.mesos_role_manager.modify_target_capacity(new_target_capacity)

    def load_signal(self):
        """Load the signal object to use for autoscaling.

        First try to load the custom signal configured by the role.
        If that fails, or doesn't exist, use the default signal defined by the service.
        """
        logger.info(f'Loading autoscaling signal for {self.role} in {self.cluster}')

        role_namespace = ROLE_NAMESPACE.format(role=self.role)
        try:
            signal_config = read_signal_config(role_namespace)
            if signal_config:
                self.signal = self._init_signal_from_config(self.role, signal_config)
                return
            else:
                logger.info(f'No signal configured for {self.role}, falling back to default')
        except Exception:
            logger.exception(f'Error loading signal for {self.role}, falling back to default')

        signal_config = read_signal_config(DEFAULT_NAMESPACE)
        self.signal = self._init_signal_from_config(
            staticconf.read_string('autoscaling.default_signal_role'),
            signal_config,
        )

    def _init_signal_from_config(self, signal_role, signal_config):
        """Initialize a signal object, given the role where the signal class is defined and config values for the signal.

        :param signal_role: string, corresponding to the package name in clusterman_signals where the signal is defined
        :param signal_config: a SignalConfig, containing values to initialize the signal
        """
        signal_module = import_module(f'clusterman_signals.{signal_role}')
        signal_class = getattr(signal_module, signal_config.name)
        signal = signal_class(
            self.cluster,
            self.role,
            period_minutes=signal_config.period_minutes,
            required_metrics=signal_config.required_metrics,
            custom_parameters=signal_config.custom_parameters,
            metrics_client=self.metrics_client,
        )
        logger.info(f'Loaded signal {signal_config.name} from role {signal_role}')
        return signal

    def _compute_cluster_delta(self, timestamp):
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :returns: a capacity delta for the role
        """
        resources = self.signal.get_signal(timestamp)
        if resources.cpus is None:
            logger.info(f'No data from signal, not changing capacity')
            return 0
        signal_cpus = float(resources.cpus)

        # Get autoscaling settings.
        setpoint = staticconf.read_float('autoscaling.setpoint')
        setpoint_margin = staticconf.read_float('autoscaling.setpoint_margin')
        cpus_per_weight = staticconf.read_int('autoscaling.cpus_per_weight')

        # If the percentage allocated differs by more than the allowable margin from the setpoint,
        # we scale up/down to reach the setpoint.
        total_cpus = self.mesos_role_manager.get_resource_total('cpus')
        cpus_difference_from_setpoint = signal_cpus - setpoint * total_cpus

        logger.info(f'Signal requested {signal_cpus} CPUs, current total is {total_cpus} CPUs')
        capacity_delta = 0
        if abs(cpus_difference_from_setpoint / total_cpus) >= setpoint_margin:
            # We want signal_cpus / new_total_cpus = setpoint.
            # So new_total_cpus should be signal_cpus / setpoint.

            # The number of cpus to add/remove, cpus_delta, is new_total_cpus - total_cpus.
            # cpus_delta = signal_cpus / setpoint - total_cpus

            # We already have cpus_difference_from_setpoint = signal_cpus - setpoint * total_cpus.
            # Dividing by setpoint, we get signal_cpus / setpoint - total_cpus, which is the desired cpus_delta above.
            cpus_delta = cpus_difference_from_setpoint / setpoint

            # Finally, convert CPUs to capacity units.
            capacity_delta = self._constrain_cluster_delta(cpus_delta / cpus_per_weight)
        logger.info(f'Computed capacity delta is {capacity_delta} units ({capacity_delta * cpus_per_weight} CPUs)')
        return capacity_delta

    def _constrain_cluster_delta(self, delta):
        """ Signals can return arbitrary values, so make sure we don't add or remove too much capacity """
        if delta > 0:
            return min(
                self.mesos_role_manager.max_capacity - self.mesos_role_manager.target_capacity,
                self.role_config.read_int('scaling_limits.max_weight_to_add'),
                delta,
            )
        elif delta < 0:
            return max(
                self.mesos_role_manager.min_capacity - self.mesos_role_manager.target_capacity,
                -self.role_config.read_int('scaling_limits.max_weight_to_remove'),
                delta,
            )
        else:
            return 0
