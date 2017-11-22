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


# TODO: put in config file (CLUSTERMAN-141)
DEFAULT_SIGNAL_ROLE = 'clusterman'
# If the percentage allocated differs by more than the allowable margin from the setpoint, we scale up/down to reach the setpoint.
SETPOINT = 0.7
SETPOINT_MARGIN = 0.1
CPUS_PER_WEIGHT = 8


class AutoscalerV2:
    def __init__(self, cluster, role):
        self.cluster = cluster
        self.role = role

        logger.info(f'Initializing autoscaler engine for {self.role} in {self.cluster}...')
        # TODO (CLUSTERMAN-107) we'll want to monitor this config for changes and reload as needed
        self.role_config = staticconf.NamespaceReaders(ROLE_NAMESPACE.format(role=self.role))
        self.delta_gauge = yelp_meteorite.create_gauge(DELTA_GAUGE_NAME, {'cluster': cluster, 'role': role, 'version': 'v2'})

        logger.info('Connecting to Mesos')
        self.mesos_role_manager = MesosRoleManager(self.cluster, self.role)

        self.load_signal()

        logger.info('Initialization complete')

    def get_period_seconds(self):
        """Get how often the autoscaler should be run."""
        return self.signal.period_minutes * 60

    def run(self, dry_run=False):
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: Don't actually modify the fleet size, just print what would happen
        """
        # TODO (CLUSTERMAN-122): reload signal if signals code or configs change
        delta = self._compute_cluster_delta()
        logger.info(f'Computed capacity delta is {delta}')
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
        self.signal = self._init_signal_from_config(DEFAULT_SIGNAL_ROLE, signal_config)

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
        )
        logger.info(f'Loaded signal {signal_config.name} from role {signal_role}')
        return signal

    def _compute_cluster_delta(self):
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :returns: a capacity delta for the role
        """
        resources = self.signal.get_signal()
        if resources.cpus is None:
            logger.info(f'No data from signal, not changing capacity')
            return 0
        signal_cpus = float(resources.cpus)

        total_cpus = self.mesos_role_manager.get_resource_total('cpus')
        cpus_difference_from_setpoint = signal_cpus - SETPOINT * total_cpus

        logger.info(f'Signal was {signal_cpus}, current total CPUs is {total_cpus}')
        if abs(cpus_difference_from_setpoint / total_cpus) >= SETPOINT_MARGIN:
            # We want signal_cpus / new_total_cpus = SETPOINT.
            # So new_total_cpus should be signal_cpus / SETPOINT.

            # The number of cpus to add/remove, cpus_delta, is new_total_cpus - total_cpus.
            # cpus_delta = signal_cpus / SETPOINT - total_cpus

            # We already have cpus_difference_from_setpoint = signal_cpus - SETPOINT * total_cpus.
            # Dividing by SETPOINT, we get signal_cpus / SETPOINT - total_cpus, which is the desired cpus_delta above.
            cpus_delta = cpus_difference_from_setpoint / SETPOINT

            # Finally, convert CPUs to capacity units.
            capacity_delta = cpus_delta / CPUS_PER_WEIGHT
            return self._constrain_cluster_delta(capacity_delta)
        return 0

    def _constrain_cluster_delta(self, delta):
        """ Signals can return arbitrary values, so make sure we don't add or remove too much capacity """
        if delta > 0:
            return min(
                self.role_config.read_int('defaults.max_capacity') - self.mesos_role_manager.target_capacity,
                self.role_config.read_int('defaults.max_weight_to_add'),
                delta,
            )
        elif delta < 0:
            return max(
                self.role_config.read_int('defaults.min_capacity') - self.mesos_role_manager.target_capacity,
                -self.role_config.read_int('defaults.max_weight_to_remove'),
                delta,
            )
        else:
            return 0
