import time

import arrow
import simplejson as json
import staticconf
import yelp_meteorite
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE
from staticconf.errors import ConfigurationError

from clusterman.autoscaler.util import evaluate_signal
from clusterman.autoscaler.util import load_signal_connection
from clusterman.autoscaler.util import read_signal_config
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.exceptions import SignalValidationError
from clusterman.mesos.constants import ROLE_NAMESPACE
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import get_clusterman_logger


DELTA_GAUGE_NAME = 'clusterman.autoscaler.delta'
logger = get_clusterman_logger(__name__)


class Autoscaler:
    def __init__(self, cluster, role, *, role_manager=None, metrics_client=None):
        self.cluster = cluster
        self.role = role

        logger.info(f'Initializing autoscaler engine for {self.role} in {self.cluster}...')
        self.role_config = staticconf.NamespaceReaders(ROLE_NAMESPACE.format(role=self.role))
        self.delta_gauge = yelp_meteorite.create_gauge(DELTA_GAUGE_NAME, {'cluster': cluster, 'role': role})

        self.mesos_role_manager = role_manager or MesosRoleManager(self.cluster, self.role)

        mesos_region = staticconf.read_string('aws.region')
        self.metrics_client = metrics_client or ClustermanMetricsBotoClient(mesos_region, app_identifier=self.role)
        self.load_signal()

        logger.info('Initialization complete')

    def time_to_next_activation(self, timestamp=None):
        timestamp = timestamp or time.time()
        period_seconds = self.signal_config.period_minutes * 60
        return period_seconds - timestamp % period_seconds

    def run(self, dry_run=False, timestamp=None):
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: Don't actually modify the fleet size, just print what would happen
        """
        timestamp = timestamp or arrow.utcnow()
        logger.info(f'Autoscaling run starting at {timestamp}')
        delta = self._compute_cluster_delta(timestamp)
        self.delta_gauge.set(delta, {'dry_run': dry_run})
        new_target_capacity = self.mesos_role_manager.target_capacity + delta
        self.mesos_role_manager.modify_target_capacity(new_target_capacity, dry_run=dry_run)

    def load_signal(self):
        """Load the signal object to use for autoscaling."""
        logger.info(f'Loading autoscaling signal for {self.role} in {self.cluster}')

        role_namespace = ROLE_NAMESPACE.format(role=self.role)
        self.signal_config = read_signal_config(DEFAULT_NAMESPACE)
        use_default = True
        try:
            # see if the role has set up a custom signal correctly; if not, fall back to the default
            # signal configuration (preloaded above)
            self.signal_config = read_signal_config(role_namespace)
            use_default = False
        except NoSignalConfiguredException:
            logger.info(f'No signal configured for {self.role}, falling back to default')
        except (ConfigurationError, SignalValidationError):
            logger.exception(f'Error loading signal for {self.role}, falling back to default')

        self._init_signal_connection(use_default)

    def _init_signal_connection(self, use_default):
        """ Initialize the signal socket connection/communication layer.

        :param use_default: use the default signal with whatever parameters are stored in self.signal_config
        """
        if not use_default:
            # Try to set up the (non-default) signal specified in the signal_config
            #
            # If it fails, it might be because the signal_config is requesting a different signal (or different
            # configuration) for a signal in the default role, so we fall back to the default in that case
            try:
                # Look for the signal name under the "role" directory in clusterman_signals
                self.signal_conn = load_signal_connection(self.signal_config.branch_or_tag, self.role, self.signal_config.name)
            except SignalConnectionError:
                # If it's not there, see if the signal is one of our default signals
                logger.info(f'Signal {self.signal_config.name} not found in {self.role}, checking default signals')
                use_default = True

        # This is not an "else" because the value of use_default may have changed in the above block
        if use_default:
            default_role = staticconf.read_string('autoscaling.default_signal_role')
            self.signal_conn = load_signal_connection(self.signal_config.branch_or_tag, default_role, self.signal_config.name)

        signal_kwargs = json.dumps({
            'cluster': self.cluster,
            'role': self.role,
            'parameters': self.signal_config.parameters
        })
        self.signal_conn.send(signal_kwargs.encode())
        logger.info(f'Loaded signal {self.signal_config.name}')

    def _get_metrics(self, end_time):
        metrics = {}
        for metric in self.signal_config.required_metrics:
            start_time = end_time.shift(minutes=-metric.minute_range)
            metric_key = (
                generate_key_with_dimensions(metric.name, {'cluster': self.cluster, 'role': self.role})
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

    def _compute_cluster_delta(self, timestamp):
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :returns: a capacity delta for the role
        """
        resource_request = evaluate_signal(self._get_metrics(timestamp), self.signal_conn)
        if resource_request['cpus'] is None:
            logger.info(f'No data from signal, not changing capacity')
            return 0
        signal_cpus = float(resource_request['cpus'])

        # Get autoscaling settings.
        setpoint = staticconf.read_float('autoscaling.setpoint')
        setpoint_margin = staticconf.read_float('autoscaling.setpoint_margin')
        cpus_per_weight = staticconf.read_int('autoscaling.cpus_per_weight')

        # If the percentage allocated differs by more than the allowable margin from the setpoint,
        # we scale up/down to reach the setpoint.  We want to use target_capacity here instead of
        # get_resource_total to protect against short-term fluctuations in the cluster.
        total_cpus = self.mesos_role_manager.target_capacity * cpus_per_weight
        cpus_difference_from_setpoint = signal_cpus - setpoint * total_cpus

        window_size = setpoint_margin * total_cpus
        lb, ub = total_cpus - window_size, total_cpus + window_size
        logger.info(f'Current CPU total is {total_cpus}; setpoint window is [{lb}, {ub}]')
        logger.info(f'Signal {self.signal_config.name} requested {signal_cpus} CPUs')
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
            capacity_delta = cpus_delta / cpus_per_weight
            logger.info(f'Computed capacity delta is {capacity_delta} units ({cpus_delta} CPUs)')
        else:
            logger.info('Requested CPUs within setpoint margin, capacity_delta is 0')
        return capacity_delta
