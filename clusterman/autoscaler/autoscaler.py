import traceback
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import arrow
import colorlog
import staticconf
import yelp_meteorite
from clusterman_metrics import ClustermanMetricsBotoClient
from pysensu_yelp import Status
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.config import get_autoscaling_config
from clusterman.autoscaler.signals import Signal
from clusterman.autoscaler.signals import SignalResponseDict
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.util import sensu_checkin

SIGNAL_LOAD_CHECK_NAME = 'signal_configuration_failed'
TARGET_CAPACITY_GAUGE_NAME = 'clusterman.autoscaler.target_capacity'
RESOURCE_GAUGE_BASE_NAME = 'clusterman.autoscaler.requested_{resource}'
logger = colorlog.getLogger(__name__)


class Autoscaler:
    def __init__(
        self,
        cluster: str,
        pool: str,
        apps: List[str],
        *,
        pool_manager: Optional[MesosPoolManager] = None,
        metrics_client: Optional[ClustermanMetricsBotoClient] = None,
        monitoring_enabled: bool = True,
    ) -> None:
        """ Class containing the core logic for autoscaling a cluster

        :param cluster: the name of the cluster to autoscale
        :param pool: the name of the pool to autoscale
        :param apps: a list of apps running on the pool
        :param pool_manager: a MesosPoolManager object (used for simulations)
        :param metrics_client: a ClustermanMetricsBotoClient object (used for simulations)
        :param monitoring_enabled: set to False to disable sensu alerts during scaling
        """
        self.cluster = cluster
        self.pool = pool
        self.apps = apps
        self.monitoring_enabled = monitoring_enabled

        # TODO: handle multiple apps in the autoscaler (CLUSTERMAN-126)
        if len(self.apps) > 1:
            raise NotImplementedError('Scaling multiple apps in a cluster is not yet supported')

        logger.info(f'Initializing autoscaler engine for {self.pool} in {self.cluster}...')

        gauge_dimensions = {'cluster': cluster, 'pool': pool}
        self.target_capacity_gauge = yelp_meteorite.create_gauge(TARGET_CAPACITY_GAUGE_NAME, gauge_dimensions)
        self.resource_request_gauges: Dict[str, yelp_meteorite.metrics.Gauge] = {}
        for resource in ('cpus', 'mem', 'disk'):
            self.resource_request_gauges[resource] = yelp_meteorite.create_gauge(
                RESOURCE_GAUGE_BASE_NAME.format(resource=resource),
                gauge_dimensions,
            )

        self.autoscaling_config = get_autoscaling_config(POOL_NAMESPACE.format(pool=self.pool))
        self.mesos_pool_manager = pool_manager or MesosPoolManager(self.cluster, self.pool)

        self.mesos_region = staticconf.read_string('aws.region')
        self.metrics_client = metrics_client or ClustermanMetricsBotoClient(self.mesos_region)
        self.default_signal = Signal(
            self.cluster,
            self.pool,
            '__default__',
            DEFAULT_NAMESPACE,
            self.metrics_client,
            signal_namespace=staticconf.read_string('autoscaling.default_signal_role'),
        )
        self.signal = self._get_signal_for_app(self.apps[0])
        logger.info('Initialization complete')

    @property
    def run_frequency(self) -> int:
        return self.signal.period_minutes * 60

    def run(self, dry_run: bool = False, timestamp: Optional[arrow.Arrow] = None) -> None:
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: boolean; if True, don't modify the pool size, just print what would happen
        :param timestamp: an arrow object indicating the current time
        """
        timestamp = timestamp or arrow.utcnow()
        logger.info(f'Autoscaling run starting at {timestamp}')

        try:
            signal_name = self.signal.name
            resource_request = self.signal.evaluate(timestamp)
            exception = None
        except Exception as e:
            logger.error(f'Client signal {self.signal.name} failed; using default signal')
            signal_name = self.default_signal.name
            resource_request = self.default_signal.evaluate(timestamp)
            exception, tb = e, traceback.format_exc()

        logger.info(f'Signal {signal_name} requested {resource_request}')
        self.mesos_pool_manager.reload_state()
        new_target_capacity = self._compute_target_capacity(resource_request)

        self.target_capacity_gauge.set(new_target_capacity, {'dry_run': dry_run})
        self._emit_requested_resource_metrics(resource_request, dry_run=dry_run)

        self.mesos_pool_manager.modify_target_capacity(new_target_capacity, dry_run=dry_run)

        if exception:
            logger.error(f'The client signal failed with:\n{tb}')
            raise exception

    def _emit_requested_resource_metrics(self, resource_request: SignalResponseDict, dry_run: bool) -> None:
        for resource_type, resource_gauge in self.resource_request_gauges.items():
            if resource_type in resource_request and resource_request[resource_type] is not None:
                resource_gauge.set(resource_request[resource_type], {'dry_run': dry_run})

    def _get_signal_for_app(self, app: str) -> Signal:
        """Load the signal object to use for autoscaling for a particular app

        :param app: the name of the app to load a Signal for
        :returns: the configured app signal, or the default signal in case of an error
        """
        logger.info(f'Loading autoscaling signal for {app} on {self.pool} in {self.cluster}')

        # TODO (CLUSTERMAN-126, CLUSTERMAN-195) apps will eventually have separate namespaces from pools
        pool_namespace = POOL_NAMESPACE.format(pool=app)
        signal_namespace = staticconf.get_string('autoscale_signal.namespace', default=app, namespace=pool_namespace)

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
                noop=not self.monitoring_enabled,
                pool=self.pool,
            )
            return self.default_signal

    def _compute_target_capacity(self, resource_request: SignalResponseDict) -> float:
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :param resource_request: a resource_request object from the signal evaluation
        :returns: the new target capacity we should scale to
        """
        current_target_capacity = self.mesos_pool_manager.target_capacity
        non_orphan_fulfilled_capacity = self.mesos_pool_manager.non_orphan_fulfilled_capacity
        logger.info(f'Currently at target_capacity of {current_target_capacity}')

        if all(requested_quantity is None for requested_quantity in resource_request.values()):
            logger.info('No data from signal, not changing capacity')
            return current_target_capacity
        elif all(requested_quantity == 0 for requested_quantity in resource_request.values()):
            return 0
        elif current_target_capacity == 0:
            logger.info(
                'Current target capacity is 0 and we received a non-zero resource request, scaling up by 1 to get '
                'some data'
            )
            return 1
        elif non_orphan_fulfilled_capacity == 0:
            # Entering the main body of this method with non_orphan_fulfilled_capacity = 0 guarantees that
            # new_target_capacity will be 0, which we do not want (since the resource request is non-zero)
            logger.info(
                'Non-orphan fulfilled capacity is 0 and current target capacity > 0, not changing target to let the '
                'new instances join'
            )
            return current_target_capacity

        most_constrained_resource, usage_pct = self._get_most_constrained_resource_for_request(resource_request)
        logger.info(
            f'Fulfilling resource request will cause {most_constrained_resource} to be the most constrained resource '
            f'at {usage_pct} usage'
        )

        # We want to scale the cluster so that requested / (total * scale_factor) = setpoint.
        # We already have requested/total in the form of usage_pct, so we can solve for scale_factor:
        scale_factor = usage_pct / self.autoscaling_config.setpoint

        # Because we scale by the percentage of the "most fulfilled resource" we want to make sure that the
        # target capacity change is based on what's currently present.  A simple example illustrates the point:
        #
        #   * Suppose we have target_capacity = 50, fulfilled_capacity = 10, and setpoint = 0.5
        #   * The signal requests 100 CPUs, and Mesos says there are 200 CPUs in the cluster (this is the
        #       non_orphan_fulfilled_capacity)
        #   * The new target capacity in this case should be 10, not 100 (as it would be if we scaled off the
        #       current target_capacity)
        #
        # This also ensures that the right behavior happens when rolling a resource group.  To see this, let
        # X be the target_capacity of the original resource group; if we create the new resource group with target
        # capacity X, then our non_orphan_fulfilled_capacity will (eventually) be 2X and our scale_factor will be
        # (setpoint / 2) / setpoint (assuming the utilization doesn't change), so our new target_capacity will be X.
        # Since stale resource groups have a target_capacity of 0 and aren't included in modify_target_capacity
        # calculations, this ensures the correct behaviour.  The math here continues to work out as the old resource
        # group scales down, because as the fulfilled_capacity decreases, the scale_factor increases by the same
        # amount.  Tada!
        new_target_capacity = non_orphan_fulfilled_capacity * scale_factor

        # If the percentage requested differs by more than the allowable margin from the setpoint,
        # we scale up/down to reach the setpoint.  We want to use target_capacity here instead of
        # get_resource_total to protect against short-term fluctuations in the cluster.
        setpoint_distance = abs(new_target_capacity - current_target_capacity) / current_target_capacity
        logger.info(f'Distance from setpoint of {self.autoscaling_config.setpoint}: {setpoint_distance}')
        margin = self.autoscaling_config.setpoint_margin
        if setpoint_distance >= margin:
            logger.info(
                f'Setpoint distance is greater than setpoint margin ({margin}). Scaling to {new_target_capacity}.'
            )
        else:
            logger.info(
                f'We are within our setpoint margin ({margin}). Not changing target capacity.'
            )
            new_target_capacity = current_target_capacity

        return new_target_capacity

    def _get_most_constrained_resource_for_request(self, resource_request: SignalResponseDict) -> Tuple[str, float]:
        """Determine what would be the most constrained resource if were to fulfill a resource_request without scaling
        the cluster.

        :param resource_rquest: dictionary of resource name (cpu, mem, disk) to the requested quantity of that resource
        :returns: a tuple of the most constrained resource name and its utilization percentage if the provided request
            were to be fulfilled
        """
        requested_resource_usage_pcts = {}
        for resource in ('cpus', 'mem', 'disk'):
            if resource not in resource_request or resource_request[resource] is None:
                continue

            resource_total = self.mesos_pool_manager.get_resource_total(resource)
            # mypy isn't smart enough to see resource_request[resource] can't be None here
            requested_resource_usage_pcts[resource] = resource_request[resource] / resource_total  # type: ignore
        return max(requested_resource_usage_pcts.items(), key=lambda x: x[1])
