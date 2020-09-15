# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import traceback
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import arrow
import colorlog
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import METADATA
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.config import get_autoscaling_config
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.interfaces.signal import Signal
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.monitoring_lib import get_monitoring_client
from clusterman.signals.external_signal import ExternalSignal
from clusterman.signals.pending_pods_signal import PendingPodsSignal
from clusterman.util import autoscaling_is_paused
from clusterman.util import ClustermanResources
from clusterman.util import get_cluster_dimensions
from clusterman.util import sensu_checkin
from clusterman.util import SignalResourceRequest
from clusterman.util import Status

SIGNAL_LOAD_CHECK_NAME = 'signal_configuration_failed'
TARGET_CAPACITY_GAUGE_NAME = 'clusterman.autoscaler.target_capacity_{resource}'
RESOURCE_GAUGE_BASE_NAME = 'clusterman.autoscaler.requested_{resource}'
logger = colorlog.getLogger(__name__)


class Autoscaler:
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        apps: List[str],
        pool_manager: Optional[PoolManager] = None,
        metrics_client: Optional[ClustermanMetricsBotoClient] = None,
        monitoring_enabled: bool = True,
    ) -> None:
        """ Class containing the core logic for autoscaling a cluster

        :param cluster: the name of the cluster to autoscale
        :param pool: the name of the pool to autoscale
        :param apps: a list of apps running on the pool
        :param pool_manager: a PoolManager object (used for simulations)
        :param metrics_client: a ClustermanMetricsBotoClient object (used for simulations)
        :param monitoring_enabled: set to False to disable sensu alerts during scaling
        """
        self.cluster = cluster
        self.pool = pool
        self.scheduler = scheduler
        self.apps = apps
        self.monitoring_enabled = monitoring_enabled

        # TODO: handle multiple apps in the autoscaler (CLUSTERMAN-126)
        if len(self.apps) > 1:
            raise NotImplementedError('Scaling multiple apps in a cluster is not yet supported')

        logger.info(f'Initializing autoscaler engine for {self.pool} in {self.cluster}...')

        gauge_dimensions = {'cluster': cluster, 'pool': pool}
        monitoring_client = get_monitoring_client()
        self.target_capacity_gauges: Dict[str, Any] = {}
        for resource in ClustermanResources._fields:
            self.target_capacity_gauges[resource] = monitoring_client.create_gauge(
                TARGET_CAPACITY_GAUGE_NAME.format(resource=resource),
                gauge_dimensions,
            )
        self.resource_request_gauges: Dict[str, Any] = {}
        for resource in SignalResourceRequest._fields:
            self.resource_request_gauges[resource] = monitoring_client.create_gauge(
                RESOURCE_GAUGE_BASE_NAME.format(resource=resource),
                gauge_dimensions,
            )

        self.autoscaling_config = get_autoscaling_config(
            POOL_NAMESPACE.format(pool=self.pool, scheduler=self.scheduler),
        )
        self.pool_manager = pool_manager or PoolManager(self.cluster, self.pool, self.scheduler)

        self.mesos_region = staticconf.read_string('aws.region')
        self.metrics_client = metrics_client or ClustermanMetricsBotoClient(self.mesos_region)
        self.default_signal: Signal
        if staticconf.read_bool('autoscale_signal.internal', default=False):
            # we should never get here unless we're on Kubernetes; this assert makes mypy happy
            assert isinstance(self.pool_manager.cluster_connector, KubernetesClusterConnector)
            self.default_signal = PendingPodsSignal(
                self.cluster,
                self.pool,
                self.scheduler,
                '__default__',
                DEFAULT_NAMESPACE,
                self.metrics_client,
                self.pool_manager.cluster_connector,
            )
        else:
            self.default_signal = ExternalSignal(
                self.cluster,
                self.pool,
                self.scheduler,
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
        if autoscaling_is_paused(self.cluster, self.pool, self.scheduler, timestamp):
            logger.info('Autoscaling is currently paused; doing nothing')
            return

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
        self.pool_manager.reload_state()
        if isinstance(resource_request, list):
            pass
        else:
            new_target_capacity = self._compute_target_capacity(resource_request)
            for resource in ClustermanResources._fields:
                self.target_capacity_gauges[resource].set(getattr(new_target_capacity, resource), {'dry_run': dry_run})
            self._emit_requested_resource_metrics(resource_request, dry_run=dry_run)

        self.pool_manager.modify_target_capacity(new_target_capacity, dry_run=dry_run)

        if exception:
            logger.error(f'The client signal failed with:\n{tb}')
            raise exception

    def _emit_requested_resource_metrics(self, resource_request: SignalResourceRequest, dry_run: bool) -> None:
        for resource_type, resource_gauge in self.resource_request_gauges.items():
            if getattr(resource_request, resource_type) is not None:
                resource_gauge.set(getattr(resource_request, resource_type), {'dry_run': dry_run})

    def _get_signal_for_app(self, app: str) -> Signal:
        """Load the signal object to use for autoscaling for a particular app

        :param app: the name of the app to load a Signal for
        :returns: the configured app signal, or the default signal in case of an error
        """
        logger.info(f'Loading autoscaling signal for {app} on {self.pool} in {self.cluster}')

        # TODO (CLUSTERMAN-126, CLUSTERMAN-195) apps will eventually have separate namespaces from pools
        pool_namespace = POOL_NAMESPACE.format(pool=app, scheduler=self.scheduler)

        try:
            # see if the pool has set up a custom signal correctly; if not, fall back to the default signal
            if staticconf.read_bool('autoscale_signal.internal', default=False, namespace=pool_namespace):
                # we should never get here unless we're on Kubernetes; this assert makes mypy happy
                assert isinstance(self.pool_manager.cluster_connector, KubernetesClusterConnector)
                return PendingPodsSignal(
                    self.cluster,
                    self.pool,
                    self.scheduler,
                    app,
                    pool_namespace,
                    self.metrics_client,
                    self.pool_manager.cluster_connector,
                )
            return ExternalSignal(
                self.cluster,
                self.pool,
                self.scheduler,
                app,
                pool_namespace,
                self.metrics_client,
                signal_namespace=staticconf.read_string(
                    'autoscale_signal.namespace',
                    default=app,
                    namespace=pool_namespace,
                ),
            )
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
                scheduler=self.scheduler,
                page=False,
                ttl=None,
                app=app,
                noop=not self.monitoring_enabled,
                pool=self.pool,
            )
            return self.default_signal

    def _compute_target_capacity(self, resource_request: SignalResourceRequest) -> ClustermanResources:
        """ Compare signal to the resources allocated and compute appropriate capacity change.

        :param resource_request: a resource_request object from the signal evaluation
        :returns: the new target capacity we should scale to
        """
        current_target_capacity = self.pool_manager.target_capacity
        cluster_total_resources = self._get_cluster_total_resources()
        cluster_allocated_resources = self._get_cluster_allocated_resources()
        non_orphan_fulfilled_capacity = self.pool_manager.non_orphan_fulfilled_capacity
        logger.info(f'Currently at target_capacity of {current_target_capacity}')
        logger.info(f'Currently non-orphan fulfilled capacity is {non_orphan_fulfilled_capacity}')
        logger.info(f'Current cluster total resources: {cluster_total_resources}')
        logger.info(f'Current cluster allocated resources: {cluster_allocated_resources}')

        # This block of code is kinda complicated logic for figuring out what happens if the cluster
        # or the resource request is empty.  There are essentially four checks, as follows:
        #
        # 1. If the resource request is all 'None', this is shorthand for "don't change the cluster"
        #
        # 2. Otherwise if the resource request contains 0s and Nones, this is a "real" zero request,
        #    so set the target capacity to zero
        #
        # 3. If we have a non-zero resource request but the cluster is empty, we scale up by the amount requested.
        #
        # 4. If the resource request and the target capacity are non-zero, but the nodes haven't joined
        #    the cluster yet, we just need to wait until they join before doing anything else.

        if all(requested_quantity is None for requested_quantity in resource_request):
            logger.info('No data from signal, not changing capacity')
            return current_target_capacity
        elif all(requested_quantity in {0, None} for requested_quantity in resource_request):
            return ClustermanResources()
        elif non_orphan_fulfilled_capacity == ClustermanResources():
            # Entering the main body of this method with non_orphan_fulfilled_capacity = 0 guarantees that
            # new_target_capacity will be 0, which we do not want (since the resource request is non-zero)
            logger.info(
                'Non-orphan fulfilled capacity is 0 and current target capacity > 0, not changing target to let the '
                'new instances join'
            )
            return current_target_capacity

        new_target_capacity = ClustermanResources(
            cpus=0 if resource_request.cpus is None else resource_request.cpus,
            mem=0 if resource_request.mem is None else resource_request.mem,
            disk=0 if resource_request.disk is None else resource_request.disk,
            gpus=0 if resource_request.gpus is None else resource_request.gpus,
        ) / self.autoscaling_config.setpoint

        # If the percentage change between current target capacity and the new target capacity is more than the
        # allowable margin we scale up/down to reach the setpoint. We want to use target_capacity here instead of
        # get_resource_total to protect against short-term fluctuations in the cluster.
        target_capacity_change = new_target_capacity - current_target_capacity
        margin = self.autoscaling_config.target_capacity_margin
        margin_amount = current_target_capacity * margin
        if target_capacity_change.any_gt(margin_amount) or (-1 * target_capacity_change).any_gt(margin_amount):
            logger.info(
                f'Percentage change between current and new target capacities is greater than margin {margin_amount} '
                f'({margin} of current capacity). '
                f'Scaling to {new_target_capacity}.'
            )
        else:
            logger.info(
                f'We are within our target capacity margin ({margin}). Not changing target capacity.'
            )
            new_target_capacity = current_target_capacity

        return new_target_capacity

    def _get_cluster_total_resources(self) -> ClustermanResources:
        total_resources = {
            resource: self.pool_manager.cluster_connector.get_resource_total(resource)
            for resource in ClustermanResources._fields
        }
        return ClustermanResources(**total_resources)

    def _get_cluster_allocated_resources(self) -> ClustermanResources:
        allocated_resources = {
            resource: self.pool_manager.cluster_connector.get_resource_allocation(resource)
            for resource in ClustermanResources._fields
        }
        return ClustermanResources(**allocated_resources)

    def _get_smoothed_non_zero_metadata(
        self,
        metric_name: str,
        time_start: arrow.Arrow,
        time_end: arrow.Arrow,
        smoothing: int = 5,
    ) -> Optional[Tuple[int, int, float]]:
        """ Compute some smoothed-out historical metrics metadata

        :param metric_name: the metadata metric to query
        :param time_start: the beginning of the historical time window to query
        :param time_end: the end of the historical time window to query
        :param smoothing: take this many non-zero metric values and average them together
        :returns: the start and end times over which the average was taken, and smoothed-out metric value during this
            time period; or None, if no historical data exists
        """
        metrics = self.metrics_client.get_metric_values(
            metric_name,
            METADATA,
            time_start,
            time_end,
            extra_dimensions=get_cluster_dimensions(self.cluster, self.pool, self.scheduler),
        )[metric_name]
        latest_non_zero_values = [(ts, val) for ts, val in metrics if val > 0][-smoothing:]
        if not latest_non_zero_values:
            return None
        return (
            latest_non_zero_values[0][0],
            latest_non_zero_values[-1][0],
            sum([float(val) for __, val in latest_non_zero_values]) / len(latest_non_zero_values),
        )
