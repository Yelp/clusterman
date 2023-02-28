from typing import List
from typing import Optional
from typing import Union

import arrow
import colorlog
from clusterman_metrics import ClustermanMetricsBotoClient
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.autoscaler.config import get_autoscaling_config
from clusterman.interfaces.signal import Signal
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.kubernetes.util import total_pod_resources
from clusterman.util import ClustermanResources
from clusterman.util import SignalResourceRequest

logger = colorlog.getLogger(__name__)


class PendingPodsSignal(Signal):
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        app: str,
        config_namespace: str,
        metrics_client: ClustermanMetricsBotoClient,
        cluster_connector: KubernetesClusterConnector,
    ) -> None:
        super().__init__(self.__class__.__name__, cluster, pool, scheduler, app, config_namespace)
        self.cluster_connector = cluster_connector
        self.config_namespace = config_namespace

    def evaluate(
        self,
        timestamp: arrow.Arrow,
        retry_on_broken_pipe: bool = True,
    ) -> Union[SignalResourceRequest, List[KubernetesPod]]:
        allocated_resources = self.cluster_connector.get_cluster_allocated_resources()
        pending_pods = self.cluster_connector.get_unschedulable_pods()

        # Get the most recent metrics _now_ and when the boost was set (if any) and merge them
        if self.parameters.get("per_pod_resource_requests"):
            return pending_pods
        elif self.parameters.get("v2", True):
            total_resources = self.cluster_connector.get_cluster_total_resources()
            autoscaling_config = get_autoscaling_config(self.config_namespace)
            return self._get_resource_request_v2(
                allocated_resources,
                total_resources,
                autoscaling_config.target_capacity_margin,
                pending_pods,
            )
        else:
            return self._get_resource_request(allocated_resources, pending_pods)

    def _get_resource_request(
        self,
        allocated_resources: ClustermanResources,
        pending_pods: Optional[List[KubernetesPod]] = None,
    ) -> SignalResourceRequest:
        """Given a list of metrics, construct a resource request based on the most recent
        data for allocated and pending pods"""

        multiplier = self.parameters.get("pending_pods_multiplier", 2)

        resource_request = SignalResourceRequest()
        pending_pods = pending_pods or []
        if pending_pods:
            for pod in pending_pods:
                # This is a temporary measure to try to improve scaling behaviour when Clusterman thinks
                # there are enough resources but no single box can hold a new pod.  The goal is to replace
                # this with a more intelligent solution in the future.
                resource_request += total_pod_resources(pod) * multiplier
            logger.info(f"Pending pods adding resource request: {resource_request} (multiplier {multiplier})")

        return resource_request + allocated_resources

    def _get_resource_request_v2(
        self,
        allocated_resources: ClustermanResources,
        total_resources: ClustermanResources,
        target_capacity_margin: float,
        pending_pods: Optional[List[KubernetesPod]] = None,
    ) -> SignalResourceRequest:
        """Given a list of metrics, construct a resource request based on the most recent
        data for allocated and pending pods"""

        multiplier = self.parameters.get("pending_pods_multiplier", 2)

        resource_request = SignalResourceRequest()
        pending_pods = pending_pods or []

        if len(pending_pods) > 0:
            for pod in pending_pods:
                resource_request += total_pod_resources(pod) * multiplier
            min_resources_to_bump = SignalResourceRequest(*total_resources * target_capacity_margin)

            # We want to be sure that clusterman will bump capacity if there is any pendings pods
            resources_to_add = max(min_resources_to_bump, resource_request)

            return resources_to_add + total_resources
        else:
            return SignalResourceRequest(*allocated_resources)
