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
import copy
from collections import defaultdict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import arrow
import colorlog
import kubernetes
import staticconf
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod
from kubernetes.client.rest import ApiException

from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import AgentState
from clusterman.kubernetes.util import allocated_node_resources
from clusterman.kubernetes.util import CachedCoreV1Api
from clusterman.kubernetes.util import get_node_ip
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.kubernetes.util import total_node_resources
from clusterman.kubernetes.util import total_pod_resources
from clusterman.util import strtobool


logger = colorlog.getLogger(__name__)
KUBERNETES_SCHEDULED_PHASES = {"Pending", "Running"}
CLUSTERMAN_TERMINATION_TAINT_KEY = "clusterman.yelp.com/terminating"


class KubernetesClusterConnector(ClusterConnector):
    SCHEDULER = "kubernetes"
    _core_api: kubernetes.client.CoreV1Api
    _pods: List[KubernetesPod]
    _prev_nodes_by_ip: Mapping[str, KubernetesNode]
    _nodes_by_ip: Mapping[str, KubernetesNode]
    _unschedulable_pods: List[KubernetesPod]
    _excluded_pods_by_ip: Mapping[str, List[KubernetesPod]]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]

    def __init__(self, cluster: str, pool: Optional[str]) -> None:
        super().__init__(cluster, pool)
        self.kubeconfig_path = staticconf.read_string(f"clusters.{cluster}.kubeconfig_path")
        self._safe_to_evict_annotation = staticconf.read_string(
            f"clusters.{cluster}.pod_safe_to_evict_annotation",
            default="cluster-autoscaler.kubernetes.io/safe-to-evict",
        )
        self._nodes_by_ip = {}

    def reload_state(self) -> None:
        logger.info("Reloading nodes")

        self._core_api = CachedCoreV1Api(self.kubeconfig_path)

        # store the previous _nodes_by_ip for use in get_removed_nodes_before_last_reload()
        self._prev_nodes_by_ip = copy.deepcopy(self._nodes_by_ip)
        self._nodes_by_ip = self._get_nodes_by_ip()
        logger.info("Reloading pods")
        (
            self._pods_by_ip,
            self._unschedulable_pods,
            self._excluded_pods_by_ip,
        ) = self._get_pods_info()

    def get_num_removed_nodes_before_last_reload(self) -> int:
        previous_nodes = self._prev_nodes_by_ip
        current_nodes = self._nodes_by_ip

        return max(0, len(previous_nodes) - len(current_nodes))

    def get_resource_pending(self, resource_name: str) -> float:
        return getattr(
            allocated_node_resources([p for p, __ in self.get_unschedulable_pods()]),
            resource_name,
        )

    def get_resource_allocation(self, resource_name: str) -> float:
        return sum(getattr(allocated_node_resources(pod), resource_name) for pod in self._pods_by_ip.values())

    def get_resource_total(self, resource_name: str) -> float:
        if self._excluded_pods_by_ip:
            logger.info(f"Excluded {self.get_resource_excluded(resource_name)} {resource_name} from daemonset pods")
        return sum(
            getattr(
                total_node_resources(node, self._excluded_pods_by_ip.get(node_ip, [])),
                resource_name,
            )
            for node_ip, node in self._nodes_by_ip.items()
        )

    def get_resource_excluded(self, resource_name: str) -> float:
        return sum(
            getattr(
                allocated_node_resources(self._excluded_pods_by_ip.get(node_ip, [])),
                resource_name,
            )
            for node_ip in self._nodes_by_ip.keys()
        )

    def get_unschedulable_pods(
        self,
    ) -> List[Tuple[KubernetesPod, PodUnschedulableReason]]:
        unschedulable_pods = []
        for pod in self._unschedulable_pods:
            unschedulable_pods.append((pod, self._get_pod_unschedulable_reason(pod)))
        return unschedulable_pods

    def freeze_agent(self, agent_id: str) -> None:
        now = str(arrow.now().timestamp)
        try:
            body = {
                "spec": {"taints": [{"effect": "NoSchedule", "key": CLUSTERMAN_TERMINATION_TAINT_KEY, "value": now}]}
            }
            self._core_api.patch_node(agent_id, body)
        except ApiException as e:
            logger.warning(f"Failed to freeze {agent_id}: {e}")

    def _pod_belongs_to_daemonset(self, pod: KubernetesPod) -> bool:
        return pod.metadata.owner_references and any(
            [owner_reference.kind == "DaemonSet" for owner_reference in pod.metadata.owner_references]
        )

    def _get_pod_unschedulable_reason(self, pod: KubernetesPod) -> PodUnschedulableReason:
        pod_resource_request = total_pod_resources(pod)
        for node_ip, pods_on_node in self._pods_by_ip.items():
            node = self._nodes_by_ip.get(node_ip)
            if node:
                available_node_resources = total_node_resources(
                    node, self._excluded_pods_by_ip.get(node_ip, [])
                ) - allocated_node_resources(pods_on_node)
                if pod_resource_request < available_node_resources:
                    return PodUnschedulableReason.Unknown

        return PodUnschedulableReason.InsufficientResources

    def _get_agent_metadata(self, node_ip: str) -> AgentMetadata:
        node = self._nodes_by_ip.get(node_ip)
        if not node:
            return AgentMetadata(state=AgentState.ORPHANED)
        return AgentMetadata(
            agent_id=node.metadata.name,
            allocated_resources=allocated_node_resources(self._pods_by_ip[node_ip]),
            is_safe_to_kill=self._is_node_safe_to_kill(node_ip),
            is_frozen=self._is_node_frozen(node),
            batch_task_count=self._count_batch_tasks(node_ip),
            state=(AgentState.RUNNING if self._pods_by_ip[node_ip] else AgentState.IDLE),
            task_count=len(self._pods_by_ip[node_ip]),
            total_resources=total_node_resources(node, self._excluded_pods_by_ip.get(node_ip, [])),
        )

    def _is_node_frozen(self, node: KubernetesNode) -> bool:
        if not node.spec:
            return False

        if node.spec.taints:
            for taint in node.spec.taints:
                if taint.key == CLUSTERMAN_TERMINATION_TAINT_KEY:
                    return True
        return False

    def _is_node_safe_to_kill(self, node_ip: str) -> bool:
        for pod in self._pods_by_ip[node_ip]:
            annotations = pod.metadata.annotations or dict()
            pod_safe_to_evict = strtobool(annotations.get(self.safe_to_evict_key, "true"))
            if not pod_safe_to_evict:
                return False
        return True

    def _get_nodes_by_ip(self) -> Mapping[str, KubernetesNode]:
        use_different_label_for_nodes = self.pool_config.read_string("use_different_label_for_nodes", default=False)

        if use_different_label_for_nodes:
            node_label_selector = self.pool_config.read_string("node_label_key", default="clusterman.com/pool")
        else:
            node_label_selector = self.pool_config.read_string("pool_label_key", default="clusterman.com/pool")

        label_selector = f"{node_label_selector}={self.pool}"
        pool_nodes = self._core_api.list_node(label_selector=label_selector).items
        return {get_node_ip(node): node for node in pool_nodes}

    def _get_pods_info(
        self,
    ) -> Tuple[Mapping[str, List[KubernetesPod]], List[KubernetesPod], Mapping[str, List[KubernetesPod]],]:
        pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)
        unschedulable_pods: List[KubernetesPod] = []
        excluded_pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)

        exclude_daemonset_pods = self.pool_config.read_bool(
            "exclude_daemonset_pods",
            default=staticconf.read_bool("exclude_daemonset_pods", default=False),
        )
        label_selector = f"{self.pool_label_key}={self.pool}"

        for pod in self._core_api.list_pod_for_all_namespaces(label_selector=label_selector).items:
            if exclude_daemonset_pods and self._pod_belongs_to_daemonset(pod):
                excluded_pods_by_ip[pod.status.host_ip].append(pod)
            elif pod.status.phase == "Running" or self._is_recently_scheduled(pod):
                pods_by_ip[pod.status.host_ip].append(pod)
            elif self._is_unschedulable(pod):
                unschedulable_pods.append(pod)
            else:
                logger.info(f"Skipping {pod.metadata.name} pod ({pod.status.phase})")
        return pods_by_ip, unschedulable_pods, excluded_pods_by_ip

    def _count_batch_tasks(self, node_ip: str) -> int:
        count = 0
        for pod in self._pods_by_ip[node_ip]:
            if pod.metadata.annotations is None:
                continue
            for annotation, value in pod.metadata.annotations.items():
                if annotation == self._safe_to_evict_annotation:
                    count += not strtobool(value)  # if it's safe to evict, it's NOT a batch task
                    break
        return count

    def _is_recently_scheduled(self, pod: KubernetesPod) -> bool:
        # To find pods which in pending phase but already scheduled to the node.
        # The phase of these pods is changed to running asap,
        # Therefore, we should consider these pods for our next steps.
        if pod.status.phase != "Pending":
            return False
        if not pod.status or not pod.status.conditions:
            return False
        for condition in pod.status.conditions:
            if condition.type == "PodScheduled" and condition.status == "True":
                return True
        return False

    def _is_unschedulable(self, pod: KubernetesPod) -> bool:
        if pod.status.phase != "Pending":
            return False
        if not pod.status or not pod.status.conditions:
            return False
        for condition in pod.status.conditions:
            if condition.type == "PodScheduled" and condition.reason == "Unschedulable":
                return True
        return False

    @property
    def pool_label_key(self):
        return self.pool_config.read_string("pool_label_key", default="clusterman.com/pool")

    @property
    def safe_to_evict_key(self):
        return self.pool_config.read_string("safe_to_evict_key", default="clusterman.com/safe_to_evict")
