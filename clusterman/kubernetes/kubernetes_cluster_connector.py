from collections import defaultdict
from distutils.util import strtobool
from typing import List
from typing import Mapping

import colorlog
import kubernetes
import staticconf
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.kubernetes.util import allocated_node_resources
from clusterman.kubernetes.util import get_node_ip
from clusterman.kubernetes.util import total_node_resources

logger = colorlog.getLogger(__name__)


class KubernetesClusterConnector(ClusterConnector):
    _nodes_by_ip: Mapping[str, KubernetesNode]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]

    def __init__(self, cluster: str, pool: str) -> None:
        super().__init__(cluster, pool, 'kubernetes')
        kubernetes.config.load_kube_config(staticconf.read_string(f'clusters.{cluster}.kubeconfig_path'))
        self._core_api = kubernetes.client.CoreV1Api()
        self._safe_to_evict_annotation = staticconf.read_string(
            f'clusters.{cluster}.pod_safe_to_evict_annotation',
            default='cluster-autoscaler.kubernetes.io/safe-to-evict',
        )

    def reload_state(self) -> None:
        logger.info('Reloading nodes')
        self._nodes_by_ip = self._get_nodes_by_ip()
        self._pods_by_ip = self._get_pods_by_ip()

    def get_resource_allocation(self, resource_name: str) -> float:
        return sum(
            getattr(allocated_node_resources(self._pods_by_ip[node_ip]), resource_name)
            for node_ip, node in self._nodes_by_ip.items()
        )

    def get_resource_total(self, resource_name: str) -> float:
        return sum(
            getattr(total_node_resources(node), resource_name)
            for node in self._nodes_by_ip.values()
        )

    def _get_agent_metadata(self, node_ip: str) -> AgentMetadata:
        node = self._nodes_by_ip[node_ip]
        return AgentMetadata(
            agent_id=node.metadata.name,
            allocated_resources=allocated_node_resources(self._pods_by_ip[node_ip]),
            batch_task_count=self._count_batch_tasks(node_ip),
            state=(AgentState.RUNNING if self._pods_by_ip[node_ip] else AgentState.IDLE),
            task_count=len(self._pods_by_ip[node_ip]),
            total_resources=total_node_resources(node),
        )

    def _get_nodes_by_ip(self) -> Mapping[str, KubernetesNode]:
        pool_label_selector = self.pool_config.read_string('pool_label_key') + '=' + self.pool
        pool_nodes = self._core_api.list_node(label_selector=pool_label_selector).items
        return {
            get_node_ip(node): node
            for node in pool_nodes
        }

    def _get_pods_by_ip(self) -> Mapping[str, List[KubernetesPod]]:
        all_pods = self._core_api.list_pod_for_all_namespaces().items
        pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)
        for pod in all_pods:
            if pod.status.phase == 'Running' and pod.status.host_ip in self._nodes_by_ip:
                pods_by_ip[pod.status.host_ip].append(pod)
        return pods_by_ip

    def _count_batch_tasks(self, node_ip: str) -> int:
        count = 0
        for pod in self._pods_by_ip[node_ip]:
            for annotation, value in pod.metadata.annotations.items():
                if annotation == self._safe_to_evict_annotation:
                    count += (not strtobool(value))  # if it's safe to evict, it's NOT a batch task
                    break
        return count
