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
from typing import Set
from typing import Tuple

import arrow
import colorlog
import kubernetes
import staticconf
from kubernetes.client import V1beta1Eviction
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1ObjectMeta
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod
from kubernetes.client.rest import ApiException

from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import AgentState
from clusterman.kubernetes.util import allocated_node_resources
from clusterman.kubernetes.util import CachedCoreV1Api
from clusterman.kubernetes.util import ConciseCRDApi
from clusterman.kubernetes.util import get_node_ip
from clusterman.kubernetes.util import get_node_kernel_version
from clusterman.kubernetes.util import get_node_lsbrelease
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.kubernetes.util import selector_term_matches_requirement
from clusterman.kubernetes.util import total_node_resources
from clusterman.kubernetes.util import total_pod_resources
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import MigrationStatus
from clusterman.util import strtobool


logger = colorlog.getLogger(__name__)
KUBERNETES_SCHEDULED_PHASES = {"Pending", "Running"}
CLUSTERMAN_TERMINATION_TAINT_KEY = "clusterman.yelp.com/terminating"
MIGRATION_CRD_GROUP = "clusterman.yelp.com"
MIGRATION_CRD_VERSION = "v1"
MIGRATION_CRD_PLURAL = "nodemigrations"
MIGRATION_CRD_STATUS_LABEL = "clusterman.yelp.com/migration_status"
NOT_FOUND_STATUS = 404
# we don't want to block on eviction/deletion as we're potentially evicting/deleting a ton of pods
# AND there's a delay before we go ahead and terminate
# AND at Yelp we run a script on shutdown that will also try to drain one final time.
PROPAGATION_POLICY = "Background"


class KubernetesClusterConnector(ClusterConnector):
    SCHEDULER = "kubernetes"
    _core_api: kubernetes.client.CoreV1Api
    _migration_crd_api: Optional[kubernetes.client.CustomObjectsApi]
    _pods: List[KubernetesPod]
    _prev_nodes_by_ip: Mapping[str, KubernetesNode]
    _nodes_by_ip: Mapping[str, KubernetesNode]
    _unschedulable_pods: List[KubernetesPod]
    _excluded_pods_by_ip: Mapping[str, List[KubernetesPod]]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]

    def __init__(self, cluster: str, pool: Optional[str], init_crd: bool = False) -> None:
        super().__init__(cluster, pool)
        self.kubeconfig_path = staticconf.read_string(f"clusters.{cluster}.kubeconfig_path")
        self._safe_to_evict_annotation = staticconf.read_string(
            f"clusters.{cluster}.pod_safe_to_evict_annotation",
            default="cluster-autoscaler.kubernetes.io/safe-to-evict",
        )
        self._nodes_by_ip = {}
        self._init_crd_client = init_crd

    def reload_state(self) -> None:
        logger.info("Reloading nodes")

        self.reload_client()

        # store the previous _nodes_by_ip for use in get_removed_nodes_before_last_reload()
        self._prev_nodes_by_ip = copy.deepcopy(self._nodes_by_ip)
        self._nodes_by_ip = self._get_nodes_by_ip()
        logger.info("Reloading pods")
        (self._pods_by_ip, self._unschedulable_pods, self._excluded_pods_by_ip,) = (
            self._get_pods_info_with_label()
            if self.pool_config.read_bool("use_labels_for_pods", default=False)
            else self._get_pods_info()
        )

    def reload_client(self) -> None:
        self._core_api = CachedCoreV1Api(self.kubeconfig_path)
        self._migration_crd_api = (
            ConciseCRDApi(
                self.kubeconfig_path,
                group=MIGRATION_CRD_GROUP,
                version=MIGRATION_CRD_VERSION,
                plural=MIGRATION_CRD_PLURAL,
            )
            if self._init_crd_client
            else None
        )

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

    def freeze_agent(self, node_name: str) -> None:
        now = str(arrow.now().timestamp)
        try:
            body = {
                "spec": {"taints": [{"effect": "NoSchedule", "key": CLUSTERMAN_TERMINATION_TAINT_KEY, "value": now}]}
            }
            self._core_api.patch_node(node_name, body)
        except ApiException as e:
            logger.warning(f"Failed to freeze {node_name}: {e}")

    def drain_node(self, node_name: str, disable_eviction: bool) -> bool:
        try:
            logger.info(f"Cordoning {node_name}...")
            self.cordon_node(node_name)
        except Exception:
            logger.exception(f"Failed to cordon {node_name} - continuing to proceed anyway.")
        try:
            logger.info(f"Evicting/Deleting pods on {node_name}...")
            pods_on_node = [
                pod for pod in self._list_all_pods_on_node(node_name) if not self._pod_belongs_to_daemonset(pod)
            ]
            if not self._evict_or_delete_pods(node_name, pods_on_node, disable_eviction):
                logger.info(f"Some pods couldn't be evicted/deleted on {node_name}")
                return False
            logger.info(f"Drained {node_name}")
            return True
        except Exception as e:
            logger.warning(f"Failed to drain {node_name}: {e}")
            return False

    def cordon_node(self, node_name: str) -> bool:
        try:
            self._core_api.patch_node(
                name=node_name,
                body={
                    "spec": {
                        "unschedulable": True,
                    },
                },
            )
            return True
        except ApiException as e:
            logger.warning(f"Failed to cordon {node_name}: {e}")
            return False

    def uncordon_node(self, node_name: str) -> bool:
        try:
            self._core_api.patch_node(
                name=node_name,
                body={
                    "spec": {
                        "unschedulable": False,
                    },
                },
            )
            return True
        except ApiException as e:
            logger.warning(f"Failed to uncordon {node_name}: {e}")
            return False

    def list_node_migration_resources(self, statuses: List[MigrationStatus]) -> Set[MigrationEvent]:
        """Fetch node migration event resource from k8s CRD

        :param List[MigrationStatus] statuses: event status to look for
        :return: collection of migration events
        """
        assert self._migration_crd_api, "CRD client was not initialized"
        try:
            label_filter = ",".join(status.value for status in statuses)
            label_selector = f"{MIGRATION_CRD_STATUS_LABEL} in ({label_filter})"
            if self.pool:
                label_selector += f",{self.pool_label_key}={self.pool}"
            resources = self._migration_crd_api.list_cluster_custom_object(label_selector=label_selector)
            return set(map(MigrationEvent.from_crd, resources.get("items", [])))
        except Exception as e:
            logger.error(f"Failed fetching migration events: {e}")
        return set()

    def mark_node_migration_resource(self, event_name: str, status: MigrationStatus) -> None:
        """Set status for migration event resource

        :param str event_name: name of the resource CRD
        :status MigrationStatus status: status to be set
        """
        assert self._migration_crd_api, "CRD client was not initialized"
        try:
            self._migration_crd_api.patch_cluster_custom_object(
                name=event_name,
                body={
                    "metadata": {
                        "labels": {
                            MIGRATION_CRD_STATUS_LABEL: status.value,
                        }
                    }
                },
            )
        except Exception as e:
            logger.error(f"Failed updating migration event status: {e}")

    def create_node_migration_resource(
        self, event: MigrationEvent, status: MigrationStatus = MigrationStatus.PENDING
    ) -> None:
        """Create CRD resource for node migration

        :param MigrationEvent event: event to submit
        :param MigrationStatus status: event status (pending by default)
        """
        assert self._migration_crd_api, "CRD client was not initialized"
        try:
            body = event.to_crd_body(labels={MIGRATION_CRD_STATUS_LABEL: status.value, self.pool_label_key: event.pool})
            self._migration_crd_api.create_cluster_custom_object(body=body)
        except Exception as e:
            logger.error(f"Failed creating migration event resource: {e}")

    def _evict_or_delete_pods(self, node_name: str, pods: List[KubernetesPod], disable_eviction: bool) -> bool:
        all_done = True
        action_name = "deleted" if disable_eviction else "evicted"
        logger.info(f"{len(pods)} pods being {action_name} on {node_name}")
        for pod in pods:
            try:
                if disable_eviction:
                    self._delete_pod(pod)
                else:
                    self._evict_pod(pod)
                logger.info(f"{pod.metadata.name} ({pod.metadata.namespace}) was {action_name} on {node_name}")
            except ApiException as e:
                logger.warning(
                    f"{pod.metadata.name} ({pod.metadata.namespace}) couldn't be {action_name} on {node_name}"
                    f":{e.status}-{e.reason}"
                )
                if e.status != NOT_FOUND_STATUS:
                    all_done = False

        return all_done

    def _delete_pod(self, pod: KubernetesPod):
        self._core_api.delete_namespaced_pod(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            propagation_policy=PROPAGATION_POLICY,
        )

    def _evict_pod(self, pod: KubernetesPod):
        self._core_api.create_namespaced_pod_eviction(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            body=V1beta1Eviction(
                metadata=V1ObjectMeta(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                ),
                delete_options=V1DeleteOptions(
                    # we don't want to block on eviction as we're potentially evicting a ton of pods
                    # AND there's a delay before we go ahead and terminate
                    # AND at Yelp we run a script on shutdown that will also try to drain one final time.
                    propagation_policy=PROPAGATION_POLICY,
                ),
            ),
        )

    def _list_all_pods_on_node(self, node_name: str) -> List[KubernetesPod]:
        return self._core_api.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node_name}").items

    def _pod_belongs_to_daemonset(self, pod: KubernetesPod) -> bool:
        return pod.metadata.owner_references and any(
            [owner_reference.kind == "DaemonSet" for owner_reference in pod.metadata.owner_references]
        )

    def _pod_belongs_to_pool(self, pod: KubernetesPod) -> bool:
        # Check if the pod is on a node in the pool -- this should cover most cases
        if pod.status.phase in KUBERNETES_SCHEDULED_PHASES and pod.status.host_ip in self._nodes_by_ip:
            return True

        # Otherwise, check if the node selector matches the pool; we'll only get to either of the
        # following checks if the pod _should_ be running on the cluster, but isn't currently.  (This won't catch things
        # that have a nodeSelector or nodeAffinity for anything other than "pool name", for example, system-level
        # DaemonSets like kiam)
        if pod.spec.node_selector:
            for key, value in pod.spec.node_selector.items():
                if key == self.pool_label_key:
                    return value == self.pool

        # Lastly, check if an affinity rule matches
        selector_requirement = V1NodeSelectorRequirement(key=self.pool_label_key, operator="In", values=[self.pool])

        if pod.spec.affinity and pod.spec.affinity.node_affinity:
            node_affinity = pod.spec.affinity.node_affinity
            terms: List[V1NodeSelectorTerm] = []
            if node_affinity.required_during_scheduling_ignored_during_execution:
                terms.extend(node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms)
            if node_affinity.preferred_during_scheduling_ignored_during_execution:
                terms.extend(
                    [term.preference for term in node_affinity.preferred_during_scheduling_ignored_during_execution]
                )
            if selector_term_matches_requirement(terms, selector_requirement):
                return True
        return False

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
            kernel=get_node_kernel_version(node),
            lsbrelease=get_node_lsbrelease(node),
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
        # TODO(CLUSTERMAN-659): Switch to using just pool_label_key once the new node labels are applied everywhere
        node_label_selector = self.pool_config.read_string(
            "node_label_key", default=self.pool_config.read_string("pool_label_key", default="clusterman.com/pool")
        )
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
        all_pods = self._core_api.list_pod_for_all_namespaces().items
        for pod in all_pods:
            if self._pod_belongs_to_pool(pod):
                if exclude_daemonset_pods and self._pod_belongs_to_daemonset(pod):
                    excluded_pods_by_ip[pod.status.host_ip].append(pod)
                elif pod.status.phase == "Running" or self._is_recently_scheduled(pod):
                    pods_by_ip[pod.status.host_ip].append(pod)
                elif self._is_unschedulable(pod):
                    unschedulable_pods.append(pod)
                else:
                    logger.info(f"Skipping {pod.metadata.name} pod ({pod.status.phase})")
        return pods_by_ip, unschedulable_pods, excluded_pods_by_ip

    def _get_pods_info_with_label(
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
