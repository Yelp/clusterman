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
from clusterman.kubernetes.util import total_node_resources
from clusterman.kubernetes.util import total_pod_resources
from clusterman.migration.constants import MIGRATION_CRD_ATTEMPTS_LABEL
from clusterman.migration.constants import MIGRATION_CRD_GROUP
from clusterman.migration.constants import MIGRATION_CRD_KIND
from clusterman.migration.constants import MIGRATION_CRD_PLURAL
from clusterman.migration.constants import MIGRATION_CRD_STATUS_LABEL
from clusterman.migration.constants import MIGRATION_CRD_VERSION
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import MigrationStatus
from clusterman.util import ClustermanResources
from clusterman.util import strtobool


logger = colorlog.getLogger(__name__)
KUBERNETES_SCHEDULED_PHASES = {"Pending", "Running"}
CLUSTERMAN_TERMINATION_TAINT_KEY = "clusterman.yelp.com/terminating"
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
    _excluded_pods: List[KubernetesPod]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]
    _label_selectors: List[str]
    _unschedulable_pods_resources: ClustermanResources
    _allocated_pods_resources: ClustermanResources

    def __init__(self, cluster: str, pool: Optional[str], init_crd: bool = False) -> None:
        super().__init__(cluster, pool)
        self.kubeconfig_path = staticconf.read_string(f"clusters.{cluster}.kubeconfig_path")
        self._safe_to_evict_annotation = staticconf.read_string(
            f"clusters.{cluster}.pod_safe_to_evict_annotation",
            default="cluster-autoscaler.kubernetes.io/safe-to-evict",
        )
        self._unschedulable_pods_resources = ClustermanResources()
        self._allocated_pods_resources = ClustermanResources()
        self._nodes_by_ip = {}
        self._init_crd_client = init_crd
        self._label_selectors = []
        if self.pool:
            # TODO(CLUSTERMAN-659): Switch to using just pool_label_key once the new node labels are applied everywhere
            node_label_selector = self.pool_config.read_string("node_label_key", default="clusterman.com/pool")
            self._label_selectors.append(f"{node_label_selector}={self.pool}")

    def reload_state(self, load_pods_info: bool = True) -> None:
        """Reload information from cluster/pool

        :param bool load_pods_info: do not load data about pods.
                                    NOTE: all resouce utilization metrics won't be available when setting this
        """
        logger.info("Reloading nodes")

        self.reload_client()

        # store the previous _nodes_by_ip for use in get_removed_nodes_before_last_reload()
        self._prev_nodes_by_ip = copy.deepcopy(self._nodes_by_ip)
        self._nodes_by_ip = self._get_nodes_by_ip()
        logger.info(f"Successfully reloaded {len(self._nodes_by_ip)} nodes.")

        if load_pods_info:
            logger.info("Reloading pods")
            (
                self._pods_by_ip,
                self._unschedulable_pods,
                self._unschedulable_pods_resources,
                self._allocated_pods_resources,
            ) = self._get_pods_info_with_label()
            self._excluded_pods = self._get_excluded_pods()

            pods_by_ip_count = sum(len(self._pods_by_ip[ip]) for ip in self._pods_by_ip)
            excluded_pods_count = len(self._excluded_pods)
            logger.info(
                f"Successfully reloaded pods: {pods_by_ip_count} running/recently scheduled pods, "
                f"{len(self._unschedulable_pods)} unscheduled pods, {excluded_pods_count} excluded pods (per node)."
            )

        else:
            self._pods_by_ip, self._unschedulable_pods, self._excluded_pods = (
                dict.fromkeys(self._nodes_by_ip, []),
                [],
                [],
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

    def set_label_selectors(self, label_selectors: List[str], add_to_existing: bool = False) -> None:
        """Set label selectors for node listing purposes

        :param List[str] label_selectors: list of selectors (joined with logic and)
        :param bool add_to_existing: if set add to existing selectors rather than replacing
        """
        self._label_selectors = sorted(
            (set(self._label_selectors) | set(label_selectors)) if add_to_existing else set(label_selectors)
        )

    def get_num_removed_nodes_before_last_reload(self) -> int:
        previous_nodes = self._prev_nodes_by_ip
        current_nodes = self._nodes_by_ip

        return max(0, len(previous_nodes) - len(current_nodes))

    def get_resource_pending(self, resource_name: str) -> float:
        return getattr(
            self._unschedulable_pods_resources,
            resource_name,
        )

    def get_resource_allocation(self, resource_name: str) -> float:
        return getattr(self._allocated_pods_resources, resource_name)

    def get_resource_total(self, resource_name: str) -> float:
        if self._excluded_pods:
            logger.info(f"Excluded {self.get_resource_excluded(resource_name)} {resource_name} from daemonset pods")
        return sum(
            getattr(
                total_node_resources(node, self._excluded_pods),
                resource_name,
            )
            for node_ip, node in self._nodes_by_ip.items()
        )

    def get_resource_excluded(self, resource_name: str) -> float:
        return getattr(allocated_node_resources(self._excluded_pods), resource_name) * len(self._nodes_by_ip)

    def get_unschedulable_pods(self) -> List[KubernetesPod]:
        return self._unschedulable_pods

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
        now = str(arrow.now().timestamp)
        try:
            node = self._core_api.read_node(node_name)
            if not node:
                logger.warning(f"Node doesn't exist: {node_name}")
                return False
            taints = (
                list(filter(lambda x: x.key != CLUSTERMAN_TERMINATION_TAINT_KEY, node.spec.taints))
                if node.spec.taints
                else []
            )
            taints.append({"effect": "NoSchedule", "key": CLUSTERMAN_TERMINATION_TAINT_KEY, "value": now})
            self._core_api.patch_node(name=node_name, body={"spec": {"taints": taints}})
            return True
        except ApiException as e:
            logger.warning(f"Failed to cordon {node_name}: {e.status} - {e.reason}")
            return False

    def uncordon_node(self, node_name: str) -> bool:
        try:
            node = self._core_api.read_node(node_name)
            if not node:
                logger.warning(f"Node doesn't exist: {node_name}")
                return False
            taints = (
                list(filter(lambda x: x.key != CLUSTERMAN_TERMINATION_TAINT_KEY, node.spec.taints))
                if node.spec.taints
                else []
            )
            self._core_api.patch_node(name=node_name, body={"spec": {"taints": taints}})
            return True
        except ApiException as e:
            logger.warning(f"Failed to uncordon {node_name}: {e.status} - {e.reason}")
            return False

    def list_node_migration_resources(
        self, statuses: List[MigrationStatus], max_attempts: Optional[int] = None
    ) -> Set[MigrationEvent]:
        """Fetch node migration event resource from k8s CRD

        :param List[MigrationStatus] statuses: event status to look for
        :param Optional[int] max_attempts: max number of attempts done on event
        :return: collection of migration events
        """
        assert self._migration_crd_api, "CRD client was not initialized"
        try:
            label_filter = ",".join(status.value for status in statuses)
            label_selector = f"{MIGRATION_CRD_STATUS_LABEL} in ({label_filter})"
            if self.pool:
                label_selector += f",{self.pool_label_key}={self.pool}"
            if max_attempts is not None:
                attemps_filter = ",".join(map(str, range(max_attempts)))
                label_selector += f",{MIGRATION_CRD_ATTEMPTS_LABEL} in ({attemps_filter})"
            resources = self._migration_crd_api.list_cluster_custom_object(label_selector=label_selector)
            return set(map(MigrationEvent.from_crd, resources.get("items", [])))
        except Exception as e:
            logger.error(f"Failed fetching migration events: {e}")
        return set()

    def mark_node_migration_resource(
        self, event_name: str, status: MigrationStatus, attempts: Optional[int] = None
    ) -> None:
        """Set status for migration event resource

        :param str event_name: name of the resource CRD
        :param MigrationStatus status: status to be set
        :param Optional[int] attempts: number of failed attempts to complete migration
        """
        assert self._migration_crd_api, "CRD client was not initialized"
        labels = {MIGRATION_CRD_STATUS_LABEL: status.value}
        if attempts is not None:
            labels[MIGRATION_CRD_ATTEMPTS_LABEL] = str(attempts)
        try:
            self._migration_crd_api.patch_cluster_custom_object(
                name=event_name,
                body={"metadata": {"labels": labels}},
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
            body = event.to_crd_body(
                labels={
                    MIGRATION_CRD_STATUS_LABEL: status.value,
                    MIGRATION_CRD_ATTEMPTS_LABEL: "0",
                    self.pool_label_key: event.pool,
                },
            )
            body["apiVersion"] = f"{MIGRATION_CRD_GROUP}/{MIGRATION_CRD_VERSION}"
            body["kind"] = MIGRATION_CRD_KIND
            self._migration_crd_api.create_cluster_custom_object(body=body)
        except Exception as e:
            logger.error(f"Failed creating migration event resource: {e}")

    def has_enough_capacity_for_pods(self) -> bool:
        """Checks whether there are unschedulable pods due to insufficient resources

        :return: True if no unschedulable pods are due to resource constraints
        """
        return not any(self.get_unschedulable_pods())

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

    def _get_agent_metadata(self, node_ip: str) -> AgentMetadata:
        node = self._nodes_by_ip.get(node_ip)
        if not node:
            return AgentMetadata(state=AgentState.ORPHANED)
        return AgentMetadata(
            agent_id=node.metadata.name,
            allocated_resources=allocated_node_resources(self._pods_by_ip[node_ip]),
            is_safe_to_kill=self._is_node_safe_to_kill(node_ip),
            is_draining=self._is_node_draining(node),
            batch_task_count=self._count_batch_tasks(node_ip),
            priority=self.get_node_priority(node_ip),
            state=(AgentState.RUNNING if self._pods_by_ip[node_ip] else AgentState.IDLE),
            task_count=len(self._pods_by_ip[node_ip]),
            total_resources=total_node_resources(node, self._excluded_pods),
            kernel=get_node_kernel_version(node),
            lsbrelease=get_node_lsbrelease(node),
        )

    def _is_node_draining(self, node: KubernetesNode) -> bool:
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
        kwargs = {"label_selector": ",".join(self._label_selectors)} if self._label_selectors else {}
        pool_nodes = self._core_api.list_node(**kwargs).items
        return {get_node_ip(node): node for node in pool_nodes}

    def _get_pods_info_with_label(
        self,
    ) -> Tuple[Mapping[str, List[KubernetesPod]], List[KubernetesPod], ClustermanResources, ClustermanResources,]:
        pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)
        unschedulable_pods: List[KubernetesPod] = []

        unschedulable_pods_resources: ClustermanResources = ClustermanResources()
        allocated_pods_resources: ClustermanResources = ClustermanResources()

        exclude_daemonset_pods = self.pool_config.read_bool(
            "exclude_daemonset_pods",
            default=staticconf.read_bool("exclude_daemonset_pods", default=False),
        )
        label_selector = f"{self.pool_label_key}={self.pool}"

        for pod in self._core_api.list_pod_for_all_namespaces(label_selector=label_selector).items:
            if exclude_daemonset_pods and self._pod_belongs_to_daemonset(pod):
                # In the current situation, this will never be reached. Because daemonsets don't have pool label
                continue
            elif pod.status.phase == "Running" or self._is_recently_scheduled(pod):
                pods_by_ip[pod.status.host_ip].append(pod)
                allocated_pods_resources += total_pod_resources(pod)
            elif self._is_unschedulable(pod):
                unschedulable_pods.append(pod)
                unschedulable_pods_resources += total_pod_resources(pod)
        return (
            pods_by_ip,
            unschedulable_pods,
            unschedulable_pods_resources,
            allocated_pods_resources,
        )

    def _get_excluded_pods(self) -> List[KubernetesPod]:
        excluded_pods: List[KubernetesPod] = []

        exclude_daemonset_pods = self.pool_config.read_bool(
            "exclude_daemonset_pods",
            default=staticconf.read_bool("exclude_daemonset_pods", default=False),
        )
        if len(self._nodes_by_ip) == 0 or not exclude_daemonset_pods:
            return excluded_pods

        # deamonsets pods are same on any node in the pool, therefore checking one node will be enough
        arbitrary_node = self._nodes_by_ip[next(iter(self._nodes_by_ip))]
        arbitrary_node_pods = self._list_all_pods_on_node(arbitrary_node.metadata.name)
        for pod in arbitrary_node_pods:
            if self._pod_belongs_to_daemonset(pod):
                excluded_pods.append(pod)

        return excluded_pods

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

    def get_node_priority(self, node_id: str) -> float:
        pods = self._pods_by_ip[node_id]
        if not pods:
            return 0.0
        max_requested_resource = max([self.get_pod_resources_score(pod) for pod in pods])
        # nodes have bigger pods = higher priority = lower possibility for choosing termination
        return max_requested_resource

    def get_pod_resources_score(self, pod: KubernetesPod) -> float:
        resource = total_pod_resources(pod)
        cpus = getattr(resource, "cpus") * 2
        mem = getattr(resource, "mem") / 1000
        return cpus + mem

    @property
    def pool_label_key(self):
        return self.pool_config.read_string("pool_label_key", default="clusterman.com/pool")

    @property
    def safe_to_evict_key(self):
        return self.pool_config.read_string("safe_to_evict_key", default="clusterman.com/safe_to_evict")
