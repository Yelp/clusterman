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
from unittest import mock

import arrow
import pytest
from kubernetes.client import V1Container
from kubernetes.client import V1NodeStatus
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1OwnerReference
from kubernetes.client import V1Pod
from kubernetes.client import V1PodCondition
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements
from kubernetes.client.models.v1_affinity import V1Affinity
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_address import V1NodeAddress
from kubernetes.client.models.v1_node_affinity import V1NodeAffinity
from kubernetes.client.models.v1_node_selector import V1NodeSelector
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_preferred_scheduling_term import V1PreferredSchedulingTerm
from staticconf.testing import PatchConfiguration

from clusterman.config import POOL_NAMESPACE
from clusterman.interfaces.types import AgentState
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.migration.event import MigrationCondition
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import ConditionOperator
from clusterman.migration.event_enums import ConditionTrait
from clusterman.migration.event_enums import MigrationStatus


@pytest.fixture
def running_pod_1():
    return V1Pod(
        metadata=V1ObjectMeta(name="running_pod_1"),
        status=V1PodStatus(phase="Running", host_ip="10.10.10.2"),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            node_selector={"clusterman.com/pool": "bar"},
        ),
    )


@pytest.fixture
def running_pod_2():
    return V1Pod(
        metadata=V1ObjectMeta(name="running_pod_2", owner_references=[]),
        status=V1PodStatus(phase="Running", host_ip="10.10.10.3"),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            node_selector={"clusterman.com/pool": "bar"},
        ),
    )


@pytest.fixture
def running_pod_on_nonexistent_node():
    return V1Pod(
        metadata=V1ObjectMeta(name="running_pod_on_nonexistent_node", owner_references=[]),
        status=V1PodStatus(phase="Running", host_ip="10.10.10.4"),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            node_selector={"clusterman.com/pool": "bar"},
        ),
    )


@pytest.fixture
def unevictable_pod():
    return V1Pod(
        metadata=V1ObjectMeta(
            name="unevictable_pod",
            annotations={"clusterman.com/safe_to_evict": "false"},
            owner_references=[],
        ),
        status=V1PodStatus(phase="Running", host_ip="10.10.10.2"),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ]
        ),
    )


@pytest.fixture
def unschedulable_pod():
    return V1Pod(
        metadata=V1ObjectMeta(name="unschedulable_pod", annotations=dict(), owner_references=[]),
        status=V1PodStatus(
            phase="Pending",
            conditions=[V1PodCondition(status="False", type="PodScheduled", reason="Unschedulable")],
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container2",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            node_selector={"clusterman.com/pool": "bar"},
        ),
    )


@pytest.fixture
def pending_pod():
    return V1Pod(
        metadata=V1ObjectMeta(name="pending_pod", annotations=dict(), owner_references=[]),
        status=V1PodStatus(
            phase="Pending",
            conditions=None,
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container2",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            node_selector={"clusterman.com/pool": "bar"},
        ),
    )


@pytest.fixture
def daemonset_pod_1():
    return V1Pod(
        metadata=V1ObjectMeta(
            name="daemonset_pod",
            annotations=dict(),
            owner_references=[V1OwnerReference(kind="DaemonSet", api_version="foo", name="daemonset", uid="bar")],
        ),
        status=V1PodStatus(
            phase="Running",
            host_ip="10.10.10.1",
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
        ),
    )


@pytest.fixture
def daemonset_pod_2():
    return V1Pod(
        metadata=V1ObjectMeta(
            name="daemonset_pod",
            annotations=dict(),
            owner_references=[V1OwnerReference(kind="DaemonSet", api_version="foo", name="daemonset", uid="bar")],
        ),
        status=V1PodStatus(
            phase="Running",
            host_ip="10.10.10.2",
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
        ),
    )


@pytest.fixture
def daemonset_pod_3():
    return V1Pod(
        metadata=V1ObjectMeta(
            name="daemonset_pod",
            annotations=dict(),
            owner_references=[V1OwnerReference(kind="DaemonSet", api_version="foo", name="daemonset", uid="bar")],
        ),
        status=V1PodStatus(
            phase="Running",
            host_ip="10.10.10.3",
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container1",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
        ),
    )


@pytest.fixture
def pod_with_required_affinity():
    return V1Pod(
        status=V1PodStatus(
            phase="Pending",
            conditions=[V1PodCondition(status="False", type="PodScheduled", reason="Unschedulable")],
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            affinity=V1Affinity(
                node_affinity=V1NodeAffinity(
                    required_during_scheduling_ignored_during_execution=V1NodeSelector(
                        node_selector_terms=[
                            V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(
                                        key="clusterman.com/pool",
                                        operator="In",
                                        values=["bar"],
                                    )
                                ]
                            )
                        ]
                    )
                )
            ),
        ),
    )


@pytest.fixture
def pod_with_preferred_affinity():
    return V1Pod(
        status=V1PodStatus(
            phase="Pending",
            conditions=[V1PodCondition(status="False", type="PodScheduled", reason="Unschedulable")],
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="container",
                    resources=V1ResourceRequirements(requests={"cpu": "1.5"}),
                )
            ],
            affinity=V1Affinity(
                node_affinity=V1NodeAffinity(
                    required_during_scheduling_ignored_during_execution=V1NodeSelector(
                        node_selector_terms=[
                            V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(
                                        key="clusterman.com/scheduler",
                                        operator="Exists",
                                    )
                                ]
                            )
                        ]
                    ),
                    preferred_during_scheduling_ignored_during_execution=[
                        V1PreferredSchedulingTerm(
                            weight=10,
                            preference=V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(
                                        key="clusterman.com/pool",
                                        operator="In",
                                        values=["bar"],
                                    )
                                ]
                            ),
                        )
                    ],
                )
            ),
        ),
    )


@pytest.fixture(scope="function")
def mock_cluster_connector(
    running_pod_1,
    running_pod_2,
    running_pod_on_nonexistent_node,
    unevictable_pod,
    unschedulable_pod,
    pending_pod,
    daemonset_pod_1,
    daemonset_pod_2,
    daemonset_pod_3,
):
    with mock.patch("clusterman.kubernetes.kubernetes_cluster_connector.kubernetes",), mock.patch(
        "clusterman.kubernetes.kubernetes_cluster_connector.CachedCoreV1Api",
    ) as mock_core_api, PatchConfiguration(
        {"clusters": {"kubernetes-test": {"kubeconfig_path": "/var/lib/clusterman.conf"}}},
    ):
        mock_core_api.return_value.list_node.return_value.items = [
            KubernetesNode(
                metadata=V1ObjectMeta(name="node1", labels={"clusterman.com/pool": "bar"}),
                status=V1NodeStatus(
                    allocatable={"cpu": "4", "gpu": 2},
                    capacity={"cpu": "4", "gpu": "2"},
                    addresses=[V1NodeAddress(type="InternalIP", address="10.10.10.1")],
                ),
            ),
            KubernetesNode(
                metadata=V1ObjectMeta(name="node2", labels={"clusterman.com/pool": "bar"}),
                status=V1NodeStatus(
                    allocatable={"cpu": "6.5"},
                    capacity={"cpu": "8"},
                    addresses=[V1NodeAddress(type="InternalIP", address="10.10.10.2")],
                ),
            ),
            KubernetesNode(
                metadata=V1ObjectMeta(name="node2", labels={"clusterman.com/pool": "bar"}),
                status=V1NodeStatus(
                    allocatable={"cpu": "1"},
                    capacity={"cpu": "8"},
                    addresses=[V1NodeAddress(type="InternalIP", address="10.10.10.3")],
                ),
            ),
        ]
        mock_core_api.return_value.list_pod_for_all_namespaces.return_value.items = [
            running_pod_1,
            running_pod_2,
            running_pod_on_nonexistent_node,
            unevictable_pod,
            unschedulable_pod,
            pending_pod,
            daemonset_pod_1,
            daemonset_pod_2,
            daemonset_pod_3,
        ]
        mock_cluster_connector = KubernetesClusterConnector("kubernetes-test", "bar")
        mock_cluster_connector.reload_state()
        yield mock_cluster_connector


@pytest.fixture
def mock_cluster_connector_crd(mock_cluster_connector):
    mock_cluster_connector._init_crd_client = True
    with mock.patch.object(mock_cluster_connector, "_migration_crd_api"):
        yield mock_cluster_connector


@pytest.mark.parametrize(
    "ip_address,expected_state",
    [
        (None, AgentState.UNKNOWN),
        ("1.2.3.4", AgentState.ORPHANED),
        ("10.10.10.1", AgentState.IDLE),
        ("10.10.10.2", AgentState.RUNNING),
    ],
)
def test_get_agent_metadata(mock_cluster_connector, ip_address, expected_state):
    with PatchConfiguration(
        {"exclude_daemonset_pods": True},
        namespace=POOL_NAMESPACE.format(pool=mock_cluster_connector.pool, scheduler=mock_cluster_connector.SCHEDULER),
    ):
        mock_cluster_connector.reload_state()
        agent_metadata = mock_cluster_connector.get_agent_metadata(ip_address)
        assert agent_metadata.is_safe_to_kill == (ip_address != "10.10.10.2")
        assert agent_metadata.state == expected_state


def test_get_nodes_by_ip(mock_cluster_connector):
    mock_cluster_connector._core_api.list_node.reset_mock()
    mock_cluster_connector.set_label_selectors(["foobar.clusterman.com/something=stuff"], add_to_existing=True)
    mock_cluster_connector._get_nodes_by_ip()
    mock_cluster_connector._core_api.list_node.assert_called_once_with(
        label_selector="clusterman.com/pool=bar,foobar.clusterman.com/something=stuff",
    )


def test_reload_state_no_pods(mock_cluster_connector):
    mock_cluster_connector.reload_state(load_pods_info=False)
    assert mock_cluster_connector._pods_by_ip == {"10.10.10.1": [], "10.10.10.2": [], "10.10.10.3": []}


def test_allocation(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_allocation("cpus") == 10.5


def test_allocation_with_excluded_pods(mock_cluster_connector, daemonset_pod_1, daemonset_pod_2, daemonset_pod_3):
    with PatchConfiguration(
        {"exclude_daemonset_pods": True},
        namespace=POOL_NAMESPACE.format(pool=mock_cluster_connector.pool, scheduler=mock_cluster_connector.SCHEDULER),
    ), mock.patch(
        "clusterman.kubernetes.kubernetes_cluster_connector.KubernetesClusterConnector._list_all_pods_on_node",
        autospec=True,
    ) as mock_list_all_pods_on_node:
        mock_list_all_pods_on_node.return_value = [daemonset_pod_1]
        mock_cluster_connector.reload_state()
        assert daemonset_pod_1 not in mock_cluster_connector._pods_by_ip["10.10.10.1"]
        assert daemonset_pod_2 not in mock_cluster_connector._pods_by_ip["10.10.10.2"]
        assert daemonset_pod_3 not in mock_cluster_connector._pods_by_ip["10.10.10.3"]
        assert mock_cluster_connector.get_resource_total("cpus") == 7
        assert mock_cluster_connector.get_resource_allocation("cpus") == 6


def test_total_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_total("cpus") == 11.5


def test_get_unschedulable_pods(mock_cluster_connector):
    assert len(mock_cluster_connector.get_unschedulable_pods()) == 1


def test_pending_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_pending("cpus") == 1.5


def test_pod_belongs_to_daemonset(mock_cluster_connector, running_pod_1, daemonset_pod_1):
    assert not mock_cluster_connector._pod_belongs_to_daemonset(running_pod_1)
    assert mock_cluster_connector._pod_belongs_to_daemonset(daemonset_pod_1)


def test_list_node_migration_resources(mock_cluster_connector_crd):
    mock_timestamp = "2023-02-10T11:18:17Z"
    mock_cluster_connector_crd._migration_crd_api.list_cluster_custom_object.return_value = {
        "items": [
            {
                "metadata": {
                    "creationTimestamp": mock_timestamp,
                    "name": f"mesos-test-bar-220912-{i}",
                    "labels": {
                        "clusterman.yelp.com/migration_status": "pending",
                        "clusterman.yelp.com/attempts": "1",
                    },
                },
                "spec": {
                    "cluster": "mesos-test",
                    "pool": "bar",
                    "condition": {
                        "trait": "uptime",
                        "operator": "lt",
                        "target": f"9{i}d",
                    },
                },
            }
            for i in range(3)
        ]
    }
    assert mock_cluster_connector_crd.list_node_migration_resources(
        [MigrationStatus.PENDING, MigrationStatus.INPROGRESS],
        3,
    ) == {
        MigrationEvent(
            resource_name=f"mesos-test-bar-220912-{i}",
            cluster="mesos-test",
            pool="bar",
            label_selectors=[],
            condition=MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, (90 + i) * 24 * 60 * 60),
            previous_attempts=1,
            created=arrow.get(mock_timestamp),
        )
        for i in range(3)
    }
    mock_cluster_connector_crd._migration_crd_api.list_cluster_custom_object.assert_called_once_with(
        label_selector=(
            "clusterman.yelp.com/migration_status in (pending,inprogress)"
            ",clusterman.com/pool=bar"
            ",clusterman.yelp.com/attempts in (0,1,2)"
        )
    )


def test_list_node_migration_resources_no_init(mock_cluster_connector):
    with pytest.raises(AssertionError):
        mock_cluster_connector.list_node_migration_resources([MigrationStatus.PENDING])


def test_mark_node_migration_resource(mock_cluster_connector_crd):
    mock_cluster_connector_crd.mark_node_migration_resource("mesos-test-bar-220912-0", MigrationStatus.COMPLETED)
    mock_cluster_connector_crd._migration_crd_api.patch_cluster_custom_object.assert_called_once_with(
        name="mesos-test-bar-220912-0",
        body={"metadata": {"labels": {"clusterman.yelp.com/migration_status": "completed"}}},
    )


def test_create_node_migration_resource(mock_cluster_connector_crd):
    mock_cluster_connector_crd.create_node_migration_resource(
        MigrationEvent(
            resource_name="mesos-test-bar-111222333",
            cluster="mesos-test",
            pool="bar",
            label_selectors=[],
            condition=MigrationCondition(ConditionTrait.LSBRELEASE, ConditionOperator.GE, "22.04"),
        ),
        MigrationStatus.PENDING,
    )
    mock_cluster_connector_crd._migration_crd_api.create_cluster_custom_object.assert_called_once_with(
        body={
            "apiVersion": "clusterman.yelp.com/v1",
            "kind": "NodeMigration",
            "metadata": {
                "name": "mesos-test-bar-111222333",
                "labels": {
                    "clusterman.yelp.com/migration_status": "pending",
                    "clusterman.yelp.com/attempts": "0",
                    "clusterman.com/pool": "bar",
                },
            },
            "spec": {
                "cluster": "mesos-test",
                "pool": "bar",
                "label_selectors": [],
                "condition": {"trait": "lsbrelease", "operator": "ge", "target": "22.04"},
            },
        },
    )


@pytest.mark.parametrize(
    "unschedulable,expected",
    (
        (
            [mock.MagicMock()],
            False,
        ),
        (
            [],
            True,
        ),
    ),
)
def test_has_enough_capacity_for_pods(mock_cluster_connector, unschedulable, expected):
    with mock.patch.object(mock_cluster_connector, "get_unschedulable_pods") as mock_get_unsched:
        mock_get_unsched.return_value = unschedulable
        assert mock_cluster_connector.has_enough_capacity_for_pods() is expected
