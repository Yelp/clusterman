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
from copy import deepcopy

import mock
import pytest
from kubernetes.client import V1NodeSpec
from kubernetes.client import V1NodeStatus
from kubernetes.client import V1ObjectMeta
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm

from clusterman.interfaces.types import AgentState
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector


@pytest.fixture
def mock_cluster_connector(pod1, pod2, pod3, pod5):
    with mock.patch('clusterman.kubernetes.kubernetes_cluster_connector.kubernetes'), \
            mock.patch('clusterman.kubernetes.kubernetes_cluster_connector.staticconf'):
        mock_cluster_connector = KubernetesClusterConnector('kubernetes-test', 'bar')
        mock_cluster_connector._nodes_by_ip = {
            '10.10.10.1': KubernetesNode(
                metadata=V1ObjectMeta(name='node1', labels={'clusterman.com/node': 'foo'}),
                spec=V1NodeSpec(unschedulable='false'),
                status=V1NodeStatus(
                    allocatable={'cpu': '4', 'gpu': '2', 'pods': '1234'},
                    capacity={'cpu': '4', 'gpu': '2', 'pods': '1234'}
                )
            ),
            '10.10.10.2': KubernetesNode(
                metadata=V1ObjectMeta(name='node2', labels={'clusterman.com/node': 'foo'}),
                spec=V1NodeSpec(unschedulable='false'),
                status=V1NodeStatus(
                    allocatable={'cpu': '6.5', 'pods': '1234'},
                    capacity={'cpu': '8', 'pods': '1234'}
                )
            )
        }
        pod4 = deepcopy(pod1)
        mock_cluster_connector._pods_by_ip = {
            '10.10.10.1': [],
            '10.10.10.2': [pod1, pod2],
            '10.10.10.3': [pod4, pod5],
        }
        mock_cluster_connector._pods = [pod1, pod2, pod3, pod4, pod5]
        return mock_cluster_connector


@pytest.mark.parametrize('ip_address,expected_state', [
    (None, AgentState.UNKNOWN),
    ('1.2.3.4', AgentState.ORPHANED),
    ('10.10.10.1', AgentState.IDLE),
    ('10.10.10.2', AgentState.RUNNING),
])
def test_get_agent_metadata(mock_cluster_connector, ip_address, expected_state):
    agent_metadata = mock_cluster_connector.get_agent_metadata(ip_address)
    assert agent_metadata.is_safe_to_kill == (ip_address != '10.10.10.2')
    assert agent_metadata.state == expected_state


def test_allocation(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_allocation('cpus') == 3.0


def test_total_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_total('cpus') == 10.5


def test_get_pending_pods(mock_cluster_connector):
    assert len(mock_cluster_connector._get_pending_pods()) == 2


def test_get_unschedulable_pods(mock_cluster_connector):
    assert len(mock_cluster_connector.get_unschedulable_pods()) == 1


def test_pending_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_pending('cpus') == 1.5


def test_evict_pods_on_node(mock_cluster_connector):
    with mock.patch(
            'clusterman.kubernetes.kubernetes_cluster_connector.KubernetesClusterConnector.evict_pods_on_node',
            autospec=True,
    ) as mock_evict_pods_on_node:
        mock_node = mock.Mock()
        mock_evict_pods_on_node.return_value = None
        mock_cluster_connector.evict_pods_on_node(mock_node)
        assert mock_evict_pods_on_node.called


def test_set_node_unscheduable(mock_cluster_connector):
    with mock.patch(
            'clusterman.kubernetes.kubernetes_cluster_connector.KubernetesClusterConnector.set_node_unschedulable',
            autospec=True,
    ) as mock_set_node_unschedulable:
        mock_node = mock.Mock()
        mock_set_node_unschedulable.return_value = None
        mock_cluster_connector.set_node_unschedulable(mock_node)
        assert mock_set_node_unschedulable.called


def test_delete_node(mock_cluster_connector):
    with mock.patch(
            'clusterman.kubernetes.kubernetes_cluster_connector.KubernetesClusterConnector.delete_node',
            autospec=True,
    ) as mock_delete_node:
        mock_node = mock.Mock()
        mock_delete_node.return_value = None
        mock_cluster_connector.delete_node(mock_node)
        assert mock_delete_node.called


def test_pod_matches_node_selector_or_affinity(mock_cluster_connector, pod2, pod5, pod6, pod7):
    pod8 = deepcopy(pod6)
    pod8.spec.node_selector = {'clusterman.com/pool': 'not-bar'}
    pod9 = deepcopy(pod6)
    pod9.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution \
        .node_selector_terms[0].match_expressions[0] \
        .values = ['not-bar']
    assert not mock_cluster_connector._pod_matches_node_selector_or_affinity(pod2)
    assert mock_cluster_connector._pod_matches_node_selector_or_affinity(pod5)
    assert mock_cluster_connector._pod_matches_node_selector_or_affinity(pod6)
    assert mock_cluster_connector._pod_matches_node_selector_or_affinity(pod7)
    assert not mock_cluster_connector._pod_matches_node_selector_or_affinity(pod8)
    assert not mock_cluster_connector._pod_matches_node_selector_or_affinity(pod9)


def test_selector_term_matches_requirement(mock_cluster_connector):
    selector_term = V1NodeSelectorTerm(
        match_expressions=[
            V1NodeSelectorRequirement(
                key='clusterman.com/scheduler',
                operator='Exists'
            ),
            V1NodeSelectorRequirement(
                key='clusterman.com/pool',
                operator='In',
                values=['bar']
            )
        ]
    )
    selector_requirement = V1NodeSelectorRequirement(
        key='clusterman.com/pool',
        operator='In',
        values=['bar']
    )
    assert mock_cluster_connector._selector_term_matches_requirement(selector_term, selector_requirement)
