import mock
import pytest

from clusterman.exceptions import PoolManagerError
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.mesos.mesos_cluster_connector import MesosClusterConnector


@pytest.fixture
def mock_cluster_connector():
    mock_cluster_connector = MesosClusterConnector('mesos-test', 'bar')
    mock_cluster_connector._agents = {
        '10.10.10.1': {
            'id': 'idle',
            'resources': {'cpus': 4, 'gpus': 2},
        },
        '10.10.10.2': {
            'id': 'no-gpus',
            'resources': {'cpus': 8},
            'used_resources': {'cpus': 1.5},
        },
    }
    mock_cluster_connector._batch_task_count_per_agent = {'idle': 0, 'no-gpus': 0}
    mock_cluster_connector._task_count_per_agent = {'idle': 0, 'no-gpus': 0}
    return mock_cluster_connector


def test_init(mock_cluster_connector):
    assert mock_cluster_connector.api_endpoint == 'http://the.mesos.leader:5050/'


@pytest.mark.parametrize('ip_address,expected_state', [
    (None, AgentState.UNKNOWN),
    ('1.2.3.4', AgentState.ORPHANED),
    ('10.10.10.1', AgentState.IDLE),
    ('10.10.10.2', AgentState.RUNNING),
])
def test_get_orphaned_agent(mock_cluster_connector, ip_address, expected_state):
    agent_metadata = mock_cluster_connector.get_agent_metadata(ip_address)
    assert agent_metadata.state == expected_state


def test_count_tasks_by_agent(mock_cluster_connector):
    mock_cluster_connector._tasks = [
        {'slave_id': 1, 'state': 'TASK_RUNNING'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'},
        {'slave_id': 3, 'state': 'TASK_FINISHED'},
        {'slave_id': 1, 'state': 'TASK_FAILED'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'}
    ]
    assert mock_cluster_connector._count_tasks_per_agent() == {1: 1, 2: 2}


def test_count_batch_tasks_by_agent(mock_cluster_connector):
    mock_cluster_connector._tasks = [
        {'slave_id': '1', 'state': 'TASK_RUNNING', 'framework_id': '2'},
        {'slave_id': '2', 'state': 'TASK_RUNNING', 'framework_id': '2'},
        {'slave_id': '3', 'state': 'TASK_FINISHED', 'framework_id': '2'},
        {'slave_id': '1', 'state': 'TASK_FAILED', 'framework_id': '2'},
        {'slave_id': '2', 'state': 'TASK_RUNNING', 'framework_id': '1'}
    ]
    mock_cluster_connector._frameworks = {
        '1': {'name': 'chronos'},
        '2': {'name': 'marathon123'},
    }

    ret = mock_cluster_connector._count_batch_tasks_per_agent()
    assert ret == {'2': 1}
    assert ret['1'] == 0


def test_is_batch_task(mock_cluster_connector):
    mock_cluster_connector.non_batch_framework_prefixes = ('marathon', 'paasta')
    assert mock_cluster_connector._is_batch_framework('chronos4')
    assert not mock_cluster_connector._is_batch_framework('marathon123')


@mock.patch('clusterman.mesos.mesos_cluster_connector.mesos_post')
class TestAgentListing:
    def test_agent_list_error(self, mock_post, mock_cluster_connector):
        mock_post.side_effect = PoolManagerError('dummy error')
        with pytest.raises(PoolManagerError):
            mock_cluster_connector._get_agents()

    def test_filter_pools(self, mock_post, mock_agents_response, mock_cluster_connector):
        mock_post.return_value = mock_agents_response
        agents = mock_cluster_connector._get_agents()
        assert len(agents) == 1
        assert agents['10.10.10.12']['hostname'] == 'im-in-the-pool.yelpcorp.com'

        # Multiple calls should have the same result.
        assert agents == mock_cluster_connector._get_agents()


@pytest.mark.parametrize('resource_name,expected', [('cpus', 1.5)])
def test_allocation(mock_cluster_connector, resource_name, expected):
    assert mock_cluster_connector.get_resource_allocation(resource_name) == expected


@pytest.mark.parametrize('resource_name,expected', [('cpus', 12)])
def test_total_cpus(mock_cluster_connector, resource_name, expected):
    assert mock_cluster_connector.get_resource_total(resource_name) == expected


@pytest.mark.parametrize('resource_name,expected', [('mem', 0), ('cpus', 0.125)])
def test_average_allocation(mock_cluster_connector, resource_name, expected):
    assert mock_cluster_connector.get_percent_resource_allocation(resource_name) == expected
