from datetime import datetime

import mock
import pytest
import staticconf

from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.util import _get_agent_by_ip
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.mesos.util import get_cluster_name_list
from clusterman.mesos.util import get_mesos_agent_and_state_from_aws_instance
from clusterman.mesos.util import get_pool_name_list
from clusterman.mesos.util import get_task_count_per_agent
from clusterman.mesos.util import mesos_post
from clusterman.mesos.util import MesosAgentState


@pytest.fixture
def mock_market_capacities():
    return {'market-1': 1000, 'market-2': 5}


@pytest.fixture
def mock_agent_pid_to_ip():
    with mock.patch('clusterman.mesos.util.agent_pid_to_ip') as mock_agent_pid_to_ip:
        mock_agent_pid_to_ip.return_value = '1.2.3.4'
        yield


def test_agent_pid_to_ip():
    ret = agent_pid_to_ip('slave(1)@10.40.31.172:5051')
    assert ret == '10.40.31.172'


def test_get_agent_by_ip(mock_agent_pid_to_ip):
    mock_agent = {'pid': 'slave(1)@1.2.3.4:5051'}
    mock_agents = []
    assert _get_agent_by_ip('1.1.1.1', mock_agents) is None
    mock_agents = [mock_agent]
    assert _get_agent_by_ip('1.1.1.1', mock_agents) is None
    assert _get_agent_by_ip('1.2.3.4', mock_agents) is mock_agent


@mock.patch('clusterman.mesos.util.allocated_cpu_resources')
class TestGetMesosAgentAndStateFromAwsInstance:
    def test_orphaned(self, mock_allocated):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = []
        mock_allocated.return_value = 100

        agent, state = get_mesos_agent_and_state_from_aws_instance(instance, agents)
        assert agent is None
        assert state == MesosAgentState.ORPHANED

    def test_idle(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = [{'hostname': 'foo.com', 'pid': 'slave(1)@1.2.3.4:5051'}]
        mock_allocated.return_value = 0

        agent, state = get_mesos_agent_and_state_from_aws_instance(instance, agents)
        assert agent == agents[0]
        assert state == MesosAgentState.IDLE

    def test_running(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = [{'hostname': 'foo.com', 'pid': 'slave(1)@1.2.3.4:5051'}]
        mock_allocated.return_value = 100

        agent, state = get_mesos_agent_and_state_from_aws_instance(instance, agents)
        assert agent == agents[0]
        assert state == MesosAgentState.RUNNING

    def test_unknown(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {}
        agents = [{'hostname': 'foo.com', 'pid': 'slave(1)@1.2.3.4:5051'}]
        mock_allocated.return_value = 100

        agent, state = get_mesos_agent_and_state_from_aws_instance(instance, agents)
        assert agent is None
        assert state == MesosAgentState.UNKNOWN


def test_task_count_per_agent():
    tasks = [
        {'slave_id': 1, 'state': 'TASK_RUNNING'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'},
        {'slave_id': 3, 'state': 'TASK_FINISHED'},
        {'slave_id': 1, 'state': 'TASK_FAILED'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'}
    ]
    task_count_per_agent = get_task_count_per_agent(tasks)
    assert task_count_per_agent == {1: 1, 2: 2}


def test_allocated_cpu_resources(mock_agents_response):
    assert allocated_cpu_resources(mock_agents_response.json()['slaves'][0]) == 0
    assert allocated_cpu_resources(mock_agents_response.json()['slaves'][1]) == 0
    assert allocated_cpu_resources(mock_agents_response.json()['slaves'][2]) == 10


@mock.patch('clusterman.mesos.util.mesos_post', wraps=mesos_post)
class TestMesosPost:
    def test_success(self, wrapped_post):
        with mock.patch('clusterman.mesos.util.requests'):
            wrapped_post('http://the.mesos.master/', 'an-endpoint')
        assert wrapped_post.call_count == 2
        assert wrapped_post.call_args_list == [
            mock.call('http://the.mesos.master/', 'an-endpoint'),
            mock.call('http://the.mesos.master/', 'redirect'),
        ]

    def test_failure(self, wrapped_post):
        with mock.patch('clusterman.mesos.util.requests') as mock_requests, \
                pytest.raises(MesosPoolManagerError):
            mock_requests.post.side_effect = Exception('something bad happened')
            wrapped_post('http://the.mesos.master/', 'an-endpoint')


def test_get_cluster_name_list():
    with staticconf.testing.MockConfiguration(
        {
            'mesos_clusters': {
                'cluster-A': {
                    'fqdn': 'service.leader',
                },
                'cluster-B': {
                    'fqdn': 'service.leader',
                },
            },
        },
        namespace=staticconf.config.DEFAULT,
    ):
        assert set(get_cluster_name_list()) == {'cluster-A', 'cluster-B'}


@mock.patch('clusterman.mesos.util.get_cluster_config_directory')
@mock.patch('os.listdir')
def test_get_pool_name_list(mock_listdir, mock_get_cluster_config_directory):
    mock_get_cluster_config_directory.return_value = '/tmp/somedir/cluster-A'
    mock_listdir.return_value = ['pool-A.yaml', 'pool-B.xml', 'pool-C.yaml', 'pool-D']
    assert set(get_pool_name_list('cluster-A')) == {'pool-A', 'pool-C'}
    assert mock_get_cluster_config_directory.call_args == mock.call('cluster-A')
    assert mock_listdir.call_args == mock.call('/tmp/somedir/cluster-A')
