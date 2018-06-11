from datetime import datetime

import mock
import pytest
import staticconf

from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.mesos.util import find_largest_capacity_market
from clusterman.mesos.util import get_agent_by_ip
from clusterman.mesos.util import get_cluster_name_list
from clusterman.mesos.util import get_mesos_state
from clusterman.mesos.util import get_pool_name_list
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
    assert get_agent_by_ip('1.1.1.1', mock_agents) is None
    mock_agents = [mock_agent]
    assert get_agent_by_ip('1.1.1.1', mock_agents) is None
    assert get_agent_by_ip('1.2.3.4', mock_agents) is mock_agent


@mock.patch('clusterman.mesos.util.allocated_cpu_resources')
class TestGetMesosState:
    def test_orphaned(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = []
        mock_allocated.return_value = 100
        assert get_mesos_state(instance, agents) == MesosAgentState.ORPHANED

    def test_idle(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = [{'hostname': 'foo.com', 'pid': 'slave(1)@1.2.3.4:5051'}]
        mock_allocated.return_value = 0
        assert get_mesos_state(instance, agents) == MesosAgentState.IDLE

    def test_running(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = [{'hostname': 'foo.com', 'pid': 'slave(1)@1.2.3.4:5051'}]
        mock_allocated.return_value = 100
        assert get_mesos_state(instance, agents) == MesosAgentState.RUNNING

    def test_unknown(self, mock_allocated, mock_agent_pid_to_ip):
        instance = {}
        agents = [{'hostname': 'foo.com', 'pid': 'slave(1)@1.2.3.4:5051'}]
        mock_allocated.return_value = 100
        assert get_mesos_state(instance, agents) == MesosAgentState.UNKNOWN


def test_allocated_cpu_resources(mock_agents_response):
    assert allocated_cpu_resources(mock_agents_response.json()['slaves'][0]) == 0
    assert allocated_cpu_resources(mock_agents_response.json()['slaves'][1]) == 0
    assert allocated_cpu_resources(mock_agents_response.json()['slaves'][2]) == 10


def test_find_largest_capacity_market_no_threshold(mock_market_capacities):
    assert find_largest_capacity_market(mock_market_capacities) == ('market-1', 1000)


def test_find_largest_capacity_empty_list(mock_market_capacities):
    assert find_largest_capacity_market({}) == (None, 0)


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
