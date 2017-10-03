import mock
import pytest

from clusterman.exceptions import MesosPoolError
from clusterman.mesos.mesos_pool import MesosPool


@pytest.fixture
def mock_mesos_pool():
    with mock.patch('builtins.open', mock.mock_open(read_data="{'mesos-master': {'host': 'foo', 'port': 1234}}")):
        return MesosPool('baz', 'dummy-file.yaml', 'mesos-master')


def test_mesos_pool_init(mock_mesos_pool):
    assert mock_mesos_pool.name == 'baz'
    assert mock_mesos_pool.api_endpoint == 'http://foo:1234/api/v1'


@mock.patch('clusterman.mesos.mesos_pool.requests.post')
class TestAgentGenerator:
    def test_agent_generator_error(self, mock_post, mock_mesos_pool):
        mock_post.return_value.ok = False
        mock_post.return_value.text = 'dummy error'
        with pytest.raises(MesosPoolError):
            for a in mock_mesos_pool._agents():
                print(a)

    def test_agent_generator_filter_pools(self, mock_post, mock_mesos_pool):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {
            'get_agents': {
                'agents': [
                    {
                        'agent_info': {
                            'attributes': [
                                {'name': 'blah', 'scalar': {'value': 10}},
                                {'name': 'pool', 'text': {'value': 'asdf'}},
                            ],
                            'hostname': 'not-in-the-pool.yelpcorp.com',
                        }
                    },
                    {
                        'agent_info': {
                            'attributes': [
                                {'name': 'blah', 'scalar': {'value': 10}},
                                {'name': 'pool', 'text': {'value': 'baz'}},
                                {'name': 'ssss', 'text': {'value': 'hjkl'}},
                            ],
                            'hostname': 'im-in-the-pool.yelpcorp.com',
                        }
                    },
                ]
            }
        }
        agents = list(mock_mesos_pool._agents())
        assert len(agents) == 1
        assert agents[0]['agent_info']['hostname'] == 'im-in-the-pool.yelpcorp.com'
