import mock
import pytest

from clusterman.exceptions import MesosRoleManagerError
from clusterman.mesos.mesos_role_manager import MesosRoleManager


@pytest.fixture
def mock_mesos_role_manager():
    with mock.patch('builtins.open', mock.mock_open(read_data="{'mesos-master': {'host': 'foo', 'port': 1234}}")):
        return MesosRoleManager('baz', 'dummy-file.yaml', 'mesos-master')


def test_mesos_role_manager_init(mock_mesos_role_manager):
    assert mock_mesos_role_manager.name == 'baz'
    assert mock_mesos_role_manager.api_endpoint == 'http://foo:1234/api/v1'


@mock.patch('clusterman.mesos.mesos_role_manager.requests.post')
class TestAgentGenerator:
    def test_agent_generator_error(self, mock_post, mock_mesos_role_manager):
        mock_post.return_value.ok = False
        mock_post.return_value.text = 'dummy error'
        with pytest.raises(MesosRoleManagerError):
            for a in mock_mesos_role_manager._agents():
                print(a)

    def test_agent_generator_filter_roles(self, mock_post, mock_mesos_role_manager):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {
            'get_agents': {
                'agents': [
                    {
                        'agent_info': {
                            'attributes': [
                                {'name': 'blah', 'scalar': {'value': 10}},
                                {'name': 'role', 'text': {'value': 'asdf'}},
                            ],
                            'hostname': 'not-in-the-role.yelpcorp.com',
                        }
                    },
                    {
                        'agent_info': {
                            'attributes': [
                                {'name': 'blah', 'scalar': {'value': 10}},
                                {'name': 'role', 'text': {'value': 'baz'}},
                                {'name': 'ssss', 'text': {'value': 'hjkl'}},
                            ],
                            'hostname': 'im-in-the-role.yelpcorp.com',
                        }
                    },
                ]
            }
        }
        agents = list(mock_mesos_role_manager._agents())
        assert len(agents) == 1
        assert agents[0]['agent_info']['hostname'] == 'im-in-the-role.yelpcorp.com'
