import mock
import pytest
import staticconf.testing

from clusterman.exceptions import MesosRoleManagerError
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.mesos_role_manager import NAMESPACE
from clusterman.mesos.mesos_role_manager import SERVICES_FILE
from tests.mesos.conftest import mock_open


@pytest.fixture
def mock_config_file():
    mock_config = {
        'defaults': {
            'min_capacity': 1,
            'max_capacity': 123,
        },
        'mesos': {
            'master_discovery': 'the.mesos.master',
            'resource_groups': {
                's3': {
                    'bucket': 'dummy-bucket',
                    'prefix': 'nowhere',
                }
            }
        }
    }

    with staticconf.testing.MockConfiguration(mock_config, namespace=NAMESPACE):
        yield


@pytest.fixture
def mock_mesos_role_manager(mock_config_file):
    with mock.patch('clusterman.mesos.mesos_role_manager.staticconf.YamlConfiguration'), \
            mock_open(SERVICES_FILE, 'the.mesos.master:\n  host: foo\n  port: 1234'), \
            mock.patch('clusterman.mesos.mesos_role_manager.load_spot_fleets_from_s3'):
        manager = MesosRoleManager('baz', 'dummy-file.yaml')
        return manager


def test_mesos_role_manager_init(mock_mesos_role_manager):
    assert mock_mesos_role_manager.name == 'baz'
    assert mock_mesos_role_manager.api_endpoint == 'http://foo:1234/api/v1'


@mock.patch('clusterman.mesos.mesos_role_manager.requests.post')
class TestAgentGenerator:
    def test_agent_generator_error(self, mock_post, mock_mesos_role_manager):
        mock_post.return_value.ok = False
        mock_post.return_value.text = 'dummy error'
        with pytest.raises(MesosRoleManagerError):
            for a in mock_mesos_role_manager._agents:
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
        agents = list(mock_mesos_role_manager._agents)
        assert len(agents) == 1
        assert agents[0]['agent_info']['hostname'] == 'im-in-the-role.yelpcorp.com'
