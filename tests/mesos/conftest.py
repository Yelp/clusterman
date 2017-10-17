import pytest
import staticconf.testing
from moto import mock_ec2

from tests.conftest import mock_open


@pytest.fixture(autouse=True)
def setup_ec2():
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    yield
    mock_ec2_obj.stop()


def cluster_configs():
    return {
        'mesos_clusters': {
            'mesos-test': {
                'leader_service': 'the.mesos.leader',
                'aws_region': 'us-test-3',
            },
        },
    }


@pytest.fixture(autouse=True)
def mock_service_config():
    mock_config = cluster_configs()
    mock_config.update({
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
        },
    })
    with staticconf.testing.MockConfiguration(mock_config):
        yield


@pytest.fixture(autouse=True)
def mock_aws_client_setup():
    with mock_open('/etc/secrets', '{"accessKeyId": "foo", "secretAccessKey": "bar"}'):
        yield


@pytest.fixture
def mock_agents_dict():
    return {
        'get_agents': {
            'agents': [
                {
                    'agent_info': {
                        'attributes': [
                            {'name': 'blah', 'scalar': {'value': 10}, 'type': 'SCALAR'},
                            {'name': 'role', 'text': {'value': 'asdf'}, 'type': 'TEXT'},
                        ],
                        'hostname': 'not-in-the-role.yelpcorp.com',
                    }
                },
                {
                    'agent_info': {
                        'hostname': 'asdf.yelpcorp.com',
                        'allocated_resources': [{'name': 'mem', 'scalar': {'value': 10}, 'type': 'SCALAR'}],
                    }
                },
                {
                    'agent_info': {
                        'attributes': [
                            {'name': 'blah', 'scalar': {'value': 10}, 'type': 'SCALAR'},
                            {'name': 'role', 'text': {'value': 'baz'}, 'type': 'TEXT'},
                            {'name': 'ssss', 'text': {'value': 'hjkl'}, 'type': 'TEXT'},
                        ],
                        'hostname': 'im-in-the-role.yelpcorp.com',
                        'allocated_resources': [
                            {'name': 'mem', 'scalar': {'value': 20}, 'type': 'SCALAR'},
                            {'name': 'cpus', 'scalar': {'value': 10}, 'type': 'SCALAR'},
                        ],
                    }
                },
            ]
        }
    }
