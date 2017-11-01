import pytest
from moto import mock_ec2


@pytest.fixture(autouse=True)
def setup_ec2():
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    yield
    mock_ec2_obj.stop()


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
