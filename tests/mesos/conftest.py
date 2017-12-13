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
        'slaves': [
            {
                'attributes': {
                    'blah': 10,
                    'role': 'asdf',
                },
                'hostname': 'not-in-the-role.yelpcorp.com',
            },
            {
                'hostname': 'asdf.yelpcorp.com',
                'used_resources': {'mem': 10},
            },
            {
                'attributes': {
                    'blah': 10,
                    'role': 'bar',
                    'ssss': 'hjkl',
                },
                'hostname': 'im-in-the-role.yelpcorp.com',
                'used_resources': {
                    'mem': 20,
                    'cpus': 10,
                },
            },
        ]
    }
