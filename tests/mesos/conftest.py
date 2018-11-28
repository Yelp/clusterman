import mock
import pytest
from moto import mock_autoscaling
from moto import mock_ec2


@pytest.fixture(autouse=True)
def setup_ec2():
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    yield
    mock_ec2_obj.stop()


@pytest.fixture(autouse=True)
def setup_autoscaling():
    mock_autoscaling_obj = mock_autoscaling()
    mock_autoscaling_obj.start()
    yield
    mock_autoscaling_obj.stop()


@pytest.fixture
def mock_agents_response():
    response = mock.Mock()
    response.json.return_value = {
        'slaves': [
            {
                'attributes': {
                    'blah': 10,
                    'pool': 'asdf',
                },
                'hostname': 'not-in-the-pool.yelpcorp.com',
            },
            {
                'hostname': 'asdf.yelpcorp.com',
                'used_resources': {'mem': 10},
            },
            {
                'attributes': {
                    'blah': 10,
                    'pool': 'bar',
                    'ssss': 'hjkl',
                },
                'hostname': 'im-in-the-pool.yelpcorp.com',
                'used_resources': {
                    'mem': 20,
                    'cpus': 10,
                },
            },
        ]
    }
    return response
