import mock
import pytest
from moto import mock_autoscaling
from moto import mock_ec2

from clusterman.aws.client import ec2


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
def mock_subnet():
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    return ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
        AvailabilityZone='us-west-2a'
    )


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
