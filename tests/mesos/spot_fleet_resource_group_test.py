import mock
import pytest
from moto import mock_ec2

from clusterman.aws.client import ec2
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup


@pytest.fixture(autouse=True)
def setup_ec2(request):
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    request.addfinalizer(lambda: mock_ec2_obj.stop())


@pytest.fixture
def mock_spot_fleet_resource_group():
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    subnet_response = ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
    )
    sfr_response = ec2.request_spot_fleet(
        SpotFleetRequestConfig={
            'AllocationStrategy': 'diversified',
            'SpotPrice': '2.0',
            'TargetCapacity': 10,
            'LaunchSpecifications': [{
                'ImageId': 'ami-foo',
                'SubnetId': subnet_response['Subnet']['SubnetId'],
                'WeightedCapacity': 1,
                'InstanceType': 'c3.8xlarge',
                'EbsOptimized': False,
            }],
            'IamFleetRole': 'foo',
        },
    )
    return SpotFleetResourceGroup(sfr_response['SpotFleetRequestId'])

# TODO (CLUSTERMAN-76) let's make some tests that actually do something


def test_modify_target_capacity(mock_spot_fleet_resource_group):
    print('modify_spot_fleet_request not implemented by moto, nothing to test')
    pass

# TODO (CLUSTERMAN-76) let's make some tests that actually do something


def test_terminate_instances_by_id(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity = mock.Mock()
    mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instances)
    print('not asserting anything yet')
    pass


def test_instances(mock_spot_fleet_resource_group):
    assert len(mock_spot_fleet_resource_group.instances) == mock_spot_fleet_resource_group.target_capacity
