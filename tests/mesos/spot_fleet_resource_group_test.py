import mock
import pytest
from moto import mock_ec2

from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup


@pytest.fixture(autouse=True)
def setup_ec2():
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    yield
    mock_ec2_obj.stop()


@pytest.fixture
def mock_subnet():
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    return ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
    )


@pytest.fixture
def mock_spot_fleet_resource_group(mock_subnet):
    sfr_response = ec2.request_spot_fleet(
        SpotFleetRequestConfig={
            'AllocationStrategy': 'diversified',
            'SpotPrice': '2.0',
            'TargetCapacity': 10,
            'LaunchSpecifications': [
                {
                    'ImageId': 'ami-foo',
                    'SubnetId': mock_subnet['Subnet']['SubnetId'],
                    'WeightedCapacity': 2,
                    'InstanceType': 'c3.8xlarge',
                    'EbsOptimized': False,
                },
                {
                    'ImageId': 'ami-foo',
                    'SubnetId': mock_subnet['Subnet']['SubnetId'],
                    'WeightedCapacity': 1,
                    'InstanceType': 'i2.4xlarge',
                    'EbsOptimized': False,
                },
            ],
            'IamFleetRole': 'foo',
        },
    )
    return SpotFleetResourceGroup(sfr_response['SpotFleetRequestId'])


def test_fulfilled_capacity(mock_spot_fleet_resource_group):
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11


def test_modify_target_capacity_up(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(20)
    assert mock_spot_fleet_resource_group.target_capacity == 20
    assert len(mock_spot_fleet_resource_group.instances) == 13


def test_modify_target_capacity_down_no_terminate(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5)
    assert mock_spot_fleet_resource_group.target_capacity == 5
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11
    assert len(mock_spot_fleet_resource_group.instances) == 7


def test_modify_target_capacity_down_terminate(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5, should_terminate=True)
    assert mock_spot_fleet_resource_group.target_capacity == 5
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 5
    assert len(mock_spot_fleet_resource_group.instances) == 4


def test_terminate_all_instances_by_id(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instances)
    assert mock_spot_fleet_resource_group.instances == []
    assert mock_spot_fleet_resource_group.target_capacity == 0


def test_terminate_all_instances_by_id_small_batch(mock_spot_fleet_resource_group):
    # First make sure the terminate_instances method is called the right number of times
    with mock.patch('clusterman.mesos.spot_fleet_resource_group.ec2.terminate_instances') as mock_terminate:
        mock_terminate.return_value = {'TerminatingInstances': []}
        mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instances, batch_size=1)
        assert mock_terminate.call_count == 7

    # Next make sure that the answer is the same
    mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instances, batch_size=1)
    assert mock_spot_fleet_resource_group.instances == []
    assert mock_spot_fleet_resource_group.target_capacity == 0


@mock.patch('clusterman.mesos.spot_fleet_resource_group.logger')
def test_terminate_some_instances_missing(mock_logger, mock_spot_fleet_resource_group):
    with mock.patch('clusterman.mesos.spot_fleet_resource_group.ec2.terminate_instances') as mock_terminate:
        mock_terminate.return_value = {
            'TerminatingInstances': [
                {'InstanceId': i} for i in mock_spot_fleet_resource_group.instances[:3]
            ]
        }
        instances, weight = mock_spot_fleet_resource_group.terminate_instances_by_id(
            mock_spot_fleet_resource_group.instances)

        assert len(instances) == 3
        assert weight == 6
        assert mock_logger.warn.call_count == 1


@mock.patch('clusterman.mesos.spot_fleet_resource_group.logger')
def test_terminate_no_instances_by_id(mock_logger, mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.terminate_instances_by_id([])
    assert len(mock_spot_fleet_resource_group.instances) == 7
    assert mock_spot_fleet_resource_group.target_capacity == 10
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11
    assert mock_logger.warn.call_count == 1


def test_instances(mock_spot_fleet_resource_group):
    assert len(mock_spot_fleet_resource_group.instances) == 7


def test_market_capacities(mock_spot_fleet_resource_group, mock_subnet):
    assert mock_spot_fleet_resource_group.market_capacities == {
        InstanceMarket('c3.8xlarge', mock_subnet['Subnet']['AvailabilityZone']): 8,
        InstanceMarket('i2.4xlarge', mock_subnet['Subnet']['AvailabilityZone']): 3,
    }
