import mock
import pytest

from clusterman.aws.ec2_fleet_resource_group import EC2FleetResourceGroup


MOCK_FLEET_ID = 'fleet-abcdef1234567890'
MOCK_DESCRIBE_FLEETS = {
    'Fleets': [{
        'Type': 'maintain',
        'FulfilledCapacity': 1.0,
        'LaunchTemplateConfigs': [
            {
                'LaunchTemplateSpecification': {
                    'Version': '1',
                    'LaunchTemplateId': 'lt-03e81ab7ba4d9aa34',
                },
                'Overrides': [
                    {
                        'AvailabilityZone': 'us-west-1a',
                        'MaxPrice': '2.0',
                        'WeightedCapacity': 1.0,
                        'Priority': 1.0,
                        'SubnetId': 'subnet-abcdef01',
                        'InstanceType': 'm5.4xlarge',
                    }
                ]
            }
        ],
        'Tags': [
            {
                'Value': '{"paasta_cluster": "mesostest", "pool": "default"}',
                'Key': 'puppet:role::paasta',
            },
        ],
        'TerminateInstancesWithExpiration': False,
        'TargetCapacitySpecification': {
            'OnDemandTargetCapacity': 0,
            'SpotTargetCapacity': 1,
            'TotalTargetCapacity': 1,
            'DefaultTargetCapacityType': 'spot',
        },
        'FulfilledOnDemandCapacity': 0.0,
        'ActivityStatus': 'error',
        'FleetId': MOCK_FLEET_ID,
        'ReplaceUnhealthyInstances': False,
        'SpotOptions': {
            'InstanceInterruptionBehavior': 'terminate',
            'AllocationStrategy': 'diversified',
        },
        'OnDemandOptions': {
            'AllocationStrategy': 'lowestPrice'
        },
        'FleetState': 'active',
        'ExcessCapacityTerminationPolicy': 'no-termination',
        'CreateTime': '2019-02-11T19:30:17.000Z',
    }]
}


@pytest.fixture
def mock_ec2_things():
    with mock.patch(
        'clusterman.aws.ec2_fleet_resource_group.ec2.describe_fleets',
        return_value=MOCK_DESCRIBE_FLEETS,
    ), mock.patch(
        'clusterman.aws.markets.ec2.describe_subnets',
        return_value={'Subnets': [{'AvailabilityZone': 'us-west-1a'}]},
    ):
        yield


@pytest.fixture
def mock_describe_fleets_paginator_response():
    with mock.patch(
        'clusterman.aws.ec2_fleet_resource_group.ec2.get_paginator',
    ) as mock_paginator:
        mock_paginator.return_value.paginate.return_value = [MOCK_DESCRIBE_FLEETS]
        yield


@pytest.fixture
def mock_ec2_fleet_resource_group(mock_ec2_things):
    return EC2FleetResourceGroup(MOCK_FLEET_ID)


def test_get_ec2_fleet_request_tags(mock_describe_fleets_paginator_response):
    # moto doesn't support ec2fleets right now so doing this the old way
    fleets = EC2FleetResourceGroup._get_resource_group_tags()
    expected = {
        MOCK_FLEET_ID: {
            'puppet:role::paasta': '{"paasta_cluster": "mesostest", "pool": "default"}',
        }
    }
    assert fleets == expected


def test_fulfilled_capacity(mock_ec2_fleet_resource_group):
    assert mock_ec2_fleet_resource_group.fulfilled_capacity == 1


def test_modify_target_capacity_stale(mock_ec2_fleet_resource_group):
    with mock.patch(
        'clusterman.aws.ec2_fleet_resource_group.EC2FleetResourceGroup.is_stale',
        mock.PropertyMock(return_value=True),
    ):
        mock_ec2_fleet_resource_group.modify_target_capacity(20)
        assert mock_ec2_fleet_resource_group.target_capacity == 0


@pytest.mark.parametrize('terminate', [True, False])
def test_modify_target_capacity(mock_ec2_fleet_resource_group, terminate):
    with mock.patch(
        'clusterman.aws.ec2_fleet_resource_group.ec2.modify_fleet',
    ) as mock_modify:
        mock_ec2_fleet_resource_group.modify_target_capacity(20, terminate_excess_capacity=terminate)
        assert mock_modify.call_args == mock.call(
            FleetId=MOCK_FLEET_ID,
            TargetCapacitySpecification={
                'TotalTargetCapacity': 20,
            },
            ExcessCapacityTerminationPolicy='termination' if terminate else 'no-termination',
        )


def test_modify_target_capacity_dry_run(mock_ec2_fleet_resource_group):
    with mock.patch(
        'clusterman.aws.ec2_fleet_resource_group.ec2.modify_fleet',
    ) as mock_modify:
        mock_ec2_fleet_resource_group.modify_target_capacity(5, dry_run=True)
        assert mock_modify.call_count == 0


def test_is_stale(mock_ec2_fleet_resource_group):
    assert not mock_ec2_fleet_resource_group.is_stale
