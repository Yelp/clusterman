import json

import mock
import pytest
from moto import mock_s3

from clusterman.aws.client import ec2
from clusterman.aws.client import s3
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.spot_fleet_resource_group import get_spot_fleet_request_tags
from clusterman.mesos.spot_fleet_resource_group import load_spot_fleets_from_ec2
from clusterman.mesos.spot_fleet_resource_group import load_spot_fleets_from_s3
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup


@pytest.fixture
def mock_subnet():
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    return ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
        AvailabilityZone='us-west-2a'
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
                    # note that this is not useful until we solve
                    # https://github.com/spulec/moto/issues/1644
                    'TagSpecifications': [{
                        'ResourceType': 'instance',
                        'Tags': [{
                            'Key': 'foo',
                            'Value': 'bar',
                        }],
                    }],
                },
                {
                    'ImageId': 'ami-foo',
                    'SubnetId': mock_subnet['Subnet']['SubnetId'],
                    'WeightedCapacity': 1,
                    'InstanceType': 'i2.4xlarge',
                    'EbsOptimized': False,
                    'TagSpecifications': [{
                        'ResourceType': 'instance',
                        'Tags': [{
                            'Key': 'foo',
                            'Value': 'bar',
                        }],
                    }],
                },
            ],
            'IamFleetRole': 'foo',
        },
    )
    return SpotFleetResourceGroup(sfr_response['SpotFleetRequestId'])


@mock_s3
@pytest.fixture
def mock_sfr_bucket():
    s3.create_bucket(Bucket='fake-clusterman-sfrs')
    s3.put_object(Bucket='fake-clusterman-sfrs', Key='fake-region/sfr-1.json', Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request': {
                'id': 'sfr-1',
                'pool': 'my-pool'
            }
        }
    }).encode())
    s3.put_object(Bucket='fake-clusterman-sfrs', Key='fake-region/sfr-2.json', Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request': {
                'id': 'sfr-2',
                'pool': 'my-pool'
            }
        }
    }).encode())
    s3.put_object(Bucket='fake-clusterman-sfrs', Key='fake-region/sfr-3.json', Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request': {
                'id': 'sfr-3',
                'pool': 'not-my-pool'
            }
        }
    }).encode())


def test_load_spot_fleets_from_s3(mock_sfr_bucket):
    with mock.patch('clusterman.mesos.spot_fleet_resource_group.SpotFleetResourceGroup.__init__') as mock_init:
        mock_init.return_value = None
        sfrgs = load_spot_fleets_from_s3('fake-clusterman-sfrs', 'fake-region', 'my-pool')
        assert len(sfrgs) == 2
        assert mock_init.call_args_list[0][0][0] == 'sfr-1'
        assert mock_init.call_args_list[1][0][0] == 'sfr-2'


def test_load_spot_fleets_from_ec2():
    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.SpotFleetResourceGroup.__init__',
    ) as mock_init, mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.get_spot_fleet_request_tags',
    ) as mock_get_spot_fleet_request_tags:
        mock_init.return_value = None
        mock_get_spot_fleet_request_tags.return_value = {
            'sfr-123': {
                'some': 'tag',
                'paasta': 'true',
                'pool': 'default',
                'cluster': 'westeros-prod',
            },
            'sfr-456': {
                'some': 'tag',
                'paasta': 'true',
                'pool': 'another',
                'cluster': 'westeros-prod',
            },
            'sfr-789': {
                'some': 'tag',
                'paasta': 'true',
                'cluster': 'westeros-prod',
            },
        }
        spot_fleets = load_spot_fleets_from_ec2(cluster='westeros-prod', pool='default')
        assert len(spot_fleets) == 1
        mock_init.assert_called_with('sfr-123')


def test_get_spot_fleet_request_tags(mock_spot_fleet_resource_group):
    # doing this the old fashioned way until
    # https://github.com/spulec/moto/issues/1644 is fixed
    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.ec2.describe_spot_fleet_requests',
    ) as mock_describe_spot_fleet_requests:
        mock_describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [{
                'SpotFleetRequestId': 'sfr-12',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [{
                        'TagSpecifications': [{
                            'Tags': [{
                                'Key': 'foo',
                                'Value': 'bar',
                            }],
                        }],
                    }],
                },
            },
                {
                'SpotFleetRequestId': 'sfr-34',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [{}],
                },
            },
                {
                'SpotFleetRequestId': 'sfr-56',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [],
                },
            },
                {
                'SpotFleetRequestId': 'sfr-78',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [{
                        'TagSpecifications': [{
                            'Tags': [{
                                'Key': 'foo',
                                'Value': 'bar',
                            },
                                {
                                'Key': 'spam',
                                'Value': 'baz',
                            }],
                        }],
                    }],
                },
            }],
        }
        sfrs = get_spot_fleet_request_tags()
        expected = {
            'sfr-12': {
                'foo': 'bar'
            },
            'sfr-34': {},
            'sfr-56': {},
            'sfr-78': {
                'foo': 'bar',
                'spam': 'baz'
            }
        }
        assert sfrs == expected


# NOTE: These tests are fairly brittle, as it depends on the implementation of modify_spot_fleet_request
# inside moto.  So if moto's implementation changes, these tests could break.  However, I still think
# these tests cover important functionality, and I can't think of a way to make them less brittle.
def test_fulfilled_capacity(mock_spot_fleet_resource_group):
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11


def test_modify_target_capacity_up(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(20)
    assert mock_spot_fleet_resource_group.target_capacity == 20
    assert len(mock_spot_fleet_resource_group.instance_ids) == 13


def test_modify_target_capacity_down_no_terminate(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5)
    assert mock_spot_fleet_resource_group.target_capacity == 5
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11
    assert len(mock_spot_fleet_resource_group.instance_ids) == 7


def test_modify_target_capacity_down_terminate(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5, terminate_excess_capacity=True)
    assert mock_spot_fleet_resource_group.target_capacity == 5
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 5
    assert len(mock_spot_fleet_resource_group.instance_ids) == 4


def test_modify_target_capacity_dry_run(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5, dry_run=True)
    assert mock_spot_fleet_resource_group.target_capacity == 10
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11


def test_terminate_all_instances_by_id(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instance_ids)
    assert mock_spot_fleet_resource_group.instance_ids == []


def mock_describe_instances_with_missing_subnet(orig):
    def describe_instances_with_missing_subnet(InstanceIds):
        ret = orig(InstanceIds=InstanceIds)
        ret['Reservations'][0]['Instances'][0].pop('SubnetId')
        return ret
    return describe_instances_with_missing_subnet


def test_terminate_instance_missing_subnet(mock_spot_fleet_resource_group):
    ec2_describe = ec2.describe_instances
    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.ec2.describe_instances',
        wraps=mock_describe_instances_with_missing_subnet(ec2_describe)
    ):
        mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instance_ids)


def test_terminate_all_instances_by_id_small_batch(mock_spot_fleet_resource_group):
    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.ec2.terminate_instances',
        wraps=ec2.terminate_instances,
    ) as mock_terminate:
        mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instance_ids, batch_size=1)
        assert mock_terminate.call_count == 7
        assert mock_spot_fleet_resource_group.instance_ids == []


@mock.patch('clusterman.mesos.spot_fleet_resource_group.logger')
def test_terminate_some_instances_missing(mock_logger, mock_spot_fleet_resource_group):
    with mock.patch('clusterman.mesos.spot_fleet_resource_group.ec2.terminate_instances') as mock_terminate:
        mock_terminate.return_value = {
            'TerminatingInstances': [
                {'InstanceId': i} for i in mock_spot_fleet_resource_group.instance_ids[:3]
            ]
        }
        instances = mock_spot_fleet_resource_group.terminate_instances_by_id(mock_spot_fleet_resource_group.instance_ids)

        assert len(instances) == 3
        assert mock_logger.warn.call_count == 2


@mock.patch('clusterman.mesos.spot_fleet_resource_group.logger')
def test_terminate_no_instances_by_id(mock_logger, mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.terminate_instances_by_id([])
    assert len(mock_spot_fleet_resource_group.instance_ids) == 7
    assert mock_spot_fleet_resource_group.target_capacity == 10
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11
    assert mock_logger.warn.call_count == 1


def test_instances(mock_spot_fleet_resource_group):
    assert len(mock_spot_fleet_resource_group.instance_ids) == 7


def test_market_capacities(mock_spot_fleet_resource_group, mock_subnet):
    assert mock_spot_fleet_resource_group.market_capacities == {
        InstanceMarket('c3.8xlarge', mock_subnet['Subnet']['AvailabilityZone']): 8,
        InstanceMarket('i2.4xlarge', mock_subnet['Subnet']['AvailabilityZone']): 3,
    }
