import json

import mock
import pytest

from clusterman.aws.client import autoscaling
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.auto_scaling_resource_group import _get_asg_tags
from clusterman.mesos.auto_scaling_resource_group import AutoScalingResourceGroup
from tests.mesos.conftest import setup_autoscaling
from tests.mesos.conftest import setup_ec2
from tests.mesos.spot_fleet_resource_group_test import mock_subnet


pytest.mark.usefixtures(setup_autoscaling, setup_ec2, mock_subnet)


@pytest.fixture
def mock_launch_config():
    launch_config = {
        'LaunchConfigurationName': 'fake_launch_config',
        'ImageId': 'fake_ami',
        'InstanceType': 't2.micro',
    }
    autoscaling.create_launch_configuration(**launch_config)
    return launch_config


@pytest.fixture
def mock_asg_name():
    return 'fake_asg'


@pytest.fixture
def mock_cluster():
    return 'fake_cluster'


@pytest.fixture
def mock_pool():
    return 'fake_pool'


@pytest.fixture
def mock_asg_config(
    mock_launch_config,
    mock_subnet,
    mock_asg_name,
    mock_cluster,
    mock_pool,
):
    asg = {
        'AutoScalingGroupName': mock_asg_name,
        'LaunchConfigurationName': mock_launch_config['LaunchConfigurationName'],
        'MinSize': 1,
        'MaxSize': 30,
        'DesiredCapacity': 10,
        'AvailabilityZones': ['us-west-2a'],
        'VPCZoneIdentifier': mock_subnet['Subnet']['SubnetId'],
        'Tags': [
            {
                'Key': 'puppet:role::paasta',
                'Value': json.dumps({
                    'pool': mock_pool,
                    'paasta_cluster': mock_cluster
                }),
            }, {
                'Key': 'fake_tag_key',
                'Value': 'fake_tag_value',
            },
        ],
    }
    autoscaling.create_auto_scaling_group(**asg)

    return asg


@pytest.fixture
@mock.patch('threading.Thread', autospec=True)
def mock_auto_scaling_resource_group(mock_Thread, mock_asg_config):
    return AutoScalingResourceGroup(mock_asg_config['AutoScalingGroupName'])


@mock.patch('threading.Thread', autospec=True)
def test_init(mock_Thread, mock_auto_scaling_resource_group):
    mock_Thread.call_count == 1
    mock_Thread.call_args == mock.call(
        target=mock_auto_scaling_resource_group._terminate_detached_instances,
        daemon=True,
    )


def test_group_config(mock_auto_scaling_resource_group, mock_asg_config):
    group_config = mock_auto_scaling_resource_group._group_config

    assert group_config['AutoScalingGroupName'] == \
        mock_asg_config['AutoScalingGroupName']


def test_launch_config(mock_auto_scaling_resource_group, mock_launch_config):
    launch_config = mock_auto_scaling_resource_group._launch_config

    assert launch_config['LaunchConfigurationName'] == \
        mock_launch_config['LaunchConfigurationName']


def test_market_capacities(
    mock_auto_scaling_resource_group,
    mock_asg_config,
    mock_launch_config,
):
    asg_instance_market = InstanceMarket(
        mock_launch_config['InstanceType'],
        mock_asg_config['AvailabilityZones'][0],
    )

    # moto hard codes us-east-1e as the region for all instances, which is not
    # valid according to InstanceMarket. Until this is changed, we need to
    # manually set the region ourselves
    with mock.patch(
        'clusterman.aws.client.autoscaling.describe_auto_scaling_groups',
        mock.Mock(return_value=dict(
            AutoScalingGroups=[dict(
                **mock_asg_config,
                Instances=[{
                    'InstanceId': 'fake_instance_id',
                    'AvailabilityZone': mock_asg_config['AvailabilityZones'][0],
                }] * mock_asg_config['DesiredCapacity'],
            )]
        )),
        autospec=None,
    ):
        market_capacities = mock_auto_scaling_resource_group.market_capacities

    assert asg_instance_market in market_capacities
    assert market_capacities[asg_instance_market] == \
        mock_asg_config['DesiredCapacity']


@pytest.mark.parametrize('new_desired_capacity', [0, 11, 100])
def test_modify_target_capacity(
    mock_auto_scaling_resource_group,
    mock_asg_config,
    new_desired_capacity,
):
    kwargs = dict(
        terminate_excess_capacity=False,
        dry_run=False,
        honor_cooldown=False,
    )

    mock_auto_scaling_resource_group.modify_target_capacity(
        new_desired_capacity,
        **kwargs,
    )

    desired_capacity = \
        mock_auto_scaling_resource_group._group_config['DesiredCapacity']
    if new_desired_capacity < mock_asg_config['MinSize']:
        assert desired_capacity == mock_asg_config['MinSize']
    elif new_desired_capacity > mock_asg_config['MaxSize']:
        assert desired_capacity == mock_asg_config['MaxSize']
    else:
        assert desired_capacity == new_desired_capacity


@pytest.mark.parametrize('decrement_desired_capacity', [True, False])
def test_terminate_instances_by_id(
    mock_auto_scaling_resource_group,
    mock_asg_config,
    decrement_desired_capacity,
):
    instance_ids = mock_auto_scaling_resource_group.instance_ids

    mock_auto_scaling_resource_group.terminate_instances_by_id(
        instance_ids,
        decrement_desired_capacity=decrement_desired_capacity,
    )

    if decrement_desired_capacity:
        assert len(mock_auto_scaling_resource_group.instance_ids) == 0
    else:
        # New instances should've been spun up to take the place of the detached
        # instances
        assert set(instance_ids) != \
            set(mock_auto_scaling_resource_group.instance_ids)
    assert mock_auto_scaling_resource_group._marked_for_death == set(instance_ids)


@mock.patch(
    'clusterman.aws.client.ec2.terminate_instances',
    lambda InstanceIds: {
        'TerminatingInstances':
            [{'InstanceId': inst_id} for inst_id in InstanceIds],
    },
    autospec=None,
)
@mock.patch(
    'time.sleep',
    # to break out of infinite loop in test function
    mock.Mock(side_effect=AssertionError),
    autospec=None,
)
def test_terminate_detached_instances(
    mock_auto_scaling_resource_group,
    mock_asg_config,
):
    instance_ids = mock_auto_scaling_resource_group.instance_ids
    mock_auto_scaling_resource_group._marked_for_death.update(instance_ids)
    autoscaling.detach_instances(
        InstanceIds=instance_ids,
        AutoScalingGroupName=mock_asg_config['AutoScalingGroupName'],
        ShouldDecrementDesiredCapacity=False,
    )

    with pytest.raises(AssertionError):  # from mocked time.sleep
        mock_auto_scaling_resource_group._terminate_detached_instances()

    assert len(mock_auto_scaling_resource_group._marked_for_death) == 0


def test_get_asg_tags(mock_auto_scaling_resource_group, mock_asg_config):
    asg_id_to_tags = _get_asg_tags()

    assert mock_asg_config['AutoScalingGroupName'] in asg_id_to_tags
    tags = asg_id_to_tags[mock_asg_config['AutoScalingGroupName']]
    assert 'fake_tag_key' in tags
    assert tags['fake_tag_key'] == 'fake_tag_value'


@pytest.mark.parametrize('cluster', ['fake_cluster', 'nonexistent_cluster'])
def test_load(mock_asg_config, cluster):
    # We need to mock out AutoScalingResourceGroup because initializing one
    # spawns a thread. We can't mock that out because boto/moto uses them for
    # AWS requests.
    with mock.patch(
        'clusterman.mesos.auto_scaling_resource_group.AutoScalingResourceGroup',
        autospec=True,
    ):
        asgs = AutoScalingResourceGroup.load(
            cluster,
            'fake_pool',
            config={'tag': 'puppet:role::paasta'},
        )

    if cluster == 'fake_cluster':
        assert mock_asg_config['AutoScalingGroupName'] in asgs
    else:
        assert len(asgs) == 0
