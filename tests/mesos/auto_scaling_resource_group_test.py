import json

import mock
import pytest

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2_describe_instances
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


# not meant to be a fixture. simply replaces all of moto's invalid azs with
# proper ones
def patched_group_config(asg):
    asg_config = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg.id],
    )['AutoScalingGroups'][0]
    for i in range(len(asg_config['Instances'])):
        inst = asg_config['Instances'][i]
        # moto hardcodes the nonexistent us-east-1e for all ASG instances. We
        # fix that here by setting it to one of the ASG's AZs
        inst['AvailabilityZone'] = asg_config['AvailabilityZones'][0]
    return asg_config


@pytest.fixture
@mock.patch('threading.Thread', autospec=True)
def mock_asrg(mock_Thread, mock_asg_config):
    return AutoScalingResourceGroup(mock_asg_config['AutoScalingGroupName'])


@mock.patch('threading.Thread', autospec=True)
def test_init(mock_Thread, mock_asrg):
    mock_Thread.call_count == 1
    mock_Thread.call_args == mock.call(
        target=mock_asrg._terminate_detached_instances,
        daemon=True,
    )


def test_group_config(mock_asrg, mock_asg_config):
    group_config = mock_asrg._group_config

    assert group_config['AutoScalingGroupName'] == \
        mock_asg_config['AutoScalingGroupName']


def test_launch_config(mock_asrg, mock_launch_config):
    launch_config = mock_asrg._launch_config

    assert launch_config['LaunchConfigurationName'] == \
        mock_launch_config['LaunchConfigurationName']


def test_market_capacities(
    mock_asrg,
    mock_asg_config,
    mock_launch_config,
):
    asg_instance_market = InstanceMarket(
        mock_launch_config['InstanceType'],
        mock_asg_config['AvailabilityZones'][0],
    )

    with mock.patch.object(
        AutoScalingResourceGroup._group_config,
        'func',
        patched_group_config,
    ):
        market_capacities = mock_asrg.market_capacities

    assert asg_instance_market in market_capacities
    assert market_capacities[asg_instance_market] == \
        mock_asg_config['DesiredCapacity']


def test_modify_target_capacity_up(mock_asrg):
    new_desired_capacity = mock_asrg.target_capacity + 5

    mock_asrg.modify_target_capacity(
        new_desired_capacity,
        terminate_excess_capacity=False,
        dry_run=False,
        honor_cooldown=False,
    )

    assert mock_asrg.target_capacity == new_desired_capacity
    assert mock_asrg.fulfilled_capacity == new_desired_capacity


@pytest.mark.parametrize('terminate_excess_capacity', [True, False])
def test_modify_target_capacity_down(
    mock_asrg,
    terminate_excess_capacity,
):
    new_desired_capacity = mock_asrg.target_capacity - 5

    mock_asrg.modify_target_capacity(
        new_desired_capacity,
        terminate_excess_capacity=terminate_excess_capacity,
        dry_run=False,
        honor_cooldown=False,
    )

    if terminate_excess_capacity:
        assert mock_asrg.target_capacity == new_desired_capacity
        assert mock_asrg._scale_down_to is None
        assert mock_asrg.fulfilled_capacity == new_desired_capacity
    else:
        assert mock_asrg._scale_down_to == new_desired_capacity
        assert mock_asrg.fulfilled_capacity != new_desired_capacity


@pytest.mark.parametrize('new_desired_capacity', [0, 100])
def test_modify_target_capacity_min_max(
    mock_asrg,
    mock_asg_config,
    new_desired_capacity,
):
    mock_asrg.modify_target_capacity(
        new_desired_capacity,
        terminate_excess_capacity=False,
        dry_run=False,
        honor_cooldown=False,
    )

    desired_capacity = mock_asrg.target_capacity
    if new_desired_capacity < mock_asg_config['MinSize']:
        assert desired_capacity == mock_asg_config['MinSize']
    elif new_desired_capacity > mock_asg_config['MaxSize']:
        assert desired_capacity == mock_asg_config['MaxSize']


@pytest.mark.parametrize('_scale_down_to', [10, 5, None])
def test_terminate_instances_by_id(
    mock_asrg,
    mock_asg_config,
    _scale_down_to,
):
    mock_asrg._scale_down_to = _scale_down_to
    instance_ids = mock_asrg.instance_ids  # len = 10

    mock_asrg.terminate_instances_by_id(instance_ids)

    if _scale_down_to:
        assert mock_asrg.target_capacity == len(instance_ids) - _scale_down_to
    else:
        # New instances should've been spun up to take the place of the detached
        # instances
        assert set(instance_ids) != set(mock_asrg.instance_ids)
    assert mock_asrg._marked_for_death == set(instance_ids)


@mock.patch(
    'time.sleep',
    # to break out of infinite loop in test function
    mock.Mock(side_effect=AssertionError),
    autospec=None,
)
def test_terminate_detached_instances(
    mock_asrg,
    mock_asg_config,
):
    instance_ids = mock_asrg.instance_ids
    mock_asrg._marked_for_death.update(instance_ids)
    autoscaling.detach_instances(
        InstanceIds=instance_ids,
        AutoScalingGroupName=mock_asg_config['AutoScalingGroupName'],
        ShouldDecrementDesiredCapacity=False,
    )

    with pytest.raises(AssertionError):  # from mocked time.sleep
        mock_asrg._terminate_detached_instances()
    insts = ec2_describe_instances(instance_ids)

    assert set(instance_ids) == {inst['InstanceId'] for inst in insts}
    for inst in insts:
        assert inst['State']['Name'] in {'shutting-down', 'terminated'}


def test_get_asg_tags(mock_asrg, mock_asg_config):
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
