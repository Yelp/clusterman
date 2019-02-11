import json

import mock
import pytest

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.auto_scaling_resource_group import _get_asg_tags
from clusterman.mesos.auto_scaling_resource_group import AutoScalingResourceGroup


@pytest.fixture
def mock_launch_config():
    launch_config = {
        'LaunchConfigurationName': 'fake_launch_config',
        'ImageId': 'fake_ami',
        'InstanceType': 't2.2xlarge',
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
        'NewInstancesProtectedFromScaleIn': True,
    }
    autoscaling.create_auto_scaling_group(**asg)

    return asg


def test_group_config(mock_asg_config):
    mock_asrg = AutoScalingResourceGroup.__new__(AutoScalingResourceGroup)  # skip init
    mock_asrg.group_id = mock_asg_config['AutoScalingGroupName']

    group_config = mock_asrg._group_config

    assert group_config['AutoScalingGroupName'] == \
        mock_asg_config['AutoScalingGroupName']


@pytest.fixture
def mock_asrg(mock_asg_config):
    return AutoScalingResourceGroup(mock_asg_config['AutoScalingGroupName'])


def test_launch_config(mock_asrg, mock_launch_config):
    launch_config = mock_asrg._launch_config

    assert launch_config['LaunchConfigurationName'] == \
        mock_launch_config['LaunchConfigurationName']


def test_launch_config_retry(mock_asrg, mock_launch_config):
    no_configs = dict(LaunchConfigurations=[])
    good_configs = dict(LaunchConfigurations=[mock_launch_config])
    mock_describe_launch_configs = mock.Mock(side_effect=[
        no_configs, good_configs,
    ])

    with mock.patch(
        'clusterman.aws.client.autoscaling.describe_launch_configurations',
        mock_describe_launch_configs,
    ):
        launch_config = mock_asrg._launch_config

    assert launch_config == mock_launch_config
    assert mock_describe_launch_configs.call_count == 2


@pytest.mark.parametrize('instance_type', ['t2.2xlarge', 'm5.large'])
def test_market_weight(mock_asrg, instance_type):
    market_weight = mock_asrg.market_weight(InstanceMarket(instance_type, 'us-west-2a'))

    assert market_weight == (1.0 if instance_type == 't2.2xlarge' else 0)


def test_market_capacities(
    mock_asrg,
    mock_asg_config,
    mock_launch_config,
):
    asg_instance_market = InstanceMarket(
        mock_launch_config['InstanceType'],
        mock_asg_config['AvailabilityZones'][0],
    )

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
    old_desired_capacity = mock_asrg.target_capacity
    new_desired_capacity = old_desired_capacity - 5

    mock_asrg.modify_target_capacity(
        new_desired_capacity,
        terminate_excess_capacity=terminate_excess_capacity,
        dry_run=False,
        honor_cooldown=False,
    )

    assert mock_asrg.target_capacity == new_desired_capacity
    if terminate_excess_capacity:
        assert mock_asrg.fulfilled_capacity == new_desired_capacity
    else:
        assert mock_asrg.fulfilled_capacity == old_desired_capacity


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

    if new_desired_capacity < mock_asg_config['MinSize']:
        assert mock_asrg.target_capacity == mock_asg_config['MinSize']
    elif new_desired_capacity > mock_asg_config['MaxSize']:
        assert mock_asrg.target_capacity == mock_asg_config['MaxSize']


def test_terminate_instances_by_id(
    mock_asrg,
    mock_asg_config,
):
    mock_asrg.modify_target_capacity(30)
    instance_ids = mock_asrg.instance_ids[:25]

    mock_asrg.terminate_instances_by_id(instance_ids)
    terminated_instances = ec2_describe_instances(instance_ids)

    for inst in terminated_instances:
        assert inst['State']['Name'] in {'shutting-down', 'terminated'}


def test_get_asg_tags(mock_asrg, mock_asg_config):
    asg_id_to_tags = _get_asg_tags()

    assert mock_asg_config['AutoScalingGroupName'] in asg_id_to_tags
    tags = asg_id_to_tags[mock_asg_config['AutoScalingGroupName']]
    assert 'fake_tag_key' in tags
    assert tags['fake_tag_key'] == 'fake_tag_value'


@pytest.mark.parametrize('cluster', ['fake_cluster', 'nonexistent_cluster'])
def test_load(mock_asg_config, cluster):
    asgs = AutoScalingResourceGroup.load(
        cluster,
        'fake_pool',
        config={'tag': 'puppet:role::paasta'},
    )

    if cluster == 'fake_cluster':
        assert mock_asg_config['AutoScalingGroupName'] in asgs
    else:
        assert len(asgs) == 0
