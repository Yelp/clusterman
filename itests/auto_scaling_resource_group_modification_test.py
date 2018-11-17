import mock
import pytest
from staticconf.testing import PatchConfiguration

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.run import setup_logging
from tests.conftest import clusterman_pool_config
from tests.conftest import main_clusterman_config
from tests.conftest import mock_aws_client_setup
from tests.mesos.auto_scaling_resource_group_test import mock_asg_config
from tests.mesos.auto_scaling_resource_group_test import mock_launch_config
from tests.mesos.auto_scaling_resource_group_test import patched_group_config
from tests.mesos.conftest import setup_autoscaling
from tests.mesos.conftest import setup_ec2
from tests.mesos.spot_fleet_resource_group_test import mock_subnet


pytest.mark.usefixtures(
    mock_aws_client_setup,
    main_clusterman_config,
    clusterman_pool_config,
    setup_autoscaling,
    setup_ec2,
    mock_subnet,
    mock_launch_config,
)


@mock.patch('threading.Thread', autospec=True)
@pytest.fixture
def mock_asgs(setup_autoscaling, setup_ec2, mock_launch_config, mock_subnet):
    # with mock.patch('threading.Thread', autospec=True):
    asgs = []
    for i in range(3):
        asg_config = mock_asg_config(  # creates the asg automatically
            mock_launch_config,
            mock_subnet,
            f'fake_asg_{i}',
            'mesos-test',  # cluster
            'bar',
        )
        asgs.append(AutoScalingResourceGroup(asg_config['AutoScalingGroupName']))
    return asgs


@pytest.fixture
def mock_manager(clusterman_pool_config, mock_asgs):
    setup_logging()

    class FakeResourceGroupClass(MesosPoolResourceGroup):
        @staticmethod
        def load(cluster, pool, config):
            return {asg.id: asg for asg in mock_asgs}

    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.RESOURCE_GROUPS',
        {'asg': FakeResourceGroupClass},
        autospec=None,
    ):
        return MesosPoolManager('mesos-test', 'bar')


def test_target_capacity(mock_manager):
    # each asg (there are 3) starts with a desired_capacity of 10
    assert mock_manager.target_capacity == 30


@mock.patch(
    'time.sleep',
    # to break out of infinite loop in test function
    mock.Mock(side_effect=AssertionError),
    autospec=None,
)
def test_detach_and_terminate(mock_asgs):
    asg = mock_asgs[0]
    to_terminate = asg.instance_ids[:3]

    with pytest.raises(AssertionError):
        asg.terminate_instances_by_id(to_terminate)
        asg._terminate_detached_instances()  # run in thread normally
    insts = ec2_describe_instances(to_terminate)

    assert set(to_terminate) == {inst['InstanceId'] for inst in insts}
    for inst in insts:
        assert inst['State']['Name'] in {'shutting-down', 'terminated'}


@mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager.prune_excess_fulfilled_capacity')
def test_scale_up(mock_prune, mock_manager):
    mock_manager.max_capacity = 70
    rgs = list(mock_manager.resource_groups.values())

    # balanced scale up
    mock_manager.modify_target_capacity(34)
    assert sorted([rg.target_capacity for rg in rgs]) == [11, 11, 12]

    # dry run -- target and fulfilled capacities should remain the same
    curr_fulfilled = sorted([rg.fulfilled_capacity for rg in rgs])
    mock_manager.modify_target_capacity(200, dry_run=True)
    assert sorted([rg.target_capacity for rg in rgs]) == [11, 11, 12]
    assert sorted([rg.fulfilled_capacity for rg in rgs]) == curr_fulfilled

    # balance scale up after external modifictation
    autoscaling.set_desired_capacity(
        AutoScalingGroupName=rgs[0].id,
        DesiredCapacity=4,
    )
    mock_manager.modify_target_capacity(40)
    assert sorted([rg.target_capacity for rg in rgs]) == [13, 13, 14]

    # imbalanced scale up
    autoscaling.set_desired_capacity(
        AutoScalingGroupName=rgs[2].id,
        DesiredCapacity=30,
    )
    mock_manager.modify_target_capacity(100)
    assert sorted([rg.target_capacity for rg in rgs]) == [20, 20, 30]

    assert mock_prune.call_count == 4


# need to match get_instance_market since moto doesnt seem to return a SubnetId
# for ASG instances, which doesnt allow us to accurately determine their markets
@mock.patch(
    'clusterman.mesos.mesos_pool_manager.get_instance_market',
    mock.Mock(return_value=InstanceMarket('t2.micro', 'us-west-2a'))
)
@mock.patch.object(AutoScalingResourceGroup._group_config, 'func', patched_group_config)
def test_scale_down(mock_manager):
    mock_manager.max_capacity = 90  # combined max of all asgs, each 30
    rgs = list(mock_manager.resource_groups.values())
    patched_config = {'mesos_clusters': {'mesos-test': {'max_weight_to_remove': 1000}}}

    # all instances have agents with 0 tasks and are thus killable
    agents = []
    for rg in rgs:
        for inst in ec2_describe_instances(instance_ids=rg.instance_ids):
            agents.append({
                'pid': f'slave(1)@{inst["PrivateIpAddress"]}:1',
                'id': f'agent-{inst["InstanceId"]}',
                'hostname': 'host1',
            })

    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.agents',
        agents,
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks',
        [],
    ), PatchConfiguration(patched_config):
        mock_manager.modify_target_capacity(60)

        # balanced scale down
        mock_manager.modify_target_capacity(40)
        assert sorted([rg.target_capacity for rg in rgs]) == [13, 13, 14]

        # dry run
        curr_fulfilled = sorted([rg.fulfilled_capacity for rg in rgs])
        mock_manager.modify_target_capacity(10, dry_run=True)
        assert sorted([rg.target_capacity for rg in rgs]) == [13, 13, 14]
        assert sorted([rg.fulfilled_capacity for rg in rgs]) == curr_fulfilled

        # balanced scaled down after external modification
        autoscaling.set_desired_capacity(
            AutoScalingGroupName=rgs[0].id,
            DesiredCapacity=20,
        )
        mock_manager.modify_target_capacity(30)
        assert sorted([rg.target_capacity for rg in rgs]) == [10, 10, 10]

        # imbalanced scale down
        autoscaling.set_desired_capacity(
            AutoScalingGroupName=rgs[1].id,
            DesiredCapacity=4,
        )
        mock_manager.modify_target_capacity(20)
        assert sorted([rg.target_capacity for rg in rgs]) == [4, 8, 8]
