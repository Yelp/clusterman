import behave
import mock
import staticconf.testing
from hamcrest import assert_that
from hamcrest import close_to
from hamcrest import contains
from hamcrest import equal_to

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.mesos.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup
from itests.environment import boto_patches
from itests.environment import make_asg
from itests.environment import make_sfr

_NUM_RESOURCE_GROUPS = 5


def mock_asgs(subnet_id):
    asgs = {}
    for i in range(_NUM_RESOURCE_GROUPS):
        asg_id = f'fake-asg-{i}'
        make_asg(asg_id, subnet_id)
        asgs[asg_id] = AutoScalingResourceGroup(asg_id)
    return asgs


def mock_sfrs(subnet_id):
    sfrgs = {}
    for _ in range(_NUM_RESOURCE_GROUPS):
        sfr = make_sfr(subnet_id)
        sfrid = sfr['SpotFleetRequestId']
        sfrgs[sfrid] = SpotFleetResourceGroup(sfrid)
    return sfrgs


@behave.fixture
def mock_agents_and_tasks(context):
    agents = []
    for rg in context.mesos_pool_manager.resource_groups.values():
        for instance in ec2_describe_instances(instance_ids=rg.instance_ids):
            context.agents.append({
                'pid': f'slave(1)@{instance["PrivateIpAddress"]}:1',
                'id': f'agent-{instance["InstanceId"]}',
                'hostname': 'host1'
            })
    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.agents',
        mock.PropertyMock(return_value=agents),
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks',
        mock.PropertyMock(return_value=[]),
    ), staticconf.testing.PatchConfiguration(
        {'mesos_clusters': {'mesos-test': {'max_weight_to_remove': 1000}}},
    ):
        yield


@behave.given('a mesos pool manager with (?P<rg_type>asgs|sfrs)')
def make_mesos_pool_manager(context, rg_type):
    behave.use_fixture(boto_patches, context)
    context.rg_type = rg_type
    with mock.patch(
        'clusterman.mesos.util.AutoScalingResourceGroup.load',
        return_value=(mock_asgs(context.subnet_id) if rg_type == 'asgs' else {})
    ), mock.patch(
        'clusterman.mesos.util.SpotFleetResourceGroup.load',
        return_value=(mock_sfrs(context.subnet_id) if rg_type == 'sfrs' else {})
    ):
        context.mesos_pool_manager = MesosPoolManager('mesos-test', 'bar')
        context.mesos_pool_manager.max_capacity = 101
        context.mesos_pool_manager.prune_excess_fulfilled_capacity = mock.Mock()


@behave.given('the target capacity of the first resource group is (?P<target>\d+)')
@behave.when('the target capacity of the first resource group is (?P<target>\d+)')
def external_target_capacity(context, target):
    if context.rg_type == 'asgs':
        autoscaling.set_desired_capacity(
            AutoScalingGroupName='fake-asg-0',
            DesiredCapacity=int(target),
            HonorCooldown=True,
        )
    elif context.rg_type == 'sfrs':
        ec2.modify_spot_fleet_request(
            SpotFleetRequestId=list(context.mesos_pool_manager.resource_groups.keys())[0],
            TargetCapacity=int(target),
        )


@behave.when('we request (?P<capacity>\d+) capacity(?P<dry_run> and dry-run is active)?')
def modify_capacity(context, capacity, dry_run):
    dry_run = True if dry_run else False
    context.original_capacities = [rg.target_capacity for rg in context.mesos_pool_manager.resource_groups.values()]
    context.mesos_pool_manager.modify_target_capacity(int(capacity), dry_run=dry_run)


@behave.then('the resource groups should be at minimum capacity')
def check_at_min_capacity(context):
    for rg in context.mesos_pool_manager.resource_groups.values():
        assert_that(rg.target_capacity, equal_to(1))


@behave.then('the resource group capacities should not change')
def check_unchanged_capacity(context):
    assert_that(
        [rg.target_capacity for rg in context.mesos_pool_manager.resource_groups.values()],
        contains(*context.original_capacities),
    )


@behave.then("the first resource group's capacity should not change")
def check_first_rg_capacity_unchanged(context):
    assert_that(
        list(context.mesos_pool_manager.resource_groups.values())[0].target_capacity,
        equal_to(context.original_capacities[0]),
    )


@behave.then('the(?P<remaining> remaining)? resource groups should have evenly-balanced capacity')
def check_target_capacity(context, remaining):
    target_capacity = 0
    if remaining:
        desired_capacity = (
            (context.mesos_pool_manager.target_capacity - context.original_capacities[0]) / (_NUM_RESOURCE_GROUPS - 1)
        )
    else:
        desired_capacity = context.mesos_pool_manager.target_capacity / _NUM_RESOURCE_GROUPS

    for i, rg in enumerate(context.mesos_pool_manager.resource_groups.values()):
        target_capacity += rg.target_capacity
        if remaining and i == 0:
            continue
        assert_that(
            rg.target_capacity,
            close_to(desired_capacity, 1.0),
        )
    assert_that(target_capacity, equal_to(context.mesos_pool_manager.target_capacity))
