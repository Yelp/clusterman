import behave
import mock
import staticconf.testing
from hamcrest import assert_that
from hamcrest import close_to
from hamcrest import contains
from hamcrest import equal_to

from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.ec2_fleet_resource_group import EC2FleetResourceGroup
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.exceptions import ResourceGroupError
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from itests.environment import boto_patches
from itests.environment import make_asg
from itests.environment import make_fleet
from itests.environment import make_sfr


def mock_asgs(num, subnet_id):
    asgs = {}
    for i in range(num):
        asg_id = f'fake-asg-{i}'
        make_asg(asg_id, subnet_id)
        asgs[asg_id] = AutoScalingResourceGroup(asg_id)
    return asgs


def mock_sfrs(num, subnet_id):
    sfrgs = {}
    for _ in range(num):
        sfr = make_sfr(subnet_id)
        sfrid = sfr['SpotFleetRequestId']
        sfrgs[sfrid] = SpotFleetResourceGroup(sfrid)
    return sfrgs


def mock_fleets(num, subnet_id):
    fleets = {}
    for _ in range(num):
        fleet = make_fleet(subnet_id)
        fleet_id = fleet['FleetId']
        fleets[fleet_id] = EC2FleetResourceGroup(fleet_id)
    return fleets


@behave.fixture
def mock_agents_and_tasks(context):
    def get_agents(mesos_pool_manager):
        agents = []
        for rg in mesos_pool_manager.resource_groups.values():
            for instance in ec2_describe_instances(instance_ids=rg.instance_ids):
                agents.append({
                    'pid': f'slave(1)@{instance["PrivateIpAddress"]}:1',
                    'id': f'{instance["InstanceId"]}',
                    'hostname': 'host1'
                })
        return agents

    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.agents',
        property(get_agents),
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks',
        mock.PropertyMock(return_value=[]),
    ), staticconf.testing.PatchConfiguration(
        {'mesos_clusters': {'mesos-test': {'max_weight_to_remove': 1000}}},
    ):
        yield


@behave.given('a mesos pool manager with (?P<num>\d+) (?P<rg_type>asg|sfr|fleet) resource groups?')
def make_mesos_pool_manager(context, num, rg_type):
    behave.use_fixture(boto_patches, context)
    behave.use_fixture(mock_agents_and_tasks, context)
    context.rg_type = rg_type
    with mock.patch(
        'clusterman.mesos.util.AutoScalingResourceGroup.load',
        return_value={},
    ) as mock_asg_load, mock.patch(
        'clusterman.mesos.util.SpotFleetResourceGroup.load',
        return_value={},
    ) as mock_sfr_load, mock.patch(
        'clusterman.mesos.util.EC2FleetResourceGroup.load',
        return_value={},
    ) as mock_fleet_load:
        if context.rg_type == 'asg':
            mock_asg_load.return_value = mock_asgs(int(num), context.subnet_id)
        elif context.rg_type == 'sfr':
            mock_sfr_load.return_value = mock_sfrs(int(num), context.subnet_id)
        elif context.rg_type == 'fleet':
            mock_fleet_load.return_value = mock_fleets(int(num), context.subnet_id)
        context.mesos_pool_manager = MesosPoolManager('mesos-test', 'bar')
    context.rg_ids = [i for i in context.mesos_pool_manager.resource_groups]
    context.mesos_pool_manager.max_capacity = 101


@behave.given('the fulfilled capacity of resource group (?P<rg_index>\d+) is (?P<capacity>\d+)')
def external_target_capacity(context, rg_index, capacity):
    rg_index = int(rg_index) - 1
    if context.rg_type == 'asg':
        autoscaling.set_desired_capacity(
            AutoScalingGroupName=f'fake-asg-{rg_index}',
            DesiredCapacity=int(capacity),
            HonorCooldown=True,
        )
    elif context.rg_type == 'sfr':
        ec2.modify_spot_fleet_request(
            SpotFleetRequestId=context.rg_ids[rg_index],
            TargetCapacity=int(capacity),
        )

    # make sure our non orphan fulfilled capacity is up-to-date
    with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._reload_resource_groups'):
        context.mesos_pool_manager.reload_state()


@behave.given('we request (?P<capacity>\d+) capacity')
@behave.when('we request (?P<capacity>\d+) capacity(?P<dry_run> and dry-run is active)?')
def modify_capacity(context, capacity, dry_run=False):
    dry_run = True if dry_run else False
    context.mesos_pool_manager.prune_excess_fulfilled_capacity = mock.Mock()
    context.original_capacities = [rg.target_capacity for rg in context.mesos_pool_manager.resource_groups.values()]
    context.mesos_pool_manager.modify_target_capacity(int(capacity), dry_run=dry_run)


@behave.when('resource group (?P<rgid>\d+) is broken')
def broken_resource_group(context, rgid):
    rg = list(context.mesos_pool_manager.resource_groups.values())[0]
    rg.modify_target_capacity = mock.Mock(side_effect=ResourceGroupError('resource group is broken'))


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
        context.mesos_pool_manager.resource_groups[context.rg_ids[0]].target_capacity,
        equal_to(context.original_capacities[0]),
    )


@behave.then('the(?P<remaining> remaining)? resource groups should have evenly-balanced capacity')
def check_target_capacity(context, remaining):
    target_capacity = 0
    if remaining:
        desired_capacity = (
            (context.mesos_pool_manager.target_capacity - context.original_capacities[0]) / (len(context.rg_ids) - 1)
        )
    else:
        desired_capacity = context.mesos_pool_manager.target_capacity / len(context.rg_ids)

    for i, rg in enumerate(context.mesos_pool_manager.resource_groups.values()):
        target_capacity += rg.target_capacity
        if remaining and i == 0:
            continue
        assert_that(
            rg.target_capacity,
            close_to(desired_capacity, 1.0),
        )
    assert_that(target_capacity, equal_to(context.mesos_pool_manager.target_capacity))
