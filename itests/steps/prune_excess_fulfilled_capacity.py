import behave
import mock
from hamcrest import assert_that
from hamcrest import equal_to
from hamcrest import only_contains

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances


@behave.fixture
def mock_non_orphan_fulfilled_capacity(context):
    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.non_orphan_fulfilled_capacity',
        mock.PropertyMock(return_value=context.nofc),
    ):
        yield


@behave.fixture
def mock_rg_is_stale(context):
    response = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=context.rg_ids)
    for config in response['SpotFleetRequestConfigs']:
        if config['SpotFleetRequestId'] == context.stale_rg_id:
            config['SpotFleetRequestState'] = 'cancelled_running'

    def mock_describe_sfrs(SpotFleetRequestIds):
        return {'SpotFleetRequestConfigs': [
            c
            for c in response['SpotFleetRequestConfigs']
            if c['SpotFleetRequestId'] in SpotFleetRequestIds
        ]}

    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.ec2.describe_spot_fleet_requests',
        side_effect=mock_describe_sfrs,
    ):
        yield


@behave.given('resource group (?P<rg_index>\d+) is stale')
def resource_group_is_stale(context, rg_index):
    context.stale_rg_id = context.rg_ids[int(rg_index) - 1]
    behave.use_fixture(mock_rg_is_stale, context)


@behave.given('we can kill at most (?P<max_tasks_to_kill>\d+) tasks?')
def killed_tasks(context, max_tasks_to_kill):
    context.mesos_pool_manager.max_tasks_to_kill = int(max_tasks_to_kill)


@behave.given('there are no killable instances')
def no_killable_instances(context):
    context.mesos_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[])


@behave.given('the killable instance has weight (?P<weight>\d+)')
def killable_instance_with_weight(context, weight):
    context.mesos_pool_manager.resource_groups[context.rg_ids[0]].market_weight = mock.Mock(return_value=int(weight))
    context.mesos_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[
        context.mesos_pool_manager.get_instance_metadatas()[0],
    ])


@behave.given('the killable instance has (?P<tasks>\d+) tasks')
def killable_instance_with_tasks(context, tasks):
    def get_tasks():
        rg = context.mesos_pool_manager.resource_groups[context.rg_ids[0]]
        instances = ec2_describe_instances(instance_ids=rg.instance_ids[:1])
        return [{'slave_id': instances[0]['InstanceId'], 'state': 'TASK_RUNNING'}] * int(tasks)

    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks',
        mock.PropertyMock(side_effect=get_tasks),
    ):
        context.mesos_pool_manager._count_batch_tasks_per_mesos_agent = mock.Mock(return_value={
            i['id']: 0 for i in context.mesos_pool_manager.agents
        })
        context.mesos_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[
            context.mesos_pool_manager.get_instance_metadatas()[0],
        ])


@behave.given('the non-orphaned fulfilled capacity is (?P<nofc>\d+)')
def set_non_orphaned_fulfilled_capacity(context, nofc):
    context.nofc = int(nofc)
    behave.use_fixture(mock_non_orphan_fulfilled_capacity, context)


@behave.when('we prune excess fulfilled capacity to (?P<target>\d+)')
def prune_excess_fulfilled_capacity(context, target):
    context.original_agents = context.mesos_pool_manager.get_instance_metadatas()
    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.SpotFleetResourceGroup.target_capacity',
        mock.PropertyMock(side_effect=[int(target)] + [0] * (len(context.rg_ids) - 1)),
    ):
        context.mesos_pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=int(target))


@behave.then('(?P<num>\d+) instances? should be killed')
def check_no_instances_killed(context, num):
    assert_that(
        len(context.original_agents) - len(context.mesos_pool_manager.get_instance_metadatas()),
        equal_to(int(num)),
    )


@behave.then('the killed instances are from resource group (?P<rg_index>\d+)')
def check_killed_instance_group(context, rg_index):
    killed_agents = [
        a.group_id
        for a in context.original_agents
        if a.instance_id not in [m.instance_id for m in context.mesos_pool_manager.get_instance_metadatas()]
    ]
    assert_that(
        killed_agents,
        only_contains(context.rg_ids[int(rg_index) - 1]),
    )
