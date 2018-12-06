import behave
import mock
import staticconf.testing
from hamcrest import assert_that
from hamcrest import equal_to
from moto import mock_ec2

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.signals import ACK
from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.spot_fleet_resource_group import MesosPoolResourceGroup
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup


def _run_instances():
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    subnet = ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
        AvailabilityZone='us-west-2a'
    )
    instance_response = ec2.run_instances(
        InstanceType='m5.large',
        MinCount=20,
        MaxCount=20,
        SubnetId=subnet['Subnet']['SubnetId'],
    )
    return [i['InstanceId'] for i in instance_response['Instances']]


def _make_resource_group(instance_ids):
    return mock.Mock(
        spec=SpotFleetResourceGroup,
        target_capacity=10,
        fulfilled_capacity=10,
        is_stale=False,
        instance_ids=instance_ids,
        market_weight=mock.Mock(return_value=1),
    )


@behave.fixture
def autoscaler_setup(context):
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    instance_ids = _run_instances()
    rg1 = _make_resource_group(instance_ids[:10])
    rg2 = _make_resource_group(instance_ids[10:])

    class FakeResourceGroupClass(MesosPoolResourceGroup):
        @staticmethod
        def load(cluster, pool, config):
            return {rg1.id: rg1, rg2.id: rg2}

    resource_total = {'cpus': 80}
    with staticconf.testing.PatchConfiguration(
        {'autoscaling': {'default_signal_role': 'bar'}},
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.yelp_meteorite',
    ), mock.patch.dict(
        'clusterman.mesos.mesos_pool_manager.RESOURCE_GROUPS',
        {'sfr': FakeResourceGroupClass},
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager',
        wraps=MesosPoolManager,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager.prune_excess_fulfilled_capacity',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager._count_tasks_per_mesos_agent',
        return_value={i: 0 for i in instance_ids},
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager._count_batch_tasks_per_mesos_agent',
        return_value={i: 0 for i in instance_ids},
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager.get_resource_total',
        side_effect=resource_total.__getitem__,
    ), mock.patch(
        'clusterman.autoscaler.signals.Signal._connect_to_signal_process',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.Signal._get_metrics',
    ):
        yield
    mock_ec2_obj.stop()


@behave.given('an autoscaler object')
def autoscaler(context):
    behave.use_fixture(autoscaler_setup, context)
    context.autoscaler = Autoscaler(
        cluster='mesos-test',
        pool='bar',
        apps=['bar'],
        metrics_client=mock.Mock(),
        monitoring_enabled=False,
    )
    context.autoscaler.mesos_pool_manager.agents = []
    for rg in context.autoscaler.mesos_pool_manager.resource_groups.values():
        for inst in ec2_describe_instances(instance_ids=rg.instance_ids):
            context.autoscaler.mesos_pool_manager.agents.append({
                'pid': f'slave(1)@{inst["PrivateIpAddress"]}:1',
                'id': f'{inst["InstanceId"]}',
                'hostname': 'host1',
                'used_resources': {'cpus': 0, 'mem': 0, 'disk': 0},
                'resources': {'cpus': 10, 'mem': 10, 'disk': 10},
            })


@behave.when('the pool is empty')
def empty_pool(context):
    manager = context.autoscaler.mesos_pool_manager
    groups = list(manager.resource_groups.values())
    groups[0].target_capacity = 0
    groups[1].target_capacity = 0
    groups[0].fulfilled_capacity = 0
    groups[1].fulfilled_capacity = 0
    manager.min_capacity = 0
    manager.get_resource_total = mock.Mock(return_value=0)


@behave.when('the signal resource request is (?P<value>\d+ cpus|empty)')
def signal_resource_request(context, value):
    if value == 'empty':
        resources = '{}' if value == 'empty' else '{'
    else:
        n, t = value.split(' ')
        resources = '{"' + t + '":' + n + '}'
    context.autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": ' + resources + '}'] * 2
    context.autoscaler.run()
    context.autoscaler.run()


@behave.then('the autoscaler should scale rg(?P<rg>[12]) to (?P<target>\d+) capacity')
def rg_capacity_change(context, rg, target):
    groups = list(context.autoscaler.mesos_pool_manager.resource_groups.values())
    assert_that(
        groups[int(rg) - 1].modify_target_capacity.call_args_list,
        equal_to([mock.call(int(target), terminate_excess_capacity=False, dry_run=False)] * 2),
    )


@behave.then('the log should contain (?P<message>.*)')
def check_log(context, message):
    assert_that(context.log_capture.find_event(message))
