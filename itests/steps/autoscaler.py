import behave
import mock
import staticconf.testing
from hamcrest import assert_that
from hamcrest import contains

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.signals import ACK
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup
from itests.environment import boto_patches


@behave.fixture
def autoscaler_patches(context):
    behave.use_fixture(boto_patches, context)
    rg1 = mock.Mock(spec=SpotFleetResourceGroup, target_capacity=10, fulfilled_capacity=10, is_stale=False)
    rg2 = mock.Mock(spec=SpotFleetResourceGroup, target_capacity=10, fulfilled_capacity=10, is_stale=False)

    resource_totals = {'cpus': 80}
    with staticconf.testing.PatchConfiguration(
        {'autoscaling': {'default_signal_role': 'bar'}},
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.yelp_meteorite',
    ), mock.patch(
        'clusterman.mesos.util.SpotFleetResourceGroup.load',
        return_value={rg1.id: rg1, rg2.id: rg2},
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager',
        wraps=MesosPoolManager,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager.prune_excess_fulfilled_capacity',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager.get_resource_total',
        side_effect=resource_totals.__getitem__,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.MesosPoolManager.non_orphan_fulfilled_capacity',
        mock.PropertyMock(return_value=20),
    ), mock.patch(
        'clusterman.autoscaler.signals.Signal._connect_to_signal_process',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.Signal._get_metrics',
    ):
        yield


@behave.given('an autoscaler object')
def autoscaler(context):
    behave.use_fixture(autoscaler_patches, context)
    context.autoscaler = Autoscaler(
        cluster='mesos-test',
        pool='bar',
        apps=['bar'],
        metrics_client=mock.Mock(),
        monitoring_enabled=False,
    )


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
    manager.non_orphan_fulfilled_capacity = 0


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
        contains(
            mock.call(int(target), terminate_excess_capacity=False, dry_run=False),
            mock.call(int(target), terminate_excess_capacity=False, dry_run=False),
        ),
    )
