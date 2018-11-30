import mock
import pytest
import staticconf.testing

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.signals import ACK
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup
from tests.conftest import clusterman_pool_config
from tests.conftest import main_clusterman_config
from tests.conftest import mock_aws_client_setup
from tests.mesos.conftest import setup_autoscaling
from tests.mesos.conftest import setup_ec2

pytest.mark.usefixtures(
    mock_aws_client_setup,
    main_clusterman_config,
    clusterman_pool_config,
    setup_ec2,
    setup_autoscaling,
)


@pytest.fixture
def resource_groups():
    rg1 = mock.Mock(spec=SpotFleetResourceGroup, target_capacity=10, fulfilled_capacity=10, is_stale=False)
    rg2 = mock.Mock(spec=SpotFleetResourceGroup, target_capacity=10, fulfilled_capacity=10, is_stale=False)

    class FakeResourceGroupClass(MesosPoolResourceGroup):

        @staticmethod
        def load(cluster, pool, config):
            return {rg1.id: rg1, rg2.id: rg2}

    with mock.patch.dict(
        'clusterman.mesos.mesos_pool_manager.RESOURCE_GROUPS',
        {'sfr': FakeResourceGroupClass},
    ):
        yield


@pytest.fixture
def autoscaler(resource_groups):
    with mock.patch('clusterman.autoscaler.signals.Signal._connect_to_signal_process'), \
            mock.patch('clusterman.autoscaler.autoscaler.yelp_meteorite'), \
            staticconf.testing.PatchConfiguration({'autoscaling': {'default_signal_role': 'bar'}}):

        a = Autoscaler(
            cluster='mesos-test',
            pool='bar',
            apps=['bar'],
            metrics_client=mock.Mock(),
            monitoring_enabled=False,
        )
        a.signal._get_metrics = mock.Mock(return_value={})
        with mock.patch(
            'clusterman.autoscaler.autoscaler.MesosPoolManager.non_orphan_fulfilled_capacity',
            mock.PropertyMock(return_value=20),
        ):
            a.mesos_pool_manager = mock.Mock(wraps=MesosPoolManager)(cluster='mesos-test', pool='bar')

            # two resource groups with target_capacity = 10 and cpus_per_weight = 4 means cpus = 10*2*4 = 80
            resource_totals = {'cpus': 80}
            a.mesos_pool_manager.get_resource_total = mock.Mock(side_effect=resource_totals.__getitem__)
            a.mesos_pool_manager.prune_excess_fulfilled_capacity = mock.Mock()
            yield a


@pytest.mark.parametrize('signal_value', ['56', 'null', '51', '60'])
def test_autoscaler_no_change(autoscaler, signal_value):
    """ The current target capacity is 80 CPUs (setpoint of 0.7 -> 56 CPUs to maintain capacity)

    We ensure that repeated calls with a null resource request or with a constant resource request
    (within the setpoint window) do not change the capacity of the cluster.
    """
    autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": ' + signal_value + '}}'] * 2
    autoscaler.run()
    autoscaler.run()
    for group in autoscaler.mesos_pool_manager.resource_groups.values():
        assert group.modify_target_capacity.call_args_list == \
            [mock.call(10, terminate_excess_capacity=False, dry_run=False)] * 2


def test_autoscaler_scale_up(autoscaler):
    autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 70}}']
    autoscaler.run()
    rgs = list(autoscaler.mesos_pool_manager.resource_groups.values())
    assert rgs[0].modify_target_capacity.call_args == mock.call(13, terminate_excess_capacity=False, dry_run=False)
    assert rgs[1].modify_target_capacity.call_args == mock.call(12, terminate_excess_capacity=False, dry_run=False)


def test_autoscaler_scale_up_big(autoscaler):
    with staticconf.testing.PatchConfiguration({'mesos_clusters': {'mesos-test': {'max_weight_to_add': 10}}}):
        autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 1000}}']
        autoscaler.run()
        rgs = list(autoscaler.mesos_pool_manager.resource_groups.values())
        assert rgs[0].modify_target_capacity.call_args == mock.call(15, terminate_excess_capacity=False, dry_run=False)
        assert rgs[1].modify_target_capacity.call_args == mock.call(15, terminate_excess_capacity=False, dry_run=False)


def test_autoscaler_scale_down(autoscaler):
    autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 42}}']
    autoscaler.run()
    rgs = list(autoscaler.mesos_pool_manager.resource_groups.values())
    assert rgs[0].modify_target_capacity.call_args == mock.call(8, terminate_excess_capacity=False, dry_run=False)
    assert rgs[1].modify_target_capacity.call_args == mock.call(8, terminate_excess_capacity=False, dry_run=False)


def test_autoscaler_scale_down_small(autoscaler):
    autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 2}}']
    autoscaler.run()
    rgs = list(autoscaler.mesos_pool_manager.resource_groups.values())
    assert rgs[0].modify_target_capacity.call_args == mock.call(5, terminate_excess_capacity=False, dry_run=False)
    assert rgs[1].modify_target_capacity.call_args == mock.call(5, terminate_excess_capacity=False, dry_run=False)


def test_autoscaler_resource_total_0_request_0(autoscaler):
    rgs = list(autoscaler.mesos_pool_manager.resource_groups.values())
    rgs[0].target_capacity = 0
    rgs[1].target_capacity = 0
    autoscaler.mesos_pool_manager.get_resource_total = mock.Mock(return_value=0)
    autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 0}}']
    autoscaler.run()
    assert rgs[0].modify_target_capacity.call_args == mock.call(0, terminate_excess_capacity=False, dry_run=False)
    assert rgs[1].modify_target_capacity.call_args == mock.call(0, terminate_excess_capacity=False, dry_run=False)


def test_autoscaler_resource_total_0_request_pos(autoscaler):
    rgs = list(autoscaler.mesos_pool_manager.resource_groups.values())
    rgs[0].target_capacity = 0
    rgs[1].target_capacity = 0
    rgs[0].fulfilled_capacity = 0
    rgs[1].fulfilled_capacity = 0
    autoscaler.mesos_pool_manager.get_resource_total = mock.Mock(return_value=0)
    autoscaler.mesos_pool_manager.non_orphan_fulfilled_capacity = 0
    autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 20}}']
    autoscaler.run()
    assert rgs[0].modify_target_capacity.call_args == mock.call(1, terminate_excess_capacity=False, dry_run=False)
    assert rgs[1].modify_target_capacity.call_args == mock.call(0, terminate_excess_capacity=False, dry_run=False)
