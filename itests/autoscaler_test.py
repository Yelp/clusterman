import mock
import pytest
import staticconf.testing

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.util import ACK
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup
from tests.conftest import clusterman_role_config
from tests.conftest import main_clusterman_config
from tests.conftest import mock_aws_client_setup
from tests.mesos.conftest import setup_ec2

pytest.mark.usefixtures(mock_aws_client_setup, main_clusterman_config, clusterman_role_config, setup_ec2)


@pytest.fixture
def resource_groups():
    rg1 = mock.Mock(spec=SpotFleetResourceGroup, target_capacity=10, fulfilled_capacity=10)
    rg2 = mock.Mock(spec=SpotFleetResourceGroup, target_capacity=10, fulfilled_capacity=10)
    with mock.patch('clusterman.mesos.mesos_role_manager.load_spot_fleets_from_s3') as mock_load_spot_fleets:
        mock_load_spot_fleets.return_value = [rg1, rg2]
        yield


@pytest.fixture
def autoscaler(resource_groups):
    with mock.patch('clusterman.autoscaler.autoscaler.Autoscaler.load_signal'):
        a = Autoscaler(cluster='mesos-test', role='bar', metrics_client=mock.Mock())
        a.signal_config = mock.Mock()
        a.signal_conn = mock.Mock()
        a._get_metrics = mock.Mock(return_value=[])
        a.mesos_role_manager = mock.Mock(wraps=MesosRoleManager)(cluster='mesos-test', role='bar')
        a.mesos_role_manager.prune_excess_fulfilled_capacity = mock.Mock()
        return a


@pytest.mark.parametrize('signal_value', ['56', 'null', '51', '60'])
def test_autoscaler_no_change(autoscaler, signal_value):
    """ The current target capacity is 80 CPUs (setpoint of 0.7 -> 56 CPUs to maintain capacity

    We ensure that repeated calls with a null resource request or with a constant resource request
    do not change the capacity of the cluster.
    """
    autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": ' + signal_value + '}}'] * 2
    autoscaler.run()
    autoscaler.run()
    for group in autoscaler.mesos_role_manager.resource_groups:
        assert group.modify_target_capacity.call_args_list == [mock.call(10, dry_run=False)] * 2


def test_autoscaler_scale_up(autoscaler):
    autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 70}}']
    autoscaler.run()
    assert autoscaler.mesos_role_manager.resource_groups[0].modify_target_capacity.call_args == \
        mock.call(13, dry_run=False)
    assert autoscaler.mesos_role_manager.resource_groups[1].modify_target_capacity.call_args == \
        mock.call(12, dry_run=False)


def test_autoscaler_scale_up_big(autoscaler):
    with staticconf.testing.PatchConfiguration({'mesos_clusters': {'mesos-test': {'max_weight_to_add': 10}}}):
        autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 1000}}']
        autoscaler.run()
        assert autoscaler.mesos_role_manager.resource_groups[0].modify_target_capacity.call_args == \
            mock.call(15, dry_run=False)
        assert autoscaler.mesos_role_manager.resource_groups[1].modify_target_capacity.call_args == \
            mock.call(15, dry_run=False)


def test_autoscaler_scale_down(autoscaler):
    autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 42}}']
    autoscaler.run()
    assert autoscaler.mesos_role_manager.resource_groups[0].modify_target_capacity.call_args == \
        mock.call(8, dry_run=False)
    assert autoscaler.mesos_role_manager.resource_groups[1].modify_target_capacity.call_args == \
        mock.call(8, dry_run=False)


def test_autoscaler_scale_down_small(autoscaler):
    autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": 2}}']
    autoscaler.run()
    assert autoscaler.mesos_role_manager.resource_groups[0].modify_target_capacity.call_args == \
        mock.call(5, dry_run=False)
    assert autoscaler.mesos_role_manager.resource_groups[1].modify_target_capacity.call_args == \
        mock.call(5, dry_run=False)
