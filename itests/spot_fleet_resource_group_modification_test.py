import mock
import pytest
from staticconf.testing import PatchConfiguration

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from tests.conftest import clusterman_pool_config
from tests.conftest import main_clusterman_config
from tests.conftest import mock_aws_client_setup
from tests.mesos.conftest import setup_ec2
from tests.mesos.spot_fleet_resource_group_test import mock_sfr_response
from tests.mesos.spot_fleet_resource_group_test import mock_spot_fleet_resource_group
from tests.mesos.spot_fleet_resource_group_test import mock_subnet


pytest.mark.usefixtures(mock_aws_client_setup, main_clusterman_config, clusterman_pool_config, setup_ec2)


@pytest.fixture
def mock_sfrs(setup_ec2):
    sfrgs = [mock_spot_fleet_resource_group(mock_sfr_response(mock_subnet())) for i in range(5)]
    for sfrg in sfrgs:
        ec2.modify_spot_fleet_request(SpotFleetRequestId=sfrg.id, TargetCapacity=1)
    return sfrgs


@pytest.fixture
def mock_manager(main_clusterman_config, mock_aws_client_setup, mock_sfrs):
    class FakeResourceGroupClass(MesosPoolResourceGroup):

        @staticmethod
        def load(cluster, pool, config):
            return mock_sfrs

    with mock.patch.dict(
        'clusterman.mesos.mesos_pool_manager.RESOURCE_GROUPS',
        {"sfr": FakeResourceGroupClass},
    ):
        return MesosPoolManager('mesos-test', 'bar')


def test_target_capacity(mock_manager):
    assert mock_manager.target_capacity == 5


@mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager.prune_excess_fulfilled_capacity')
def test_scale_up(mock_prune, mock_manager, mock_sfrs):
    mock_manager.max_capacity = 101

    # Test balanced scale up
    mock_manager.modify_target_capacity(53)
    assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [10, 10, 11, 11, 11]

    # Test dry run -- target and fulfilled capacities should remain the same
    mock_manager.modify_target_capacity(1000, dry_run=True)
    fulfilled_capacity = sorted([rg.fulfilled_capacity for rg in mock_manager.resource_groups])
    assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [10, 10, 11, 11, 11]
    assert sorted([rg.fulfilled_capacity for rg in mock_manager.resource_groups]) == fulfilled_capacity

    # Test balanced scale up after an external modification
    ec2.modify_spot_fleet_request(SpotFleetRequestId=mock_sfrs[0].id, TargetCapacity=13)
    mock_manager.modify_target_capacity(76)
    assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [15, 15, 15, 15, 16]

    # Test an imbalanced scale up
    ec2.modify_spot_fleet_request(SpotFleetRequestId=mock_sfrs[3].id, TargetCapacity=30)
    mock_manager.modify_target_capacity(1000)
    assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [17, 18, 18, 18, 30]

    assert mock_prune.call_count == 0


def test_scale_down(mock_manager, mock_sfrs):
    mock_manager.max_capacity = 101
    mock_manager.modify_target_capacity(1000)
    patched_config = {'mesos_clusters': {'mesos-test': {'max_weight_to_remove': 1000}}}

    # all instances have agents with 0 tasks and are thus killable
    agents = []
    for rg in mock_manager.resource_groups:
        for instance in ec2_describe_instances(instance_ids=rg.instance_ids):
            agents.append({
                'pid': f'slave(1)@{instance["PrivateIpAddress"]}:1', 'id': f'agent-{instance["InstanceId"]}'
            })

    mock_agents = mock.PropertyMock(return_value=agents)
    mock_tasks = mock.PropertyMock(return_value=[])
    with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._agents', mock_agents), \
            mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._tasks', mock_tasks), \
            PatchConfiguration(patched_config):

        # Test a balanced scale down
        mock_manager.modify_target_capacity(80)
        assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [16, 16, 16, 16, 16]

        ec2.modify_spot_fleet_request(SpotFleetRequestId=mock_sfrs[0].id, TargetCapacity=1)

        # Test dry run -- target and fulfilled capacities should remain the same
        mock_manager.modify_target_capacity(22, dry_run=True)
        fulfilled_capacity = sorted([rg.fulfilled_capacity for rg in mock_manager.resource_groups])
        assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [1, 16, 16, 16, 16]
        assert sorted([rg.fulfilled_capacity for rg in mock_manager.resource_groups]) == fulfilled_capacity

        # Test an imbalanced scale down
        mock_manager.modify_target_capacity(22)
        assert sorted([rg.target_capacity for rg in mock_manager.resource_groups]) == [1, 5, 5, 5, 6]
