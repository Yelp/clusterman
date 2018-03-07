import arrow
import mock
import pytest

from clusterman.aws.markets import get_market_resources
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.util import get_total_resource_value
from clusterman.simulator.simulated_aws_cluster import Instance
from clusterman.simulator.simulated_mesos_role_manager import SimulatedMesosRoleManager
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup


TEST_MARKET = InstanceMarket('c3.4xlarge', 'us-west-2a')


@pytest.fixture
def ssfrg_config():
    return {
        'LaunchSpecifications': [],
        'AllocationStrategy': 'diversified'
    }


@pytest.fixture
def mock_ssfrg(ssfrg_config):
    ssfrg = SimulatedSpotFleetResourceGroup(ssfrg_config, None)
    instances = [Instance(TEST_MARKET, arrow.get(0)) for i in range(10)]
    ssfrg.instances = {instance.id: instance for instance in instances}
    return ssfrg


@pytest.fixture
def mock_role_manager(mock_ssfrg, simulator):
    role_manager = SimulatedMesosRoleManager('foo', 'bar', [], simulator)
    role_manager.resource_groups = [mock_ssfrg]
    return role_manager


def test_modify_target_capacity(mock_role_manager):
    with mock.patch('clusterman.simulator.simulated_mesos_role_manager.MesosRoleManager.modify_target_capacity'):
        mock_role_manager.modify_target_capacity(10)
    assert mock_role_manager.simulator.cpus.add_breakpoint.call_args == mock.call(arrow.get(0), 160)


def test_simulated_agents(mock_role_manager):
    assert len(mock_role_manager.agents) == 10
    assert get_total_resource_value(mock_role_manager.agents, 'resources', 'cpus') == \
        10 * get_market_resources(TEST_MARKET).cpus
    assert get_total_resource_value(mock_role_manager.agents, 'resources', 'mem') == \
        10 * get_market_resources(TEST_MARKET).mem
    assert get_total_resource_value(mock_role_manager.agents, 'resources', 'disk') == \
        10 * get_market_resources(TEST_MARKET).disk
    assert list(mock_role_manager._idle_agents_by_market().keys()) == [TEST_MARKET]
    assert len(mock_role_manager._idle_agents_by_market()[TEST_MARKET]) == 10
