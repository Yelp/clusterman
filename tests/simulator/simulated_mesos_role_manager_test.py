import arrow
import pytest

from clusterman.aws.markets import get_market_resources
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.util import get_total_resource_value
from clusterman.simulator.cluster import Instance
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


def test_simulated_agents(mock_ssfrg):
    role_manager = SimulatedMesosRoleManager('foo', 'bar', [], None)
    role_manager.resource_groups = [mock_ssfrg]
    assert len(role_manager.agents) == 10
    assert get_total_resource_value(role_manager.agents, 'total_resources', 'cpus') == \
        10 * get_market_resources(TEST_MARKET).cpus
    assert get_total_resource_value(role_manager.agents, 'total_resources', 'mem') == \
        10 * get_market_resources(TEST_MARKET).mem
    assert get_total_resource_value(role_manager.agents, 'total_resources', 'disk') == \
        10 * get_market_resources(TEST_MARKET).disk
    assert list(role_manager._idle_agents_by_market().keys()) == [TEST_MARKET]
    assert len(role_manager._idle_agents_by_market()[TEST_MARKET]) == 10
