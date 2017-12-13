import pytest

from clusterman.mesos.util import allocated_cpu_resources
from clusterman.mesos.util import find_largest_capacity_market


@pytest.fixture
def mock_market_capacities():
    return {'market-1': 1000, 'market-2': 5}


def test_allocated_cpu_resources(mock_agents_dict):
    assert allocated_cpu_resources(mock_agents_dict['slaves'][0]) == 0
    assert allocated_cpu_resources(mock_agents_dict['slaves'][1]) == 0
    assert allocated_cpu_resources(mock_agents_dict['slaves'][2]) == 10


def test_find_largest_capacity_market_no_threshold(mock_market_capacities):
    assert find_largest_capacity_market(mock_market_capacities) == ('market-1', 1000)


def test_find_largest_capacity_empty_list(mock_market_capacities):
    assert find_largest_capacity_market({}) == (None, 0)
