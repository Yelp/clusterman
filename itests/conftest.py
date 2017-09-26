import arrow
import pytest

from clusterman.common.aws import InstanceMarket
from clusterman.run import setup_logging
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def start_time():
    return arrow.get(0)


@pytest.fixture
def end_time(start_time):
    return start_time.shift(hours=2)


@pytest.fixture
def simulator(start_time, end_time):
    setup_logging()
    return Simulator(SimulationMetadata('Testing', 'test-tag'), start_time, end_time)


@pytest.fixture
def market_a():
    return InstanceMarket('c3.8xlarge', 'us-west-2a')


@pytest.fixture
def market_b():
    return InstanceMarket('c3.8xlarge', 'us-west-2b')


@pytest.fixture
def spot_prices(market_a, market_b):
    return {
        market_a: 1.0,
        market_b: 0.5,
    }
