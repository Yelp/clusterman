import arrow
import pytest

from clusterman.common.aws import InstanceMarket
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def start_time():
    return arrow.get(0)


@pytest.fixture
def end_time(start_time):
    return start_time.shift(hours=2)


@pytest.fixture
def simulator(start_time, end_time):
    return Simulator(start_time, end_time)


@pytest.fixture
def instance_a():
    return {InstanceMarket('c3.8xlarge', 'us-west-2a'): 1}


@pytest.fixture
def instance_b():
    return {InstanceMarket('c3.8xlarge', 'us-west-2b'): 1}


@pytest.fixture
def spot_prices():
    return {
        InstanceMarket('c3.8xlarge', 'us-west-2a'): 1.0,
        InstanceMarket('c3.8xlarge', 'us-west-2b'): 0.5,
    }
