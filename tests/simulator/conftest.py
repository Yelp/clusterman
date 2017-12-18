import arrow
import pytest

from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def simulator():
    return Simulator(SimulationMetadata('testing', 'test-tag'), arrow.get(0), arrow.get(3600), None, None)
