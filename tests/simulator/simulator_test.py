import arrow
import mock
import pytest

from clusterman.simulator.event import Event
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def simulator():
    return Simulator(SimulationMetadata('testing', 'test-tag'), arrow.get(0), arrow.get(60))


@mock.patch('clusterman.simulator.simulator.get_clusterman_logger')
class TestAddEvent:
    def test_add_late_event(self, mock_logger, simulator):
        simulator.add_event(Event(arrow.get(120)))
        assert len(simulator.event_queue) == 2

    def test_add_early_event(self, mock_logger, simulator):
        simulator.current_time = arrow.get(30)
        simulator.add_event(Event(arrow.get(10)))
        assert len(simulator.event_queue) == 2
