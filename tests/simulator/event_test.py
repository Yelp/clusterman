import arrow
import mock
import pytest

from clusterman.common.aws import InstanceMarket
from clusterman.exceptions import SimulationError
from clusterman.simulator.cluster import Instance
from clusterman.simulator.event import ComputeClusterCostEvent
from clusterman.simulator.event import ModifyClusterCapacityEvent


@pytest.fixture
def mock_instance():
    market = InstanceMarket('m4.4xlarge', 'us-west-2a')
    instance = Instance(market, arrow.get(0), 0.1)
    instance.last_price = 32
    return instance


@pytest.fixture
def mock_simulator(mock_instance):
    simulator = mock.Mock()
    simulator.cluster.all_instances = {mock_instance.id: mock_instance}
    simulator.spot_prices = {mock_instance.market: 1}
    return simulator


def test_modify_cluster_capacity_aws_terminated(mock_instance, mock_simulator):
    mock_simulator.cluster.modify_capacity.return_value = [], [mock_instance.id]
    mock_simulator.cluster.cpu = 1234

    evt = ModifyClusterCapacityEvent(arrow.get(3600), {mock_instance.market: 4})
    evt.handle(mock_simulator)

    assert mock_instance.last_price == 0
    assert mock_simulator.cost_per_hour.modify_value.call_args[0] == (mock_instance.bill_time, -32)


def test_compute_cluster_cost_invalid_time(mock_instance, mock_simulator):
    evt = ComputeClusterCostEvent(mock_instance.launch_time.shift(minutes=1), [mock_instance.id])
    with pytest.raises(SimulationError):
        evt.handle(mock_simulator)


def test_compute_cluster_cost(mock_instance, mock_simulator):
    evt = ComputeClusterCostEvent(mock_instance.launch_time.shift(hours=1), [mock_instance.id])
    evt.handle(mock_simulator)

    assert mock_instance.last_price == 1
    assert mock_simulator.cost_per_hour.modify_value.call_args[0] == (mock_instance.bill_time, -31)
    assert isinstance(mock_simulator.add_event.call_args[0][0], ComputeClusterCostEvent)
    assert mock_simulator.add_event.call_args[0][0].time == mock_instance.launch_time.shift(hours=2)
    assert mock_simulator.add_event.call_args[0][0].instance_ids == [mock_instance.id]


def test_compute_cluster_cost_inactive(mock_instance, mock_simulator):
    mock_instance.active = False
    evt = ComputeClusterCostEvent(mock_instance.launch_time.shift(hours=1), [mock_instance.id])
    evt.handle(mock_simulator)

    assert mock_simulator.cost_per_hour.modify_value.call_args[0] == (evt.time, -32)
    assert mock_simulator.add_event.call_count == 0
    assert mock_simulator.cluster.prune_instances.call_args[0][0] == [mock_instance.id]
