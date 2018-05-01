import pytest
import staticconf.testing

from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterSizeEvent


@pytest.mark.parametrize('join_time_seconds', [0, 300])
def test_join_delay(start_time, simulator, spot_prices, market_a, join_time_seconds):
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': join_time_seconds,
        'join_delay_stdev_seconds': 0,
    }):
        simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}))
        simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
        simulator.run()

    assert simulator.total_cost == pytest.approx(2.0)
    instance = list(simulator.aws_clusters[0].instances.values())[0]
    assert instance.start_time == start_time
    assert instance.join_time == start_time.shift(seconds=join_time_seconds)


def test_join_delay_override(start_time, simulator, spot_prices, market_a):
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': 300,
        'join_delay_stdev_seconds': 0,
    }):
        simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}, use_join_delay=False))
        simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
        simulator.run()

    assert simulator.total_cost == pytest.approx(2.0)
    instance = list(simulator.aws_clusters[0].instances.values())[0]
    assert instance.start_time == start_time
    assert instance.join_time == start_time


def test_instance_ends_before_joining(start_time, simulator, spot_prices, market_a):
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': 300,
        'join_delay_stdev_seconds': 0,
    }):
        simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}))
        simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
        simulator.add_event(ModifyClusterSizeEvent(start_time.shift(minutes=2), {market_a: 0}))
        simulator.run()

    assert simulator.total_cost == pytest.approx(1.0)
    for x, y in simulator.mesos_cpus.breakpoints.items():
        assert y == 0


def test_instance_ends_after_joining(start_time, simulator, spot_prices, market_a):
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': 300,
        'join_delay_stdev_seconds': 0,
    }):
        simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}))
        simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
        simulator.add_event(ModifyClusterSizeEvent(start_time.shift(minutes=30), {market_a: 0}))
        simulator.run()

    assert simulator.total_cost == pytest.approx(1.0)
    assert list(simulator.mesos_cpus.breakpoints.items()) == [
        (start_time.shift(minutes=5), 32),
        (start_time.shift(minutes=30), 0),
    ]
