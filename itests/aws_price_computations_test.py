import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterSizeEvent
from tests.simulator.conftest import sim_params

pytest.mark.usefixtures(sim_params)


def test_one_instance_constant_price(start_time, simulator, spot_prices, market_a):
    """
    Description: 1 instance is launched and runs for two hours; the spot price for those two hours
        is constant ($1.00/hr).
    Outcome: The cluster costs $2.00 total
    """
    simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}))
    simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
    simulator.run()

    assert simulator.total_cost == pytest.approx(2.0)


def test_one_instance_price_change(start_time, simulator, spot_prices, market_a):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr, but after
        30 minutes it increases to $2.00/hr
    Outcome: The cluster costs $3.00 total
    """
    simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}))
    simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2a')] = 2.0
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=30), spot_prices))
    simulator.run()

    assert simulator.total_cost == pytest.approx(3.0)


def test_two_instances_same_market_same_time(start_time, simulator, spot_prices, market_a):
    """
    Description: 2 instances are launched and run for two hours; the spot price starts at $1.00/hr, but after
        20 minutes it increases to $2.00/hr;
    Outcome: The cluster costs $6.00 total (inst_a: $1.00 + $2.00, inst_b: $1.00 + $2.00)
        (for simplicity and accuracy, the cost after the end of the simulation is ignored)
    """
    simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 2}))
    simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=20), {market_a: 2.0}))
    simulator.run()

    assert simulator.total_cost == pytest.approx(6.0)


def test_two_instances_same_market_different_times(start_time, simulator, spot_prices, market_a):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr, but after
        20 minutes it increases to $2.00/hr; after 30 minutes, a second instance of the same type is launched
        and runs until simulation end
    Outcome: The cluster costs $6.00 total (inst_a: $1.00 + $2.00, inst_b: $2.00 + $2.00 * 0.5)
        (for simplicity and accuracy, the cost after the end of the simulation is ignored)
    """
    simulator.add_event(ModifyClusterSizeEvent(start_time, {market_a: 1}))
    simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=20), {market_a: 2}))
    simulator.add_event(ModifyClusterSizeEvent(start_time.shift(minutes=30), {market_a: 2}))
    simulator.run()

    assert simulator.total_cost == pytest.approx(6.0)


def test_two_instances_different_markets_different_times(start_time, simulator, spot_prices, market_a, market_b):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr, but after
        20 minutes it increases to $2.00/hr; after 30 minutes, a second instance in a different market is launched
        and runs until simulation end; the price for this market starts at $0.50 but increases to $0.75 at 1:15 hrs
    Outcome: The cluster costs $3.875 total (inst_a: $1.00 + $2.00, inst_b: $0.50 + $0.75 * 0.5)
        (for simplicity and accuracy, the cost after the end of the simulation is ignored)
    """
    market_composition = {market_a: 1}
    simulator.add_event(ModifyClusterSizeEvent(start_time, market_composition))
    simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=20), {market_a: 2}))
    market_composition.update({market_b: 1})
    simulator.add_event(ModifyClusterSizeEvent(start_time.shift(minutes=30), market_composition))
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=75), {market_b: 0.75}))
    simulator.run()

    assert simulator.total_cost == pytest.approx(3.875)


def test_remove_instance(start_time, simulator, spot_prices, market_a, market_b):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr but after
        30 minutes it increases to $2.00/hr; after 35 minutes, a second instance in a different market is launched
        and runs for 55 minutes; the price for this market starts at $0.50 but increases to $0.75 at 1:15 hrs
    Outcome: The cluster costs $3.50 total (inst_a: $1.00 + $2.00, inst_b: $0.50)
    """
    market_composition = {market_a: 1}
    simulator.add_event(ModifyClusterSizeEvent(start_time, market_composition))
    simulator.add_event(InstancePriceChangeEvent(start_time, spot_prices))
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=30), {market_a: 2.0}))
    market_composition.update({market_b: 1})
    simulator.add_event(ModifyClusterSizeEvent(start_time.shift(minutes=35), market_composition))
    simulator.add_event(InstancePriceChangeEvent(start_time.shift(minutes=75), {market_b: 0.75}))
    del market_composition[market_b]
    simulator.add_event(ModifyClusterSizeEvent(start_time.shift(minutes=90), market_composition))
    simulator.run()

    assert simulator.total_cost == pytest.approx(3.5)
