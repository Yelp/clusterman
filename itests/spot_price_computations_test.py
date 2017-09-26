from clusterman.common.aws import InstanceMarket
from clusterman.simulator.event import AddClusterCapacityEvent
from clusterman.simulator.event import SpotPriceChangeEvent
from clusterman.simulator.event import TerminateInstancesEvent


def test_one_instance_constant_price(start_time, simulator, instance_a, spot_prices):
    """
    Description: 1 instance is launched and runs for two hours; the spot price for those two hours
        is constant ($1.00/hr).
    Outcome: The cluster costs $2.00 total
    """
    a = AddClusterCapacityEvent(start_time, instance_a)
    print(a.msg)
    simulator.add_event(AddClusterCapacityEvent(start_time, instance_a))
    simulator.add_event(SpotPriceChangeEvent(start_time, spot_prices))
    simulator.run()

    assert simulator.total_cost == 2.0


def test_one_instance_price_change(start_time, simulator, instance_a, spot_prices):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr, but after
        30 minutes it increases to $2.00/hr
    Outcome: The cluster costs $3.00 total
    """
    simulator.add_event(AddClusterCapacityEvent(start_time, instance_a))
    simulator.add_event(SpotPriceChangeEvent(start_time, spot_prices))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2a')] = 2.0
    simulator.add_event(SpotPriceChangeEvent(start_time.shift(minutes=30), spot_prices))
    simulator.run()

    assert simulator.total_cost == 3.0


def test_two_instances_same_market_different_times(start_time, simulator, instance_a, spot_prices):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr, but after
        30 minutes it increases to $2.00/hr; after 35 minutes, a second instance of the same type is launched
        and runs for 1.5 hours
    Outcome: The cluster costs $7.00 total (inst_a: $1.00 + $2.00, inst_b: $2.00 + $2.00)
    """
    simulator.add_event(AddClusterCapacityEvent(start_time, instance_a))
    simulator.add_event(SpotPriceChangeEvent(start_time, spot_prices))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2a')] = 2.0
    simulator.add_event(SpotPriceChangeEvent(start_time.shift(minutes=30), spot_prices))
    simulator.add_event(AddClusterCapacityEvent(start_time.shift(minutes=35), instance_a))
    simulator.run()

    assert simulator.total_cost == 7.0


def test_two_instances_different_markets_different_times(start_time, simulator, instance_a, instance_b, spot_prices):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr, but after
        30 minutes it increases to $2.00/hr; after 35 minutes, a second instance in a different market is launched
        and runs for 1.5 hours; the price for this market starts at $0.50 but increases to $0.75 at 1:15 hrs
    Outcome: The cluster costs $4.25 total (inst_a: $1.00 + $2.00, inst_b: $0.50 + $0.75)
    """
    simulator.add_event(AddClusterCapacityEvent(start_time, instance_a))
    simulator.add_event(SpotPriceChangeEvent(start_time, spot_prices))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2a')] = 2.0
    simulator.add_event(SpotPriceChangeEvent(start_time.shift(minutes=30), spot_prices))
    simulator.add_event(AddClusterCapacityEvent(start_time.shift(minutes=35), instance_b))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2b')] = 0.75
    simulator.add_event(SpotPriceChangeEvent(start_time.shift(minutes=75), spot_prices))
    simulator.run()

    assert simulator.total_cost == 4.25


def test_remove_instance(start_time, simulator, instance_a, instance_b, spot_prices):
    """
    Description: 1 instance is launched and runs for two hours; the spot price starts at $1.00/hr but after
        30 minutes it increases to $2.00/hr; after 35 minutes, a second instance in a different market is launched
        and runs for 55 minutes; the price for this market starts at $0.50 but increases to $0.75 at 1:15 hrs
    Outcome: The cluster costs $3.50 total (inst_a: $1.00 + $2.00, inst_b: $0.50)
    """
    simulator.add_event(AddClusterCapacityEvent(start_time, instance_a))
    simulator.add_event(SpotPriceChangeEvent(start_time, spot_prices))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2a')] = 2.0
    simulator.add_event(SpotPriceChangeEvent(start_time.shift(minutes=30), spot_prices))
    simulator.add_event(AddClusterCapacityEvent(start_time.shift(minutes=35), instance_b))
    spot_prices[InstanceMarket('c3.8xlarge', 'us-west-2b')] = 0.75
    simulator.add_event(SpotPriceChangeEvent(start_time.shift(minutes=75), spot_prices))
    simulator.add_event(TerminateInstancesEvent(start_time.shift(minutes=90), instance_b))
    simulator.run()

    assert simulator.total_cost == 3.5
