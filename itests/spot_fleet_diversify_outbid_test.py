from collections import defaultdict

import arrow
import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup
from tests.simulator.conftest import sim_params

pytest.mark.usefixtures(sim_params)

MARKETS = [
    InstanceMarket('c3.4xlarge', 'us-west-1a'),
    InstanceMarket('c3.4xlarge', 'us-west-1b'),
    InstanceMarket('i2.8xlarge', 'us-west-2a'),
    InstanceMarket('m4.4xlarge', 'us-west-2b'),
    InstanceMarket('r4.2xlarge', 'us-west-1c'),
    InstanceMarket('d2.2xlarge', 'us-west-1c'),
    InstanceMarket('r4.4xlarge', 'us-west-2c'),
    InstanceMarket('d2.4xlarge', 'us-west-2c'),
]


@pytest.fixture
def spot_prices():
    instance_price = defaultdict(lambda: PiecewiseConstantFunction())
    instance_price[MARKETS[0]].add_breakpoint(arrow.get(0), 0.5)
    instance_price[MARKETS[1]].add_breakpoint(arrow.get(0), 0.7)
    instance_price[MARKETS[2]].add_breakpoint(arrow.get(0), 0.6)
    instance_price[MARKETS[3]].add_breakpoint(arrow.get(0), 0.55)
    instance_price[MARKETS[4]].add_breakpoint(arrow.get(0), 0.65)
    instance_price[MARKETS[5]].add_breakpoint(arrow.get(0), 0.75)
    instance_price[MARKETS[6]].add_breakpoint(arrow.get(0), 0.8)
    instance_price[MARKETS[7]].add_breakpoint(arrow.get(0), 0.9)
    return instance_price


def get_fake_instance_market(spec):
    return InstanceMarket(spec['InstanceType'], spec['SubnetId'])


@pytest.fixture
def spot_fleet_request_config():
    return {
        'AllocationStrategy': 'diversified',
        'LaunchSpecifications': [
            {
                'InstanceType': 'c3.4xlarge',
                'SpotPrice': 1.01,
                'WeightedCapacity': 1,
                'SubnetId': 'us-west-1a',
            },
            {
                'InstanceType': 'c3.4xlarge',
                'SpotPrice': 0.41,
                'WeightedCapacity': 2,
                'SubnetId': 'us-west-1b',
            },
            {
                'InstanceType': 'i2.8xlarge',
                'SpotPrice': 0.57,
                'WeightedCapacity': 3,
                'SubnetId': 'us-west-2a',
            },
            {
                'InstanceType': 'm4.4xlarge',
                'SpotPrice': 2.02,
                'WeightedCapacity': 0.5,
                'SubnetId': 'us-west-2b',
            },

            {
                'InstanceType': 'r4.2xlarge',
                'SpotPrice': 1.2,
                'WeightedCapacity': 1,
                'SubnetId': 'us-west-1c',
            },
            {
                'InstanceType': 'd2.2xlarge',
                'SpotPrice': 0.6,
                'WeightedCapacity': 1.5,
                'SubnetId': 'us-west-1c',
            },
            {
                'InstanceType': 'r4.4xlarge',
                'SpotPrice': 0.57,
                'WeightedCapacity': 2,
                'SubnetId': 'us-west-2c',
            },
            {
                'InstanceType': 'd2.4xlarge',
                'SpotPrice': 1.5,
                'WeightedCapacity': 0.8,
                'SubnetId': 'us-west-2c',
            },
        ],
    }


@pytest.fixture
def spot_fleet(spot_fleet_request_config, simulator, spot_prices):
    with mock.patch(
        'clusterman.simulator.simulated_spot_fleet_resource_group.get_instance_market',
        side_effect=get_fake_instance_market,
    ):
        s = SimulatedSpotFleetResourceGroup(spot_fleet_request_config, simulator)
    s.simulator.instance_prices = spot_prices
    return s


@pytest.mark.parametrize('target_capacity', [200, 750, 1500])
def test_spot_fleet_diversifying(target_capacity, spot_fleet, spot_prices):
    threshold = 0.1
    spot_fleet.modify_target_capacity(target_capacity)
    for market in MARKETS:
        assert (
            spot_fleet.market_size(market) * (spot_fleet._instance_types[market].weight) <=
            target_capacity * (1 + threshold) / len(MARKETS)
        )
        assert (
            spot_fleet.market_size(market) * (spot_fleet._instance_types[market].weight) >=
            target_capacity * (1 - threshold) / len(MARKETS)
        )


@pytest.mark.parametrize('target_capacity', [100, 1000])
def test_spot_fleet_refill_capacity(target_capacity, spot_fleet, spot_prices):
    outbid_market = 4
    spot_fleet.modify_target_capacity(target_capacity)
    # Outbid event happens
    spot_prices[MARKETS[outbid_market]].add_breakpoint(arrow.get(300), 3.0)
    spot_fleet.simulator.current_time = arrow.get(480)
    terminate_ids = list(spot_fleet.instance_ids_by_market[MARKETS[outbid_market]])
    spot_fleet.terminate_instances_by_id(terminate_ids)
    assert spot_fleet.market_size(MARKETS[outbid_market]) == 0
    assert spot_fleet.fulfilled_capacity >= spot_fleet.target_capacity


@pytest.mark.parametrize('target_capacity', [10, 100, 1000])
def test_spot_fleet_cost_for_outbid_instances(target_capacity, spot_fleet, spot_prices):
    outbid_market = 4
    # Assuming price of the market except outbid_markets is higher than bid price
    for market_number in range(len(MARKETS)):
        if market_number != outbid_market:
            spot_prices[MARKETS[market_number]].add_breakpoint(arrow.get(300), 3.0)
    # Increasing target_capacity, this should add instances in outbid_market, since it's the only available market
    spot_fleet.simulator.current_time = arrow.get(1200)
    spot_fleet.modify_target_capacity(target_capacity)
    size = spot_fleet.market_size(MARKETS[outbid_market])
    # Outbid event happens in outbid_market, it causes all instances to get evicted
    spot_prices[MARKETS[outbid_market]].add_breakpoint(arrow.get(3000), 3.0)
    spot_fleet.simulator.current_time = arrow.get(3120)
    instances = list(spot_fleet.instances.values())
    terminate_ids = list(spot_fleet.instance_ids_by_market[MARKETS[outbid_market]])
    spot_fleet.terminate_instances_by_id(terminate_ids)
    for instance in instances:
        spot_fleet.simulator._compute_instance_cost(instance)
    # No available market, market size shoud be zero
    assert (spot_fleet.market_size(market) == 0 for market in MARKETS)
    # The cost calculation might change when we have more information from AWS
    expected_cost = size * (3.0 * 120 + spot_prices[MARKETS[outbid_market]].call(arrow.get(1200)) * 1800) / 3600
    assert expected_cost == pytest.approx(expected_cost)


@pytest.mark.parametrize('target_capacity', [100, 500])
def test_negative_residual_scale_up(target_capacity, spot_fleet):
    spot_fleet.modify_size({MARKETS[0]: 100})
    threshold = 0.01
    spot_fleet.modify_target_capacity(target_capacity)
    # Since the residual is negative, it should not launch any new instance in MARKETS[0]
    assert spot_fleet.market_size(MARKETS[0]) == 100
    assert spot_fleet.fulfilled_capacity >= spot_fleet.target_capacity
    # Taking negative residual into account, fulfilled_capacity should be close enough with target_capacity
    assert (spot_fleet.fulfilled_capacity - spot_fleet.target_capacity) < target_capacity * threshold
