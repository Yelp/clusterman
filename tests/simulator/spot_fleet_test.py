from collections import defaultdict

import arrow
import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator
from clusterman.simulator.spot_fleet import SpotFleet


MARKETS = [
    InstanceMarket('c3.4xlarge', 'us-west-1a'),
    InstanceMarket('c3.4xlarge', 'us-west-1b'),
    InstanceMarket('i2.8xlarge', 'us-west-2a'),
    InstanceMarket('m4.4xlarge', 'us-west-2b'),
]


@pytest.fixture
def simulator():
    return Simulator(SimulationMetadata('testing', 'test-tag'), arrow.get(0), arrow.get(3600))


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
                'SpotPrice': 1.01,
                'WeightedCapacity': 2,
                'SubnetId': 'us-west-1b',
            },
            {
                'InstanceType': 'i2.8xlarge',
                'SpotPrice': 0.27,
                'WeightedCapacity': 3,
                'SubnetId': 'us-west-2a',
            },
            {
                'InstanceType': 'm4.4xlarge',
                'SpotPrice': 0.42,
                'WeightedCapacity': 0.5,
                'SubnetId': 'us-west-2b',
            },
        ],
    }


@pytest.fixture
def spot_prices():
    instance_price = defaultdict(lambda: PiecewiseConstantFunction())
    instance_price[MARKETS[0]].add_breakpoint(arrow.get(0), 0.5)
    instance_price[MARKETS[1]].add_breakpoint(arrow.get(0), 2.5)
    instance_price[MARKETS[2]].add_breakpoint(arrow.get(0), 0.1)
    instance_price[MARKETS[3]].add_breakpoint(arrow.get(0), 0.5)
    return instance_price


def get_fake_instance_market(spec):
    return InstanceMarket(spec['InstanceType'], spec['SubnetId'])


@pytest.fixture
def spot_fleet(spot_fleet_request_config, simulator, spot_prices):
    with mock.patch('clusterman.simulator.spot_fleet.get_instance_market', side_effect=get_fake_instance_market):
        s = SpotFleet(spot_fleet_request_config, simulator)
    s.simulator.instance_prices = spot_prices
    return s


@pytest.fixture
def test_instances_by_market():
    return {MARKETS[0]: 1, MARKETS[1]: 1, MARKETS[2]: 3, MARKETS[3]: 4}


@pytest.mark.parametrize('residuals,result', [
    # no overflow -- all weights evenly divide residuals
    ([(MARKETS[0], 4), (MARKETS[3], 3)], {MARKETS[0]: 5.0, MARKETS[3]: 6.0}),
    # weight of MARKETS[0] does not divide its residual
    ([(MARKETS[0], 2.5), (MARKETS[3], 3), (MARKETS[1], 3)], {MARKETS[0]: 4.0, MARKETS[1]: 3.0, MARKETS[3]: 6.0}),
    # MARKETS[0] residual is covered by overflow
    ([(MARKETS[2], 7), (MARKETS[1], 5), (MARKETS[0], 1)], {MARKETS[1]: 3.0, MARKETS[2]: 3.0}),
    # MARKETS[0] residual goes negative because of overflow
    ([(MARKETS[1], 9), (MARKETS[2], 7), (MARKETS[0], 1), (MARKETS[3], 3)],
        {MARKETS[1]: 6.0, MARKETS[2]: 3.0, MARKETS[3]: 3.0}),
    # MARKET[0] residual is negative, MARKET[1] residual goes negative because of overflow
    ([(MARKETS[0], -6), (MARKETS[1], 1), (MARKETS[2], 3), (MARKETS[3], 6)],
        {MARKETS[2]: 1.0, MARKETS[3]: 2.0}),
])
def test_get_new_market_counts(residuals, result, spot_fleet):
    spot_fleet.modify_size({MARKETS[0]: 1, MARKETS[1]: 1})
    spot_fleet._find_available_markets = mock.Mock()
    spot_fleet._compute_market_residuals = mock.Mock(return_value=residuals)
    assert spot_fleet._get_new_market_counts(10) == result


def test_compute_market_residuals_new_fleet(spot_fleet, test_instances_by_market):
    target_capacity = 10
    residuals = spot_fleet._compute_market_residuals(target_capacity, test_instances_by_market.keys())
    assert residuals == list(zip(
        sorted(list(test_instances_by_market.keys()),
               key=lambda x: spot_fleet.simulator.instance_prices[x].call(spot_fleet.simulator.current_time)),
        [target_capacity / len(test_instances_by_market)] * len(test_instances_by_market)
    ))


def test_compute_market_residuals_existing_fleet(spot_fleet, test_instances_by_market):
    target_capacity = 20
    spot_fleet.modify_size(test_instances_by_market)
    residuals = spot_fleet._compute_market_residuals(target_capacity, test_instances_by_market.keys())
    assert residuals == [(MARKETS[2], -4), (MARKETS[3], 3), (MARKETS[1], 3), (MARKETS[0], 4)]


def test_total_market_weight(spot_fleet_request_config, spot_fleet, test_instances_by_market):
    spot_fleet.modify_size(test_instances_by_market)
    for i, (market, instance_count) in enumerate(test_instances_by_market.items()):
        assert spot_fleet._total_market_weight(market) == \
            instance_count * spot_fleet_request_config['LaunchSpecifications'][i]['WeightedCapacity']


def test_find_available_markets(spot_fleet):
    available_markets = spot_fleet._find_available_markets()
    assert len(available_markets) == 2
    assert MARKETS[0] in available_markets
    assert MARKETS[2] in available_markets


def test_terminate_instance(spot_fleet, test_instances_by_market):
    # The instance after the split point (inclusive itself) will be terminated
    split_point = 2
    added_instances, __ = spot_fleet.modify_size(test_instances_by_market)
    terminate_instances_ids = (instance.id for instance in added_instances[split_point:])
    spot_fleet.terminate_instances(terminate_instances_ids)
    remain_instances = spot_fleet.instances
    assert len(remain_instances) == split_point
    for instance in added_instances[:split_point]:
        assert instance.id in remain_instances


def test_modify_spot_fleet_request(spot_fleet):
    spot_fleet.modify_spot_fleet_request(10)
    capacity = spot_fleet.target_capacity
    set1 = set(spot_fleet.instances.keys())
    spot_fleet.modify_spot_fleet_request(capacity * 2)
    set2 = set(spot_fleet.instances.keys())
    # Because of FIFO strategy, this should remove all instances in set1
    spot_fleet.modify_spot_fleet_request(spot_fleet.target_capacity - capacity)
    set3 = set(spot_fleet.instances.keys())
    assert set3 == (set2 - set1)


def test_downsize_capacity_by_small_weight(spot_fleet):
    spot_fleet.simulator.current_time.shift(seconds=+100)
    spot_fleet.modify_size({MARKETS[1]: 1, MARKETS[2]: 3})
    spot_fleet.simulator.current_time.shift(seconds=+50)
    spot_fleet.modify_size({MARKETS[0]: 1})
    spot_fleet.target_capacity = 12
    # This should removed the last one instance to meet capacity requirement
    spot_fleet.modify_spot_fleet_request(11)
    assert spot_fleet.target_capacity == 11
    assert spot_fleet.market_size(MARKETS[0]) == 0


@pytest.mark.parametrize('target_capacity', [5, 10, 30, 50, 100])
def test_restore_capacity(spot_fleet, target_capacity):
    spot_fleet.modify_spot_fleet_request(target_capacity)
    # terminate all instances
    spot_fleet.terminate_instances(spot_fleet.instances.keys())
    assert spot_fleet.fulfilled_capacity >= target_capacity
