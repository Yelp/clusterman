import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.simulator.spot_fleet import SpotFleet


MARKETS = [
    InstanceMarket('c3.4xlarge', 'us-west-1a'),
    InstanceMarket('c3.4xlarge', 'us-west-1b'),
    InstanceMarket('i2.8xlarge', 'us-west-2a'),
    InstanceMarket('m4.4xlarge', 'us-west-2b'),
]


@pytest.fixture(autouse=True)
# To avoid reach AWS endpoint
def fake_markets():
    with mock.patch('clusterman.aws.markets.get_instance_market') as mock_get_instance_market:
        mock_get_instance_market.__contains__.return_value = [
            InstanceMarket('c3.4xlarge', 'us-west-1a'),
            InstanceMarket('m4.4xlarge', 'us-west-1a'),
            InstanceMarket('c3.4xlarge', 'us-west-1b'),
            InstanceMarket('m4.4xlarge', 'us-west-1b'),
            InstanceMarket('i2.8xlarge', 'us-west-2a'),
            InstanceMarket('m4.4xlarge', 'us-west-2a'),
            InstanceMarket('m4.4xlarge', 'us-west-2b'),
        ]
    yield


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
    return {MARKETS[0]: 0.5, MARKETS[1]: 2.5, MARKETS[2]: 0.1, MARKETS[3]: 0.5}


@pytest.fixture
def spot_fleet(spot_fleet_request_config):
    return SpotFleet(spot_fleet_request_config)


@pytest.fixture
def test_instances_by_market():
    return {MARKETS[0]: 1, MARKETS[1]: 1, MARKETS[2]: 3, MARKETS[3]: 4}


@pytest.fixture
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
def test_get_new_market_counts(residuals, result, spot_fleet, spot_prices):
    spot_fleet.modify_size({MARKETS[0]: 1, MARKETS[1]: 1}, 0)
    spot_fleet._find_available_markets = mock.Mock()
    spot_fleet._compute_market_residuals = mock.Mock(return_value=residuals)
    assert spot_fleet._get_new_market_counts(10, spot_prices) == result


@pytest.fixture
def test_compute_market_residuals_new_fleet(spot_fleet, spot_prices, test_instances_by_market):
    target_capacity = 10
    residuals = spot_fleet._compute_market_residuals(target_capacity, test_instances_by_market.keys(), spot_prices)
    assert residuals == list(zip(
        sorted(list(test_instances_by_market.keys()), key=lambda x: spot_prices[x]),
        [target_capacity / len(test_instances_by_market)] * len(test_instances_by_market)
    ))


@pytest.fixture
def test_compute_market_residuals_existing_fleet(spot_fleet, spot_prices, test_instances_by_market):
    target_capacity = 20
    spot_fleet.modify_size(test_instances_by_market, 0)
    residuals = spot_fleet._compute_market_residuals(target_capacity, test_instances_by_market.keys(), spot_prices)
    assert residuals == [(MARKETS[2], -4), (MARKETS[3], 3), (MARKETS[1], 3), (MARKETS[0], 4)]


@pytest.fixture
def test_total_market_weight(spot_fleet_request_config, spot_fleet, test_instances_by_market):
    spot_fleet.modify_size(test_instances_by_market, 0)
    for i, (market, instance_count) in enumerate(test_instances_by_market.items()):
        assert spot_fleet._total_market_weight(market) == \
            instance_count * spot_fleet_request_config['LaunchSpecifications'][i]['WeightedCapacity']


@pytest.fixture
def test_find_available_markets(spot_fleet, spot_prices):
    available_markets = spot_fleet._find_available_markets(spot_prices)
    assert len(available_markets) == 2
    assert MARKETS[0] in available_markets
    assert MARKETS[2] in available_markets
