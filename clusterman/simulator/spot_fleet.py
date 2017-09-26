from collections import namedtuple
from functools import lru_cache

from clusterman.aws.markets import InstanceMarket
from clusterman.simulator.cluster import Cluster

SpotMarketConfig = namedtuple('SpotMarketConfig', ['bid_price', 'weight'])


class SpotFleet(Cluster):
    """ An implementation of a Cluster designed to model the AWS EC2 Spot Fleet object

    The Spot Fleet object encapsulates a group of spot instances and attempts to maintain a specified capacity of those
    instances, as long as the bid price for the instances does not exceed a user-specified threshold.  If a fleet gets
    outbid in a particular market, the spot fleet will try to replenish the needed capacity in one or more different
    markets.  Users may specify an instance weight for each requested market, which will be used in capacity
    calculations.

    AWS provides two modes for allocating new instances, called the "allocation strategy": lowestPrice, and diversified.
    This model implementation only supports the diversified strategy; moreover, the details on how spot fleets maintain
    capacity with the diversified strategy are sparse, so this implementation provides a naive diversification strategy
    based on the limited documentation provided by AWS.

    Specifically, the diversification strategy implemented here does the following:
    1. Find all available markets (where an available market is defined as one in which the current price is no greater
       than the bid price in that market)
    2. Compute the residual capacity needed to bring each available market up to the same capacity
    3. Starting with the market having the largest residual capacity, assign enouch instances to the available markets
       to "cover" their residual capacity.
       a. Since instance weights may not evenly divide the residual capacity, there may be some overflow in a market.
          Any overflow is subtracted evenly from each of the remaining markets to ensure that we don't allocate too many
          new instances in other markets.
       b. If two markets have the same residual capacity, we fill the market with the cheaper spot price first.
    """

    def __init__(self, config):
        """
        :param config: a configuration dictionary that follows the SFR launch configuration schema.  Not all values
            needed for the SFR config are required here.  Specifically, we require the following elements:
            {
                'LaunchSpecifications': [
                    {
                        'InstanceType': AWS EC2 instance type name,
                        'SubnetId': Subnet the instance should be launched in (should map to a region in common/aws.py),
                        'SpotPrice': How much to bid in this market,
                        'WeightedCapacity': How much to weight instances in this market by when calculating capacity
                    },
                    ...
                ]
            }
        """
        super().__init__()
        self._instance_types = {}
        for spec in config['LaunchSpecifications']:
            bid_price = spec['SpotPrice'] * spec['WeightedCapacity']
            market = InstanceMarket(spec['InstanceType'], spec['SubnetId'])
            self._instance_types[market] = SpotMarketConfig(bid_price, spec['WeightedCapacity'])

        self.target_capacity = 0
        self.allocation_strategy = config['AllocationStrategy']
        if self.allocation_strategy != 'diversified':
            raise NotImplementedError(f'{self.allocation_strategy} not supported')

    def modify_spot_fleet_request(self, target_capacity, spot_prices):
        """ Modify the requested capacity for a particular spot fleet

        :param target_capacity: desired capacity after this operation
        :param spot_prices: dictionary of current spot market prices
        """
        if self.target_capacity > target_capacity:
            raise NotImplementedError('Cannot reduce spot fleet capacity yet')  # TODO (CLUSTERMAN-52)
        else:
            new_market_counts = self._get_new_market_counts(target_capacity, spot_prices)
            added_instances, __ = self.modify_size(new_market_counts)
            for instance in added_instances:
                instance.bid_price = self._instance_types[instance.market].bid_price
            self.target_capacity = target_capacity

    def terminate_instances(self, ids):
        raise NotImplementedError('Cannot terminate instances yet')  # TODO (CLUSTERMAN-52)

    def _get_new_market_counts(self, target_capacity, spot_prices):
        """ Given a target capacity and current spot market prices, find instances to add to achieve the target capacity

        :param target_capacity: the desired total capacity of the fleet
        :param spot_prices: the current spot market prices
        :returns: a dictionary suitable for passing to Cluster.modify_size
        :raises ValueError: if target_capacity is less than the current self.target_capacity
        """
        if target_capacity < self.target_capacity:
            raise ValueError(f'Target capacity {target_capacity} < current capacity {self.target_capacity}')

        available_markets = self._find_available_markets(spot_prices)
        residuals = self._compute_market_residuals(target_capacity, available_markets, spot_prices)

        residual_correction = 0  # If we overflow in one market, correct the residuals in the remaining markets
        new_market_counts = {}

        for i, (market, residual) in enumerate(residuals):
            # We never terminate instances here, so ignore negative residuals after overflow correction
            residual -= residual_correction
            if residual <= 0:
                continue

            weight = self._instance_types[market].weight
            instance_num, remainder = divmod(residual, weight)

            # If the instance weight doesn't evenly divide the residual, add an extra instance (which will
            # cause some overflow in that market)
            if remainder > 0:
                instance_num += 1
                overflow = (instance_num * weight) - residual

                # Evenly divide the overflow among the remaining markets
                remaining_markets = len(residuals) - (i + 1)
                if remaining_markets > 0:
                    residual_correction += overflow / remaining_markets

            if instance_num != 0:
                new_market_counts[market] = instance_num + self.market_size(market)

        return new_market_counts

    def _compute_market_residuals(self, target_capacity, markets, spot_prices):
        """ Given a target capacity and list of available markets, compute the residuals needed to bring all markets up
        to an (approximately) equal capacity such that the total capacity meets or exceeds the target capacity

        :param target_capacity: the desired total capacity of the fleet
        :param markets: a list of available markets
        :param spot_prices: the current spot market prices
        :returns: a list of (market, residual) tuples, sorted first by largest capacity and next by lowest spot price
        """
        target_capacity_per_market = target_capacity / len(markets)

        # Some helper closures for computing residuals and sorting;
        @lru_cache()  # memoize the results
        def residual(market):
            return target_capacity_per_market - self._total_market_weight(market)

        def residual_sort_key(value_tuple):
            market, residual = value_tuple
            return (-residual, spot_prices[market])

        return sorted(
            [(market, residual(market)) for market in markets if residual(market) > 0],
            key=residual_sort_key,
        )

    def _total_market_weight(self, market):
        return self.market_size(market) * self._instance_types[market].weight

    def _find_available_markets(self, spot_prices):
        """
        :param spot_prices: the current spot market prices
        :returns: a list of available spot markets, e.g. markets in the spot fleet request whose bid price is above the
            current market price
        """
        # TODO (CLUSTERMAN-51) need to factor in on-demand prices here
        return [
            market
            for market, config in self._instance_types.items()
            if config.bid_price >= spot_prices[market]
        ]

    @property
    def capacity(self):
        """ The current actual capacity of the spot fleet

        Note that the actual capacity may be greater than the target capacity if instance weights do not evenly divide
        the given target capacity
        """
        return sum(self._total_market_weight(market) for market in self.instance_types)
