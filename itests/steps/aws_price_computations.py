import math
from datetime import timedelta

import arrow
import behave
import staticconf.testing

from clusterman.aws.markets import InstanceMarket
from clusterman.run import setup_logging
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterSizeEvent
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator

_MARKETS = {
    'a': InstanceMarket('c3.8xlarge', 'us-west-2a'),
    'b': InstanceMarket('c3.8xlarge', 'us-west-2b'),
    'c': InstanceMarket('c3.8xlarge', 'us-west-2c'),
}


@behave.given('market (?P<market_id>[a-cA-C]) has (?P<count>\d+) instances? at time (?P<time>\d+)')
def setup_instance(context, market_id, count, time):
    if not hasattr(context, 'market_counts'):
        context.market_counts = [(0, {})]
    last_time = context.market_counts[-1][0]

    if int(time) != last_time:
        context.market_counts.append((time, dict(context.market_counts[-1][1])))
    context.market_counts[-1][1].update({_MARKETS[market_id.lower()]: int(count)})


@behave.given('market (?P<market_id>[a-cA-C]) costs \$(?P<cost>\d+(?:\.\d+)?)/hour at time (?P<time>\d+)')
def setup_cost(context, market_id, cost, time):
    if not hasattr(context, 'markets'):
        context.markets = {}
    context.markets.setdefault(market_id.lower(), []).append((int(time), float(cost)))


@behave.when('the simulator runs for (?P<hours>\d+) hours(?P<per_second_billing> and billing is per-second)?')
def run_simulator(context, hours, per_second_billing):
    billing_frequency = timedelta(seconds=1) if per_second_billing else timedelta(hours=1)
    refund_outbid = not per_second_billing
    setup_logging()
    context.simulator = Simulator(
        SimulationMetadata('test', 'Testing', 'test-tag'),
        start_time=arrow.get(0),
        end_time=arrow.get(int(hours) * 3600),
        autoscaler_config_file=None,
        metrics_client=None,
        billing_frequency=billing_frequency,
        refund_outbid=refund_outbid,
    )
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': 0,
        'join_delay_stdev_seconds': 0,
    }):
        for join_time, market_counts in context.market_counts:
            context.simulator.add_event(ModifyClusterSizeEvent(arrow.get(join_time), market_counts))
        for market_id, prices in context.markets.items():
            for time, cost in sorted(prices):
                market = _MARKETS[market_id.lower()]
                context.simulator.add_event(InstancePriceChangeEvent(arrow.get(time), {market: cost}))
        context.simulator.run()


@behave.then('the simulated cluster costs \$(?P<cost>\d+(?:\.\d+)?) total')
def check_cost(context, cost):
    print(context.simulator.total_cost)
    assert math.isclose(context.simulator.total_cost, float(cost), abs_tol=0.01)
