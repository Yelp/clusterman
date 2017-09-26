import re

from clusterman.args import subcommand_parser
from clusterman.common.aws import InstanceMarket
from clusterman.simulator.event import SpotPriceChangeEvent
from clusterman.simulator.metrics import load_metrics_from_json
from clusterman.simulator.simulator import Simulator
from clusterman.util import parse_time_string


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    simulator = Simulator(args.start_time, args.end_time)

    metrics = load_metrics_from_json(args.metrics_data_file)
    for metric_name, data in metrics.items():
        m = re.match('spot_prices_(.*)_(.*)', metric_name)
        if m:
            market = InstanceMarket(m[1], m[2])
            for timestamp, price in data:
                simulator.add_event(SpotPriceChangeEvent(timestamp, {market: price}))
    simulator.run()


@subcommand_parser('simulate', 'simulate the behavior of a cluster', main)
def add_simulate_parser(simulate_parser):  # pragma: no cover
    simulate_parser.add_argument(
        '--start-time',
        default='-1h',
        help='time from which to begin the simulation (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )
    simulate_parser.add_argument(
        '--end-time',
        default='now',
        help='time at which to end the simulation (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )
    simulate_parser.add_argument(
        '--metrics-data-file',
        help='provide simulated values for one or more metric time series',
    )
