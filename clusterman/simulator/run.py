import re

from clusterman.args import add_start_end_args
from clusterman.args import subparser
from clusterman.common.aws import InstanceMarket
from clusterman.simulator.event import ModifyClusterCapacityEvent
from clusterman.simulator.event import SpotPriceChangeEvent
from clusterman.simulator.metrics import read_metrics_from_compressed_json
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator
from clusterman.util import get_clusterman_logger
from clusterman.util import parse_time_string


logger = get_clusterman_logger(__name__)


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    simulator = Simulator(SimulationMetadata('Seagull', 'testing'), args.start_time, args.end_time)

    metrics = {}
    if args.metrics_data_file:
        try:
            metrics = read_metrics_from_compressed_json(args.metrics_data_file)
        except OSError as e:
            logger.warn(f'{str(e)}: no metrics loaded')

    for metric_name, data in metrics.items():
        m = re.match('.*(spot_prices|capacity)_(.*)_(.*)', metric_name)
        if m:
            metric_type = m[1]
            market = InstanceMarket(m[2], m[3])
            if metric_type == 'spot_prices':
                for timestamp, price in data:
                    simulator.add_event(SpotPriceChangeEvent(timestamp, {market: price}))
            elif metric_type == 'capacity':
                for timestamp, value in data:
                    simulator.add_event(ModifyClusterCapacityEvent(timestamp, {market: value}))
    simulator.run()
    simulator.make_report()
    print('The total cost for the cluster is {cost}'.format(cost=simulator.cost()))


@subparser('simulate', 'simulate the behavior of a cluster', main)
def add_simulate_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_start_end_args(
        required_named_args,
        'simulation start time',
        'simulation end time',
    )
    optional_named_args.add_argument(
        '--metrics-data-file',
        metavar='filename',
        help='provide simulated values for one or more metric time series',
    )
