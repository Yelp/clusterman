import arrow
import staticconf
from clusterman_metrics import ClustermanMetricsSimulationClient
from clusterman_metrics import METADATA

from clusterman.args import add_cluster_arg
from clusterman.args import add_start_end_args
from clusterman.args import subparser
from clusterman.aws.markets import InstanceMarket
from clusterman.reports.report_types import REPORT_TYPES
from clusterman.reports.reports import make_report
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterCapacityEvent
from clusterman.simulator.metrics import read_metrics_from_compressed_json
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator
from clusterman.util import get_clusterman_logger
from clusterman.util import parse_time_string


logger = get_clusterman_logger(__name__)


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    simulator = Simulator(SimulationMetadata(args.cluster, 'testing'), args.start_time, args.end_time)

    metrics = {}
    if args.metrics_data_file:
        try:
            metrics = read_metrics_from_compressed_json(args.metrics_data_file, unix_timestamp=True)
        except OSError as e:
            logger.warn(f'{str(e)}: no metrics loaded')

    region_name = staticconf.read_string(f'mesos_clusters.{args.cluster}.aws_region')
    metrics_client = ClustermanMetricsSimulationClient(metrics, region_name=region_name)
    markets = set()
    __, capacity = metrics_client.get_metric_values(
        'capacity|region=norcal-prod',
        METADATA,
        args.start_time.timestamp,
        args.end_time.timestamp,
    )
    for timestamp, data in capacity:
        market_data = {InstanceMarket(*market_str.split(',')): value for market_str, value in data.items()}
        markets |= set(market_data)
        simulator.add_event(ModifyClusterCapacityEvent(arrow.get(timestamp), market_data))

    for market in markets:
        __, market_prices = metrics_client.get_metric_values(
            f'spot_price|AZ={market.az},instance_type={market.instance}',
            METADATA,
            args.start_time.timestamp,
            args.end_time.timestamp,
        )
        for timestamp, price in market_prices:
            simulator.add_event(InstancePriceChangeEvent(arrow.get(timestamp), {market: float(price)}))

    simulator.run()

    if args.reports is not None:
        for report in args.reports:
            make_report(report, simulator, args.start_time, args.end_time)
    print('The total cost for the cluster is {cost}'.format(cost=simulator.cost_data().values()[0]))


@subparser('simulate', 'simulate the behavior of a cluster', main)
def add_simulate_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_start_end_args(
        required_named_args,
        'simulation start time',
        'simulation end time',
    )
    add_cluster_arg(required_named_args, required=True)
    optional_named_args.add_argument(
        '--reports',
        nargs='+',
        choices=REPORT_TYPES.keys(),
        help='type(s) of reports to generate from the simulation',
    )
    optional_named_args.add_argument(
        '--metrics-data-file',
        metavar='filename',
        help='provide simulated values for one or more metric time series',
    )
