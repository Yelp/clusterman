import argparse
import operator
from collections import defaultdict

import arrow
import staticconf
from clusterman_metrics import ClustermanMetricsSimulationClient
from clusterman_metrics import METADATA

from clusterman.args import add_branch_or_tag_arg
from clusterman.args import add_cluster_arg
from clusterman.args import add_role_arg
from clusterman.args import add_start_end_args
from clusterman.args import subparser
from clusterman.aws.markets import get_market_resources
from clusterman.aws.markets import InstanceMarket
from clusterman.config import setup_config
from clusterman.reports.report_types import REPORT_TYPES
from clusterman.reports.reports import make_report
from clusterman.simulator.event import AutoscalingEvent
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterSizeEvent
from clusterman.simulator.io import read_object_from_compressed_json
from clusterman.simulator.io import write_object_to_compressed_json
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator
from clusterman.util import get_clusterman_logger
from clusterman.util import parse_time_string

logger = get_clusterman_logger(__name__)


def _populate_autoscaling_events(simulator, start_time, end_time):
    current_time = start_time.shift(seconds=simulator.autoscaler.time_to_next_activation(start_time.timestamp))
    while current_time < end_time:
        simulator.add_event(AutoscalingEvent(current_time))
        current_time = current_time.shift(seconds=simulator.autoscaler.time_to_next_activation(current_time.timestamp))


def _populate_cluster_size_events(simulator, start_time, end_time):
    __, capacity_ts = simulator.metrics_client.get_metric_values(
        f'fulfilled_capacity|cluster={simulator.metadata.cluster},role={simulator.metadata.role}',
        METADATA,
        start_time.timestamp,
        end_time.timestamp,
    )
    for timestamp, data in capacity_ts:
        market_data = {}
        for market_str, value in data.items():
            market = InstanceMarket.parse(market_str)
            weight = get_market_resources(market).cpus // staticconf.read_int('autoscaling.cpus_per_weight')
            market_data[market] = int(value) // weight
        simulator.markets |= set(market_data.keys())
        simulator.add_event(ModifyClusterSizeEvent(arrow.get(timestamp), market_data))


def _populate_price_changes(simulator, start_time, end_time, data_start_time, discount):
    for market in simulator.markets:
        __, market_prices = simulator.metrics_client.get_metric_values(
            f'spot_prices|aws_availability_zone={market.az},aws_instance_type={market.instance}',
            METADATA,
            start_time.timestamp,
            end_time.timestamp,
        )
        # TODO (CLUSTERMAN-161) delete this once we've updated all the old data in DynamoDB
        __, old_market_prices = simulator.metrics_client.get_metric_values(
            f'spot_prices|AZ={market.az},instance_type={market.instance}',
            METADATA,
            start_time.timestamp,
            end_time.timestamp,
        )
        market_prices.extend(old_market_prices)
        data_shift = (data_start_time - start_time).total_seconds()
        print(data_shift, start_time, data_start_time)
        for timestamp, price in market_prices:
            price = float(price) * (discount or 1.0)
            simulator.add_event(InstancePriceChangeEvent(
                arrow.get(timestamp).shift(seconds=data_shift),
                {market: price}
            ))


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    sims = []
    if args.compare:
        if len(args.compare) > 2:
            raise argparse.ArgumentError(None, f'Cannot compare more than two simulations: {args.compare}')
        sims = [read_object_from_compressed_json(sim_file) for sim_file in args.compare]

    if len(sims) < 2:
        if args.cluster_config_dir:
            staticconf.DictConfiguration({'cluster_config_directory': args.cluster_config_dir})
        setup_config(args)

        metrics = defaultdict(dict)
        for metrics_file in (args.metrics_data_files or []):
            try:
                data = read_object_from_compressed_json(metrics_file, raw_timestamps=True)
                for metric_type, values in data.items():
                    metrics[metric_type].update(values)
            except OSError as e:
                logger.warn(f'{str(e)}: no metrics loaded')

        region_name = staticconf.read_string('aws.region')
        metrics_client = ClustermanMetricsSimulationClient(metrics, region_name=region_name, app_identifier=args.role)

        metadata = SimulationMetadata(args.name, args.cluster, args.role)
        simulator = Simulator(metadata, args.start_time, args.end_time, args.autoscaler_config, metrics_client)
        if simulator.autoscaler:
            _populate_autoscaling_events(simulator, args.start_time, args.end_time)
        else:
            _populate_cluster_size_events(simulator, args.start_time, args.end_time)

        price_start_time = parse_time_string(args.price_start_time_override) or args.start_time
        price_time_range = (args.end_time - args.start_time).total_seconds()
        price_end_time = price_start_time.shift(seconds=price_time_range)
        _populate_price_changes(simulator, price_start_time, price_end_time, args.start_time, args.discount)

        simulator.run()
        sims.insert(0, simulator)

    if len(sims) == 2:
        cmp_fn = getattr(operator, args.comparison_operator)
        final_simulator = cmp_fn(*sims)
    else:
        final_simulator = sims[0]

    if args.simulation_result_file:
        write_object_to_compressed_json(final_simulator, args.simulation_result_file)

    if args.reports is not None:
        for report in args.reports:
            make_report(report, final_simulator, args.start_time, args.end_time, args.output_prefix)


@subparser('simulate', 'simulate the behavior of a cluster', main)
def add_simulate_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_start_end_args(
        required_named_args,
        'simulation start time',
        'simulation end time',
    )
    add_cluster_arg(required_named_args, required=False)
    add_role_arg(required_named_args, required=False)
    add_branch_or_tag_arg(optional_named_args)
    required_named_args.add_argument(
        '--name',
        default='simulation',
        help='Name for the simulation (helpful when comparing two simulations)',
    )
    optional_named_args.add_argument(
        '--autoscaler-config',
        default=None,
        help='file containing the spot fleet request JSON data for the autoscaler',
    )
    optional_named_args.add_argument(
        '--reports',
        nargs='+',
        choices=REPORT_TYPES.keys(),
        help='type(s) of reports to generate from the simulation',
    )
    optional_named_args.add_argument(
        '--metrics-data-files',
        metavar='filename',
        nargs='+',
        help='provide simulated values for one or more metric time series',
    )
    optional_named_args.add_argument(
        '--cluster-config-dir',
        metavar='directory',
        help='specify role configuration directory for simulation',
    )
    optional_named_args.add_argument(
        '--discount',
        metavar='percent',
        type=float,
        default=None,
        help='optional discount to apply to cost calculations',
    )
    optional_named_args.add_argument(
        '--output-prefix',
        default='',
        help='filename prefix for generated reports',
    )
    optional_named_args.add_argument(
        '--simulation-result-file',
        metavar='filename',
        help='specify filename to save simulation result for comparison',
    )
    optional_named_args.add_argument(
        '--compare',
        metavar='filename',
        nargs='+',
        help='specify one or two filenames to compare simulation result',
    )
    optional_named_args.add_argument(
        '--comparison-operator',
        choices=['add', 'sub', 'mul', 'truediv'],
        default='truediv',
        help='operation to use for comparing simulations; valid choices are binary functions from the operator module',
    )
    optional_named_args.add_argument(
        '--price-start-time-override',
        metavar='timestamp',
        default='-1h',
        help=f'Override start time for spot price time series (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )
