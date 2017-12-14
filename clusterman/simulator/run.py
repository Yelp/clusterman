import json
from datetime import timedelta

import arrow
import staticconf
from clusterman_metrics import ClustermanMetricsSimulationClient
from clusterman_metrics import METADATA

from clusterman.args import add_cluster_arg
from clusterman.args import add_role_arg
from clusterman.args import add_start_end_args
from clusterman.args import subparser
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.reports.report_types import REPORT_TYPES
from clusterman.reports.reports import make_report
from clusterman.simulator.event import AutoscalingEvent
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterCapacityEvent
from clusterman.simulator.metrics import read_metrics_from_compressed_json
from clusterman.simulator.simulated_mesos_role_manager import SimulatedMesosRoleManager
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator
from clusterman.util import get_clusterman_logger
from clusterman.util import parse_time_string


logger = get_clusterman_logger(__name__)


def _make_autoscaler(metadata, simulator, metrics_client, spot_fleet_config):
    with open(spot_fleet_config) as f:
        config = json.load(f)
    role_manager = SimulatedMesosRoleManager(metadata.cluster, metadata.role, [config], simulator)
    role_manager.modify_target_capacity(role_manager.min_capacity)
    for spec in config['LaunchSpecifications']:
        simulator.markets |= {get_instance_market(spec)}
    return Autoscaler(
        metadata.cluster,
        metadata.role,
        role_manager,
        metrics_client,
    )


def _populate_autoscaling_events(simulator, start_time, end_time, period):
    current_time = start_time
    while current_time < end_time:
        simulator.add_event(AutoscalingEvent(current_time))
        current_time += period


def _populate_cluster_capacity_events(metadata, simulator, metrics_client, start_time, end_time):
    __, capacity_ts = metrics_client.get_metric_values(
        f'target_capacity|cluster={metadata.cluster},role={metadata.role}',
        METADATA,
        start_time.timestamp,
        end_time.timestamp,
    )
    for timestamp, data in capacity_ts:
        market_data = {InstanceMarket(*market_str.split(',')): value for market_str, value in data.items()}
        simulator.markets |= set(market_data.keys())
        simulator.add_event(ModifyClusterCapacityEvent(arrow.get(timestamp), market_data))


def _populate_price_changes(simulator, metrics_client, start_time, end_time):
    for market in simulator.markets:
        __, market_prices = metrics_client.get_metric_values(
            f'spot_prices|AZ={market.az},instance_type={market.instance}',
            METADATA,
            start_time.timestamp,
            end_time.timestamp,
        )
        for timestamp, price in market_prices:
            simulator.add_event(InstancePriceChangeEvent(arrow.get(timestamp), {market: float(price)}))


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    metrics = {}
    if args.metrics_data_file:
        try:
            metrics = read_metrics_from_compressed_json(args.metrics_data_file, unix_timestamp=True)
        except OSError as e:
            logger.warn(f'{str(e)}: no metrics loaded')

    region_name = staticconf.read_string(f'mesos_clusters.{args.cluster}.aws_region')
    metrics_client = ClustermanMetricsSimulationClient(metrics, region_name=region_name)

    metadata = SimulationMetadata(args.cluster, args.role)
    simulator = Simulator(metadata, args.start_time, args.end_time)
    autoscaler = (
        _make_autoscaler(metadata, simulator, metrics_client, args.spot_fleet_config)
        if args.use_autoscaler
        else None
    )
    if autoscaler:
        autoscaler_period = timedelta(seconds=autoscaler.period_seconds)
        _populate_autoscaling_events(simulator, args.start_time, args.end_time, autoscaler_period)
    else:
        _populate_cluster_capacity_events()

    _populate_price_changes(simulator, metrics_client, args.start_time, args.end_time)

    simulator.run(autoscaler)

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
    add_role_arg(required_named_args, required=True)
    optional_named_args.add_argument(
        '--use-autoscaler',
        action='store_true',
        help='use the autoscaler in the simulation to adjust the cluster size',
    )
    optional_named_args.add_argument(
        '--spot-fleet-config',
        help='file containing the spot fleet request JSON data',
    )
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
