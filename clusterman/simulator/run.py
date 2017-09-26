from clusterman.args import subcommand_parser
from clusterman.simulator.simulator import Simulator
from clusterman.util import parse_time_string


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    simulator = Simulator(args.start_time, args.end_time)
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
