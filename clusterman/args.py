import argparse
import logging
import sys

from pkg_resources import get_distribution

from clusterman.util import get_clusterman_logger


def subcommand_parser(command, help, entrypoint):  # pragma: no cover
    """ Function decorator to simplify adding arguments to subcommands

    :param command: name of the subcommand to add
    :param help: help string for the subcommand
    :param entrypoint: the 'main' function for the subcommand to execute
    """
    def decorator(add_args):
        def wrapper(subparser):
            subcommand_parser = subparser.add_parser(command, help=help, formatter_class=help_formatter)
            add_args(subcommand_parser)
            subcommand_parser.set_defaults(entrypoint=entrypoint)
        return wrapper
    return decorator


def help_formatter(prog):  # pragma: no cover
    """Formatter for the argument parser help strings"""
    return argparse.ArgumentDefaultsHelpFormatter(prog, max_help_position=30, width=100)


def parse_args(description):  # pragma: no cover
    """Set up parser for the CLI tool and any subcommands

    :param description: a string descripting the tool
    :returns: a namedtuple of the parsed command-line options with their values
    """
    from clusterman.simulator.run import add_simulate_parser
    from clusterman.tools.generate_data import add_generate_data_parser

    root_parser = argparse.ArgumentParser(description=description, formatter_class=help_formatter)
    root_parser.add_argument(
        '--log-level',
        default='warning',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
    )
    root_parser.add_argument(
        '-v', '--version',
        action='version',
        version='clusterman ' + get_distribution('clusterman').version,
    )

    subparser = root_parser.add_subparsers(help='accepted commands')
    subparser.dest = 'subcommand'

    add_simulate_parser(subparser)
    add_generate_data_parser(subparser)

    args = root_parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    logger = get_clusterman_logger(__name__)

    if args.subcommand is None:
        logger.error('missing subcommand')
        root_parser.print_help()
        sys.exit(1)

    # Every subcommand must specify an entry point, accessed here by args.entrypoint
    # (protip) use the subcommand_parser decorator to set this up for you
    if not hasattr(args, 'entrypoint'):
        logger.critical(f'error: missing entrypoint for {args.subcommand}')
        sys.exit(1)

    return args
