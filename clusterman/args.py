import argparse
import logging
import sys

from clusterman import __version__
from clusterman.util import get_clusterman_logger


def subparser(command, help, entrypoint):  # pragma: no cover
    """ Function decorator to simplify adding arguments to subcommands

    :param command: name of the subcommand to add
    :param help: help string for the subcommand
    :param entrypoint: the 'main' function for the subcommand to execute
    """
    def decorator(add_args):
        def wrapper(subparser):
            subparser = subparser.add_parser(command, formatter_class=help_formatter, add_help=False)
            required_named_args = subparser.add_argument_group('required arguments')
            optional_named_args = subparser.add_argument_group('optional arguments')
            add_args(subparser, required_named_args, optional_named_args)
            optional_named_args.add_argument('-h', '--help', action='help', help='show this message and exit')
            subparser.set_defaults(entrypoint=entrypoint)
        return wrapper
    return decorator


def add_start_end_args(parser, start_help, end_help):  # pragma: no cover
    """ Add --start-time and --end-time args to a parser

    :param start_help: help string for --start-time
    :param end_help: help string for --end-time
    """
    parser.add_argument(
        '--start-time',
        metavar='timestamp',
        default='-1h',
        help=f'{start_help} (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )
    parser.add_argument(
        '--end-time',
        metavar='timestamp',
        default='now',
        help=f'{end_help} (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )


def add_region_arg(parser):  # pragma: no cover
    """ Add an --aws-region argument to a parser """
    parser.add_argument(
        '--aws-region',
        help='AWS region to operate in',
    )


def add_env_config_path_arg(parser):  # pragma: no cover
    """ Add a --env-config-path argument to a parser """
    parser.add_argument(
        '--env-config-path',
        default='/nail/srv/configs/clusterman.yaml',
        help='Path to clusterman configuration file',
    )


def help_formatter(prog):  # pragma: no cover
    """Formatter for the argument parser help strings"""
    return argparse.ArgumentDefaultsHelpFormatter(prog, max_help_position=35, width=100)


def _get_validated_args(parser):
    args = parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    logger = get_clusterman_logger(__name__)

    if args.subcommand is None:
        logger.error('missing subcommand')
        parser.print_help()
        sys.exit(1)

    # Every subcommand must specify an entry point, accessed here by args.entrypoint
    # (protip) use the subparser decorator to set this up for you
    if not hasattr(args, 'entrypoint'):
        logger.critical(f'error: missing entrypoint for {args.subcommand}')
        sys.exit(1)

    return args


def parse_args(description):  # pragma: no cover
    """Set up parser for the CLI tool and any subcommands

    :param description: a string descripting the tool
    :returns: a namedtuple of the parsed command-line options with their values
    """
    from clusterman.mesos.manage import add_mesos_manager_parser
    from clusterman.mesos.status import add_mesos_status_parser
    from clusterman.simulator.run import add_simulate_parser
    from clusterman.tools.generate_data import add_generate_data_parser

    root_parser = argparse.ArgumentParser(description=description, formatter_class=help_formatter)
    add_region_arg(root_parser)
    add_env_config_path_arg(root_parser)
    root_parser.add_argument(
        '--log-level',
        default='warning',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
    )
    root_parser.add_argument(
        '--private-aws-config',
        metavar='filename',
        default='.aws-private.yaml',
        help='YAML file for private AWS config values',
    )
    root_parser.add_argument(
        '-v', '--version',
        action='version',
        version='clusterman ' + __version__
    )

    subparser = root_parser.add_subparsers(help='accepted commands')
    subparser.dest = 'subcommand'

    add_generate_data_parser(subparser)
    add_mesos_manager_parser(subparser)
    add_simulate_parser(subparser)
    add_mesos_status_parser(subparser)

    args = _get_validated_args(root_parser)
    return args
