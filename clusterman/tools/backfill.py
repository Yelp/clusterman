from clusterman.args import add_start_end_args
from clusterman.args import subparser
from clusterman.common.sfx import basic_sfx_query
from clusterman.simulator.metrics import write_metrics_to_compressed_json
from clusterman.util import parse_time_string


def _parse_extra_options(opt_array):
    """ Convert any options that can't be parsed by argparse into a kwargs dict; if an option is specified
    multiple times, it will appear as a list in the results

    :param opt_array: a list of "option=value" strings
    :returns: a dict mapping from options -> values
    """
    kwargs = {}
    for opt_string in opt_array:
        opt, val = [s.strip() for s in opt_string.split('=')]
        if opt in kwargs:
            if not isinstance(kwargs[opt], list):
                kwargs[opt] = [kwargs[opt]]
            kwargs[opt].append(val)
        else:
            kwargs[opt] = val
    return kwargs


def main(args):
    kwargs = _parse_extra_options(args.option)
    start_time = parse_time_string(args.start_time)
    end_time = parse_time_string(args.end_time)
    args.dest_metric_names = args.dest_metric_names or args.src_metric_names

    if len(args.src_metric_names) != len(args.dest_metric_names):
        raise ValueError(
            'Different number of source and destination metrics\n'
            f'src = {args.src_metric_names}, dest = {args.dest_metric_names}'
        )

    values = {dest: list() for dest in args.dest_metric_names}
    if args.src == 'signalfx':
        api_token = kwargs.pop('api_token')
        filters = [s.split(':') for s in kwargs.pop('filter', '')]
        for src, dest in zip(args.src_metric_names, args.dest_metric_names):
            values[dest] = basic_sfx_query(
                api_token,
                src,
                start_time,
                end_time,
                filters=filters,
                **kwargs,
            )
        print(values)
    else:
        raise NotImplementedError('Other backfill sources not yet supported')

    if args.dest_file:
        write_metrics_to_compressed_json(values, args.dest_file)


@subparser('backfill', 'backfill data into the Clusterman metrics datastore', main)
def add_backfill_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    required_named_args.add_argument(
        '--src',
        required=True,
        choices=['aws', 'elasticsearch', 'signalfx'],
        help='source of data to backfill',
    )
    required_named_args.add_argument(
        '--src-metric-names',
        nargs='+',
        metavar='metric-name',
        required=True,
        help='list of source metric names to backfill from',
    )
    add_start_end_args(
        required_named_args,
        'initial time for backfilled datapoints',
        'final time for backfilled datapoints',
    )

    optional_named_args.add_argument(
        '--dest-file',
        metavar='filename',
        help='save the backfilled data to a file in compressed JSON format',
    )
    optional_named_args.add_argument(
        '--dest-metric-names',
        nargs='*',
        metavar='metric-name',
        default=None,
        help='list of destination metric names to backfill to (if None, same as --src-metric-names)',
    )
    optional_named_args.add_argument(
        '-o', '--option',
        nargs='*',
        help=('additional options to be passed into the backfill source, as "opt=value" strings'),
    )
