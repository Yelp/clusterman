import gzip
import json
import random
import time

import yaml

from clusterman.args import subcommand_parser
from clusterman.util import DataPoint
from clusterman.util import DataPointEncoder
from clusterman.util import parse_time_interval_seconds
from clusterman.util import parse_time_string


def get_values_function(values_conf):
    """ Returns a function to generate metric values based on configuration

    There are two modes of operation:
    1. Use functions from the python random library to generate data; the config
       should be a dict in the format

          {'distribution': <function_name>, 'params': <distribution_parameters>}

       where function_name is a function from random, and distribution_parameters
       is the kwargs for the distribution function.
    2. TODO
    """
    try:
        gen_func = getattr(random, values_conf['distribution'])
        return lambda: gen_func(**values_conf['params'])
    except (AttributeError, TypeError):
        # TODO - want to be able to parse a function of existing metric values, rn just constant
        return lambda: int(values_conf)


def get_frequency_function(frequency_conf):
    """ Returns a function to compute the next event time for a metric timeseries, based on configuration

    There are two modes of operation:
    1. Fixed frequency intervals; in this case, the config should be a single string that
       can be parsed by parsedatetime (e.g., 1m, 2h, 3 months, etc).
    2. Randomly generated using functions from the python random library; the config should
       be a dict in the format

           {'distribution': <function_name>, 'params': <distribution_parameters>}

       where function_name is a function from random, and distribution_parameters is the
       kwargs for the distribution function
    """
    if isinstance(frequency_conf, str):
        f = parse_time_interval_seconds(frequency_conf)
        return lambda current_time: current_time.shift(seconds=f)
    else:
        gen_func = getattr(random, frequency_conf['distribution'])
        return lambda current_time: current_time.shift(seconds=int(gen_func(**frequency_conf['params'])))


def load_experimental_design(inputfile):
    """ Generate metric timeseries data from an experimental design .yaml file

    The format of this file should be:
    metric_name:
      start_time: XXXX
      end_time: YYYY
      frequency: <frequency specification>
      values: <values specification>

    This will generate a set of metric values between XXXX and YYYY, with the interarrival
    time between events meeting the frequency specification and the metric values corresponding
    to the values specification

    :returns: a dictionary of metric_name -> timeseries data, that is, a list of (time, value) tuples
    """
    with open(inputfile) as f:
        design = yaml.load(f.read(), Loader=yaml.CLoader)

    metrics = {}
    for metric_name, config in design.items():
        metrics[metric_name] = {'__data__': []}
        start_time = parse_time_string(config['start_time'])
        end_time = parse_time_string(config['end_time'])
        next_time_func = get_frequency_function(config['frequency'])
        values_func = get_values_function(config['values'])

        current_time = start_time
        while current_time < end_time:
            metrics[metric_name]['__data__'].append(DataPoint(current_time, values_func()))
            current_time = next_time_func(current_time)

    return metrics


def write_metrics_data(metrics, outputfile):
    """ Write the generated metric values to a compressed (gzip'd) JSON file """
    with gzip.open(outputfile, 'w') as f:
        f.write(json.dumps(metrics, cls=DataPointEncoder).encode())


def main(args):
    if not args.seed:
        args.seed = int(time.time())

    print(f'Random seed: {args.seed}')
    random.seed(args.seed)

    metrics_data = load_experimental_design(args.input)
    write_metrics_data(metrics_data, args.output)


@subcommand_parser('generate-data', 'generate data for a simulation based on an experimental design', main)
def add_generate_data_parser(gen_data_parser):  # pragma: no coveer
    gen_data_parser.add_argument(
        '-i', '--input',
        required=True,
        help='experimental design .yaml file',
    )
    gen_data_parser.add_argument(
        '-o', '--output',
        default='metrics.json.gz',
        help='output file for generated data',
    )
    gen_data_parser.add_argument(
        '--seed',
        default=None,
        help='seed value for the random number generator',
    )
