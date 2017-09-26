import gzip
import json
import sys

from arrow import Arrow

from clusterman.util import ask_for_confirmation
from clusterman.util import get_clusterman_logger


logger = get_clusterman_logger(__name__)
FORMAT_STRING = '%Y-%m-%dT%H:%M:%S%z'


def write_metrics_to_datastore(metric_name, data):
    pass


class TimeSeriesEncoder(json.JSONEncoder):
    """ Encode an Arrow object as an ISO-8601 format string (YYYY-MM-DDThh:mm:ss+zz:zz) """

    def default(self, obj):
        if isinstance(obj, Arrow):
            return obj.strftime(FORMAT_STRING)


def TimeSeriesDecoder(obj):
    """ Decode time strings back into Arrow objects """
    dec_obj = {}
    for name, timeseries in obj.items():
        dec_obj[name] = [(Arrow.strptime(timestamp, FORMAT_STRING), float(value)) for timestamp, value in timeseries]
    return dec_obj


def read_metrics_from_compressed_json(filename):
    """ Read a metric timeseries from a gzipped JSON file """
    with gzip.open(filename) as f:
        return json.load(f, object_hook=TimeSeriesDecoder)


def write_metrics_to_compressed_json(new_metrics, filename):
    """ Write the generated metric values to a compressed (gzipped) JSON file

    If the file already exists, the new metric values will be appended to the file; if a timeseries
    with a specified metric name already exists in the file, ask the user for confirmation to overwrite.
    Exit to shell if confirmation is denied.

    :param new_metrics: a metric_name -> timeseries dictionary
    :param filename: the file to write to
    """
    try:
        metrics = read_metrics_from_compressed_json(filename)
    except OSError as e:
        metrics = {}

    for metric_name, values in new_metrics.items():
        if metric_name in metrics:
            if not ask_for_confirmation(
                f'This will overwrite existing data for {metric_name}. Do you want to proceed?',
                default=False,
            ):
                logger.error('User aborted metrics write operation; exiting.')
                sys.exit(1)

        metrics[metric_name] = values

    with gzip.open(filename, 'w') as f:
        f.write(json.dumps(metrics, cls=TimeSeriesEncoder).encode())
