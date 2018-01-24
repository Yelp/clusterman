import gzip
import sys

import simplejson as json
from arrow import Arrow
from clusterman_metrics import METRIC_TYPES

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


class TimeSeriesDecoder:
    """ Decode time strings back into Arrow objects """

    def __init__(self, unix_timestamp=False):
        self.unix_timestamp = unix_timestamp

    def __call__(self, obj):
        # If none of the object keys are in METRIC_TYPES, we're probably not at the highest-level
        # object, so return and this function will get called again later.
        if not set(obj) & METRIC_TYPES:
            return obj

        def decode_timestamp(timestamp):
            a = Arrow.strptime(timestamp, FORMAT_STRING)
            return a.timestamp if self.unix_timestamp else a

        for metric_type, metrics in obj.items():
            for metric_name, timeseries in metrics.items():
                obj[metric_type][metric_name] = [
                    (decode_timestamp(timestamp), value)
                    for timestamp, value in timeseries
                ]
        return obj


def read_metrics_from_compressed_json(filename, unix_timestamp=True):
    """ Read a metric timeseries from a gzipped JSON file """
    with gzip.open(filename) as f:
        return json.load(f, object_hook=TimeSeriesDecoder(unix_timestamp))


def write_metrics_to_compressed_json(new_metrics, filename):
    """ Write the generated metric values to a compressed (gzipped) JSON file

    If the file already exists, the new metric values will be appended to the file; if a timeseries
    with a specified metric name already exists in the file, ask the user for confirmation to overwrite.
    Exit to shell if confirmation is denied.

    :param new_metrics: a metric_name -> timeseries dictionary
    :param filename: the file to write to
    """
    try:
        metrics = read_metrics_from_compressed_json(filename, unix_timestamp=False)
    except OSError as e:
        metrics = {}

    for metric_type in new_metrics:
        for metric_name, values in new_metrics[metric_type].items():
            if metric_type not in metrics:
                metrics[metric_type] = {}

            if metric_name in metrics[metric_type]:
                if not ask_for_confirmation(
                    f'This will overwrite existing data for {metric_name}. Do you want to proceed?',
                    default=False,
                ):
                    logger.error('User aborted metrics write operation; exiting.')
                    sys.exit(1)

            metrics[metric_type][metric_name] = values

    with gzip.open(filename, 'w') as f:
        f.write(json.dumps(metrics, cls=TimeSeriesEncoder).encode())
