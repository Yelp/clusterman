import gzip
import json

from clusterman.util import timeseries_decoder


def write_metrics_to_datastore(metric_name, data):
    pass


def load_metrics_from_json(filename):
    with gzip.open(filename) as f:
        metrics_data = json.load(f, object_hook=timeseries_decoder)

    for metric_name, data in metrics_data.items():
        write_metrics_to_datastore(metric_name, data)

    # TODO eventually don't return here and load the appropriate metrics from the datastore?
    return metrics_data
