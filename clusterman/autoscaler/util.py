import time

import arrow
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS

from clusterman.exceptions import MetricsError


def get_average_cpu_util(cluster, role, query_period):
    """Get the average CPU utilization from our cluster over some time period

    :param cluster_role: the Mesos cluster role to check the utilization for
    :param query_period: the period of time (in seconds) to check the utilization
    :returns: the average CPU utilizaton over query_period seconds
    """
    now = time.time()
    metrics_client = ClustermanMetricsBotoClient()
    metric_name = generate_key_with_dimensions('cpu_allocation', {'cluster': cluster, 'role': role})
    cpu_util_history = metrics_client.get_metric_values(metric_name, SYSTEM_METRICS, now - query_period, now)

    if not cpu_util_history:
        raise MetricsError('No data for CPU utilization from {start} to {end}'.format(
            start=arrow.get(now - query_period),
            end=arrow.get(now),
        ))

    return sum([util for __, util in cpu_util_history]) / len(cpu_util_history)
