import arrow
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS

from clusterman.exceptions import MetricsError


def get_average_cpu_util(cluster, role, query_period):
    """Get the average CPU utilization from our cluster over some time period

    :param cluster: the Mesos cluster to check the utilization for
    :param role: the Mesos role on the cluster to check the utilization for
    :param query_period: the period of time (in seconds) to check the utilization
    :returns: the average CPU utilizaton over query_period seconds
    """
    end_time = arrow.now()
    start_time = arrow.now().shift(seconds=-query_period)

    metrics_client = ClustermanMetricsBotoClient()
    metric_name = generate_key_with_dimensions('cpu_allocation', {'cluster': cluster, 'role': role})
    __, cpu_util_history = metrics_client.get_metric_values(
        metric_name,
        SYSTEM_METRICS,
        start_time.timestamp,
        end_time.timestamp,
    )

    if not cpu_util_history:
        raise MetricsError('No data for CPU utilization from {start} to {end}'.format(
            start=start_time,
            end=end_time,
        ))

    return sum([util for __, util in cpu_util_history]) / len(cpu_util_history)
