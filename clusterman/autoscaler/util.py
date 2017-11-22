from collections import namedtuple

import arrow
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS
from clusterman_signals.base_signal import MetricConfig

from clusterman.exceptions import MetricsError
from clusterman.exceptions import SignalConfigurationError


SignalConfig = namedtuple('SignalConfig', 'name period_minutes required_metrics custom_parameters')


def get_average_cpu_util(cluster, role, query_period):
    """Get the average CPU utilization from our cluster over some time period

    :param cluster: the Mesos cluster to check the utilization for
    :param role: the Mesos role on the cluster to check the utilization for
    :param query_period: the period of time (in seconds) to check the utilization
    :returns: the average CPU utilizaton over query_period seconds
    """
    end_time = arrow.now()
    start_time = arrow.now().shift(seconds=-query_period)

    aws_region = staticconf.read_string(f'mesos_clusters.{cluster}.aws_region')
    metrics_client = ClustermanMetricsBotoClient(region_name=aws_region)
    metric_name = generate_key_with_dimensions('cpu_allocation_percent', {'cluster': cluster, 'role': role})
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


def read_signal_config(config_namespace):
    """Validate and return autoscaling signal config from the given namespace.

    If namespace does not contain a signal config, returns None.

    :param config_namespace: namespace to read values from
    :returns SignalConfig
    """
    reader = staticconf.NamespaceReaders(config_namespace)
    name = reader.read_string('autoscale_signal.name', default=None)
    if not name:
        return None
    period_minutes = reader.read_int('autoscale_signal.period_minutes')
    if period_minutes <= 0:
        raise SignalConfigurationError(f'Length of signal period must be positive, got {period_minutes}')
    metrics_dict_list = reader.read_list('autoscale_signal.required_metrics', default=[])
    parameter_dict_list = reader.read_list('autoscale_signal.custom_parameters', default=[])

    parameter_dict = {key: value for param_dict in parameter_dict_list for (key, value) in param_dict.items()}

    required_metric_keys = set(MetricConfig._fields)
    metric_configs = []
    for metrics_dict in metrics_dict_list:
        missing = required_metric_keys - set(metrics_dict.keys())
        if missing:
            raise SignalConfigurationError(f'Missing required metric keys {missing} in {metrics_dict}')
        metric_config = {key: metrics_dict[key] for key in metrics_dict if key in required_metric_keys}
        metric_configs.append(MetricConfig(**metric_config))
    return SignalConfig(name, period_minutes, metric_configs, parameter_dict)
