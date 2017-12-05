from collections import namedtuple

import staticconf
from clusterman_signals.base_signal import MetricConfig

from clusterman.exceptions import SignalConfigurationError


SignalConfig = namedtuple('SignalConfig', 'name period_minutes required_metrics custom_parameters')


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
