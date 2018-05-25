import re
from collections import namedtuple

import staticconf
import yaml
from clusterman_metrics import APP_METRICS
from staticconf.errors import ConfigurationError

from clusterman.aws.client import s3
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalValidationError
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
AutoscalingConfig = namedtuple(
    'AutoscalingConfig',
    ['setpoint', 'setpoint_margin', 'cpus_per_weight'],
)
MetricConfig = namedtuple('MetricConfig', ['name', 'type', 'minute_range'])
SignalConfig = namedtuple(
    'SignalConfig',
    ['name', 'repo', 'branch_or_tag', 'period_minutes', 'required_metrics', 'parameters'],
)
LOG_STREAM_NAME = 'tmp_clusterman_scaling_decisions'
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'  # TODO (CLUSTERMAN-xxx) make this a config param


def get_autoscaling_config(config_namespace):
    """Load autoscaling configuration values from the provided config_namespace, falling back to the
    values stored in the default namespace if none are specified.

    :param config_namespace: namespace to read from before falling back to the default namespace
    :returns: AutoscalingConfig object with loaded config values
    """
    default_setpoint = staticconf.read_float('autoscaling.setpoint')
    default_setpoint_margin = staticconf.read_float('autoscaling.setpoint_margin')
    default_cpus_per_weight = staticconf.read_int('autoscaling.cpus_per_weight')

    reader = staticconf.NamespaceReaders(config_namespace)
    return AutoscalingConfig(
        setpoint=reader.read_float('autoscaling.setpoint', default=default_setpoint),
        setpoint_margin=reader.read_float('autoscaling.setpoint_margin', default=default_setpoint_margin),
        cpus_per_weight=reader.read_int('autoscaling.cpus_per_weight', default=default_cpus_per_weight),
    )


def _get_metrics_index_from_s3(metrics_index_bucket, mesos_region):
    keyname = f'{mesos_region}.yaml'
    metrics_index_object = s3.get_object(Bucket=metrics_index_bucket, Key=keyname)
    return yaml.load(metrics_index_object['Body'])


def _reload_metrics_index():
    stored_metrics = None
    try:
        metrics_index_bucket = staticconf.read_string('aws.s3_metrics_index_bucket')
        aws_region = staticconf.read_string('aws.region')
        stored_metrics = _get_metrics_index_from_s3(metrics_index_bucket, aws_region)
    except staticconf.errors.ConfigurationError:
        logger.warning('no metrics_index_bucket is configured.')
    return stored_metrics


def get_metrics_configs(app_prefix, required_metrics):
    stored_metrics = _reload_metrics_index()
    if stored_metrics is not None:
        all_required_metrics = list()
        for metric in required_metrics:
            regex_name = re.escape(metric.name)
            if metric.type == APP_METRICS:
                metric_regex = re.compile(f'{app_prefix},({regex_name})')
            else:
                metric_regex = re.compile(f'({regex_name})')

            for match in (metric_regex.search(m) for m in stored_metrics[metric.type]):
                if not match:
                    continue
                all_required_metrics.append(MetricConfig(match.group(1), metric.type, metric.minute_range))
    else:
        all_required_metrics = required_metrics

    logger.info(f'Loading metrics data for {all_required_metrics}')
    return all_required_metrics


def read_signal_config(config_namespace, metrics_index=None):
    """Validate and return autoscaling signal config from the given namespace.

    :param config_namespace: namespace to read values from
    :param metrics_index: an index of "tracked" metric keys
    :returns: SignalConfig object with the values filled in
    :raises staticconf.errors.ConfigurationError: if the config namespace is missing a required value
    :raises NoSignalConfiguredException: if the config namespace doesn't define a custom signal
    :raises SignalValidationError: if some signal parameter is incorrectly set
    """
    reader = staticconf.NamespaceReaders(config_namespace)
    try:
        name = reader.read_string('autoscale_signal.name')
    except ConfigurationError:
        raise NoSignalConfiguredException(f'No signal was configured in {config_namespace}')

    period_minutes = reader.read_int('autoscale_signal.period_minutes')
    if period_minutes <= 0:
        raise SignalValidationError(f'Length of signal period must be positive, got {period_minutes}')

    parameter_dict_list = reader.read_list('autoscale_signal.parameters', default=[])
    parameter_dict = {key: value for param_dict in parameter_dict_list for (key, value) in param_dict.items()}

    required_metrics = reader.read_list('autoscale_signal.required_metrics', default=[])
    required_metric_keys = set(MetricConfig._fields)
    required_metric_configs = []
    for metrics_dict in required_metrics:
        missing = required_metric_keys - set(metrics_dict.keys())
        if missing:
            raise SignalValidationError(f'Missing required metric keys {missing} in {metrics_dict}')
        metric_config = {key: metrics_dict[key] for key in metrics_dict if key in required_metric_keys}
        required_metric_configs.append(MetricConfig(**metric_config))
    branch_or_tag = reader.read_string('autoscale_signal.branch_or_tag')
    return SignalConfig(name, SIGNALS_REPO, branch_or_tag, period_minutes, required_metric_configs, parameter_dict)
