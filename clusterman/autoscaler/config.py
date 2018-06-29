import re
from collections import namedtuple

import botocore.exceptions
import staticconf
import yaml
from clusterman_metrics import APP_METRICS

from clusterman.aws.client import s3
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
AutoscalingConfig = namedtuple(
    'AutoscalingConfig',
    ['setpoint', 'setpoint_margin', 'cpus_per_weight'],
)
MetricConfig = namedtuple('MetricConfig', ['name', 'type', 'minute_range'])
LOG_STREAM_NAME = 'tmp_clusterman_scaling_decisions'
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'  # TODO (CLUSTERMAN-xxx) make this a config param


def _get_metrics_index_from_s3(metrics_index_bucket, mesos_region):  # pragma: no cover
    keyname = f'{mesos_region}.yaml'
    metrics_index_object = s3.get_object(Bucket=metrics_index_bucket, Key=keyname)
    return yaml.safe_load(metrics_index_object['Body'])


def _reload_metrics_index():
    stored_metrics = None
    try:
        metrics_index_bucket = staticconf.read_string('aws.s3_metrics_index_bucket')
        aws_region = staticconf.read_string('aws.region')
        stored_metrics = _get_metrics_index_from_s3(metrics_index_bucket, aws_region)
    except staticconf.errors.ConfigurationError:
        logger.warning('no metrics_index_bucket is configured.')
    except botocore.exceptions.ClientError:
        logger.warning(f'Config loading error for metrics_index_bucket={metrics_index_bucket} '
                       f'and aws_region={aws_region}.', exc_info=True)
    return stored_metrics


def get_autoscaling_config(config_namespace):
    """ Load autoscaling configuration values from the provided config_namespace, falling back to the
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


def get_required_metric_configs(app_prefix, required_metrics_patterns):
    """ Translate a set of MetricsConfig objects with regex pattern names into a list of MetricsConfig
    objects with the actual names of any metrics matching those patterns

    :param app_prefix: the name of the app to get metrics for
    :param required_metrics_patterns: a list of MetricsConfig objects
    :returns: a list of MetricsConfig objects
    """
    stored_metrics = _reload_metrics_index()
    if stored_metrics is not None:
        all_required_metrics = list()
        for metric in required_metrics_patterns:
            if metric.type == APP_METRICS:
                # if we have an application metric we want to ensure that we don't get metrics
                # for any other applications that might duplicate a name; the remainder of the
                # regex allows us to find the pattern anywhere in the non-prefixed name
                metric_regex = re.compile(f'^{app_prefix},(.*{metric.name}.*)')
            else:
                metric_regex = re.compile(f'({metric.name})')

            matched = False
            for match in (metric_regex.search(m) for m in stored_metrics[metric.type]):
                if not match:
                    continue
                matched = True
                all_required_metrics.append(MetricConfig(match.group(1), metric.type, metric.minute_range))

            if not matched:
                logger.warning(f'Could not find {metric.name} in the index; is there a typo or is the index outdated?')
                logger.warning(f'Adding unmatched metric to list just in case')
                all_required_metrics.append(metric)

    else:
        all_required_metrics = required_metrics_patterns

    logger.info(f'Loading metrics data for {all_required_metrics}')
    return all_required_metrics
