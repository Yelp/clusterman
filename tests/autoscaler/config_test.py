import mock
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS

from clusterman.autoscaler.config import _reload_metrics_index
from clusterman.autoscaler.config import get_autoscaling_config
from clusterman.autoscaler.config import get_required_metric_configs
from clusterman.autoscaler.config import MetricConfig


def test_reload_metrics_index():
    with mock.patch('clusterman.autoscaler.config._get_metrics_index_from_s3') as mock_s3, \
            staticconf.testing.PatchConfiguration({
                'aws.s3_metrics_index_bucket': 'metrics-bucket',
                'aws.region': 'fake-region',
            }):
        assert _reload_metrics_index() == mock_s3.return_value


def test_reload_metrics_index_no_index_configured():
    assert _reload_metrics_index() is None


def test_get_autoscaling_config():
    default_autoscaling_values = {
        'setpoint': 0.7,
        'setpoint_margin': 0.1,
        'cpus_per_weight': 8,
    }
    pool_autoscaling_values = {
        'setpoint': 0.8,
        'cpus_per_weight': 10,
    }
    with staticconf.testing.MockConfiguration({'autoscaling': default_autoscaling_values}), \
            staticconf.testing.MockConfiguration({'autoscaling': pool_autoscaling_values}, namespace='pool_namespace'):
        autoscaling_config = get_autoscaling_config('pool_namespace')

        assert autoscaling_config.setpoint == 0.8
        assert autoscaling_config.setpoint_margin == 0.1
        assert autoscaling_config.cpus_per_weight == 10


def test_get_required_metric_configs():
    required_metric_patterns = [
        MetricConfig(name='cpus_allocated', type=SYSTEM_METRICS, minute_range=10),
        MetricConfig(name='project=.*', type=APP_METRICS, minute_range=15),
        MetricConfig(name='_max', type=APP_METRICS, minute_range=20),
        MetricConfig(name='new_max', type=APP_METRICS, minute_range=32),
    ]
    metrics_index = {
        APP_METRICS: ['app1,project=P1', 'app1,project=P2', 'app1,forced_max', 'app2,forced_max', 'app2,new_max'],
        SYSTEM_METRICS: ['cpus_allocated', 'cpus_max'],
    }
    expected_required_metrics = [
        MetricConfig(name='cpus_allocated', type=SYSTEM_METRICS, minute_range=10),
        MetricConfig(name='project=P1', type=APP_METRICS, minute_range=15),
        MetricConfig(name='project=P2', type=APP_METRICS, minute_range=15),
        MetricConfig(name='forced_max', type=APP_METRICS, minute_range=20),
        MetricConfig(name='new_max', type=APP_METRICS, minute_range=32),
    ]
    with mock.patch('clusterman.autoscaler.config._reload_metrics_index') as mock_metrics_index, \
            mock.patch('clusterman.autoscaler.config.logger') as mock_logger:
        mock_metrics_index.return_value = metrics_index
        assert get_required_metric_configs('app1', required_metric_patterns) == expected_required_metrics
        assert mock_logger.warning.call_count == 2


def test_get_required_metric_configs_no_stored_metrics():
    required_metric_patterns = [
        MetricConfig(name='cpus_allocated', type=SYSTEM_METRICS, minute_range=10),
        MetricConfig(name='project=A', type=APP_METRICS, minute_range=15),
    ]
    with mock.patch('clusterman.autoscaler.config._reload_metrics_index') as mock_metrics_index:
        mock_metrics_index.return_value = None
        assert get_required_metric_configs('app1', required_metric_patterns) == required_metric_patterns
