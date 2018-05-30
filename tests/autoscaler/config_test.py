import mock
import pytest
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS

from clusterman.autoscaler.config import _reload_metrics_index
from clusterman.autoscaler.config import get_autoscaling_config
from clusterman.autoscaler.config import get_required_metric_configs
from clusterman.autoscaler.config import get_signal_config
from clusterman.autoscaler.config import MetricConfig
from clusterman.autoscaler.config import SignalConfig
from clusterman.autoscaler.config import SIGNALS_REPO
from clusterman.exceptions import NoSignalConfiguredException


@pytest.fixture
def signal_config_base():
    return {'autoscale_signal': {
        'name': 'BarSignal3',
        'branch_or_tag': 'v42',
        'period_minutes': 7,
    }}


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
    ]
    with mock.patch('clusterman.autoscaler.config._reload_metrics_index') as mock_metrics_index:
        mock_metrics_index.return_value = metrics_index
        assert get_required_metric_configs('app1', required_metric_patterns) == expected_required_metrics


def test_get_required_metric_configs_no_stored_metrics():
    required_metric_patterns = [
        MetricConfig(name='cpus_allocated', type=SYSTEM_METRICS, minute_range=10),
        MetricConfig(name='project=A', type=APP_METRICS, minute_range=15),
    ]
    with mock.patch('clusterman.autoscaler.config._reload_metrics_index') as mock_metrics_index:
        mock_metrics_index.return_value = None
        assert get_required_metric_configs('app1', required_metric_patterns) == required_metric_patterns


def test_get_signal_config():
    with staticconf.testing.MockConfiguration({}, namespace='util_testing'), pytest.raises(NoSignalConfiguredException):
        get_signal_config('util_testing', 'us-test-3')


def test_get_signal_config_optional_values():
    config_dict = signal_config_base()
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = get_signal_config('util_testing', 'us-test-3')

    assert config == SignalConfig('BarSignal3', SIGNALS_REPO, 'v42', 7, [], {})


def test_get_signal_config_valid_values():
    config_dict = signal_config_base()
    config_dict['autoscale_signal'].update({
        'required_metrics': [
            {
                'name': 'metricB',
                'type': APP_METRICS,
                'minute_range': 1,
            },
            {
                'name': 'metricEE',
                'type': SYSTEM_METRICS,
                'minute_range': 12,
            },
        ],
        'parameters': [
            {'paramA': 'abc'},
            {'otherParam': 18},
        ],
    })
    metrics_index = {
        APP_METRICS: ['metricB'],
        SYSTEM_METRICS: ['metricEE'],
    }
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = get_signal_config('util_testing', metrics_index)

    assert config == SignalConfig(
        'BarSignal3',
        SIGNALS_REPO,
        'v42',
        7,
        mock.ANY,
        {'paramA': 'abc', 'otherParam': 18},
    )
    assert config.required_metrics == sorted(
        [MetricConfig('metricB', APP_METRICS, 1), MetricConfig('metricEE', SYSTEM_METRICS, 12)]
    )


@pytest.mark.parametrize('period_minutes', [1, -1])
def test_get_signal_config_invalid_metrics(period_minutes):
    config_dict = signal_config_base()
    config_dict['autoscale_signal'].update({
        'required_metrics': [
            {
                'name': 'metricB',
                'type': APP_METRICS,
                'minute_range': 1,
            },
            {
                'name': 'metricEE',
                'type': SYSTEM_METRICS,
            },
        ],
        'period_minutes': period_minutes,
    })
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        with pytest.raises(Exception):
            get_signal_config('util_testing', {})
