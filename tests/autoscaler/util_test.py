import mock
import pytest
import staticconf.testing
from clusterman_signals.base_signal import MetricConfig

from clusterman.autoscaler.util import get_average_cpu_util
from clusterman.autoscaler.util import read_signal_config
from clusterman.autoscaler.util import SignalConfig
from clusterman.exceptions import MetricsError


@mock.patch('clusterman.autoscaler.util.ClustermanMetricsBotoClient', autospec=True)
def test_get_average_cpu_util(mock_metrics_client):
    mock_metrics_client.return_value.get_metric_values.return_value = 'asdf', [
        [0, 0.8],
        [2, 0.1],
        [3, 0.3],
        [4, 0.6],
        [7, 0.4],
        [9, 0.4],
    ]

    assert pytest.approx(get_average_cpu_util('mesos-test', 'bar', 95),  0.433333)


@mock.patch('clusterman.autoscaler.util.ClustermanMetricsBotoClient', autospec=True)
def test_get_average_cpu_util_no_data(mock_metrics_client):
    mock_metrics_client.return_value.get_metric_values.return_value = 'asdf', []
    with pytest.raises(MetricsError):
        get_average_cpu_util('mesos-test', 'bar', 10)


@pytest.fixture
def signal_config_base():
    return {'autoscale_signal': {
        'name': 'BarSignal3',
        'period_minutes': 7,
    }}


def test_read_config_none():
    with staticconf.testing.MockConfiguration({}, namespace='util_testing'):
        config = read_signal_config('util_testing')
    assert config is None


def test_read_config_optional_values():
    config_dict = signal_config_base()
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = read_signal_config('util_testing')

    assert config == SignalConfig('BarSignal3', 7, [], {})


def test_read_config_valid_values():
    config_dict = signal_config_base()
    config_dict['autoscale_signal'].update({
        'required_metrics': [
            {
                'name': 'metricB',
                'type': 'app_metrics',
                'minute_range': 1,
            },
            {
                'name': 'metricEE',
                'type': 'system_metrics',
                'minute_range': 12,
            },
        ],
        'custom_parameters': [
            {'paramA': 'abc'},
            {'otherParam': 18},
        ],
    })
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = read_signal_config('util_testing')

    assert config == SignalConfig(
        'BarSignal3',
        7,
        mock.ANY,
        {'paramA': 'abc', 'otherParam': 18},
    )
    assert config.required_metrics == sorted(
        [MetricConfig('metricB', 'app_metrics', 1), MetricConfig('metricEE', 'system_metrics', 12)]
    )


def test_read_signal_invalid_metrics():
    config_dict = signal_config_base()
    config_dict['autoscale_signal'].update({
        'required_metrics': [
            {
                'name': 'metricB',
                'type': 'app_metrics',
                'minute_range': 1,
            },
            {
                'name': 'metricEE',
                'type': 'system_metrics',
            },
        ],
    })
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        with pytest.raises(Exception):
            read_signal_config('util_testing')
