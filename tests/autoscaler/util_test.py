import mock
import pytest
import staticconf.testing
from clusterman_signals.base_signal import MetricConfig

from clusterman.autoscaler.util import read_signal_config
from clusterman.autoscaler.util import SignalConfig


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
