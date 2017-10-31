import mock
import pytest

from clusterman.autoscaler.signals.base_signal import SignalResult
from clusterman.autoscaler.signals.downscale.cluster_underutilized import ClusterUnderutilizedSignal


@pytest.fixture
def mock_config():
    return {
        'name': 'ClusterUnderutilizedSignal',
        'priority': 0,
        'query_period_minutes': 42,
        'units_to_remove': 20,
        'scale_down_threshold': 0.3,
    }


@mock.patch('clusterman.autoscaler.signals.downscale.cluster_underutilized.get_average_cpu_util', autospec=True)
class TestClusterUnderutilizedSignal:
    def test_signal_activated(self, mock_cpu_util, mock_config):
        mock_cpu_util.return_value = mock_config['scale_down_threshold'] - 0.1

        signal = ClusterUnderutilizedSignal('foo', 'bar', mock_config)
        assert signal() == SignalResult(True, -mock_config['units_to_remove'])

    def test_signal_ignored_1(self, mock_cpu_util, mock_config):
        mock_cpu_util.side_effect = [
            mock_config['scale_down_threshold'] + 0.1,
            mock_config['scale_down_threshold'] - 0.1,
        ]

        signal = ClusterUnderutilizedSignal('foo', 'bar', mock_config)
        assert signal() == SignalResult()

    def test_signal_ignored_2(self, mock_cpu_util, mock_config):
        mock_cpu_util.side_effect = [
            mock_config['scale_down_threshold'] - 0.1,
            mock_config['scale_down_threshold'] + 0.1,
        ]

        signal = ClusterUnderutilizedSignal('foo', 'bar', mock_config)
        assert signal() == SignalResult()

    def test_ignored_3(self, mock_cpu_util, mock_config):
        mock_cpu_util.side_effect = [
            mock_config['scale_down_threshold'] + 0.1,
            mock_config['scale_down_threshold'] + 0.1,
        ]

        signal = ClusterUnderutilizedSignal('foo', 'bar', mock_config)
        assert signal() == SignalResult()
