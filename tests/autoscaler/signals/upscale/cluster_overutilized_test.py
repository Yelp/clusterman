import mock
import pytest

from clusterman.autoscaler.signals.upscale import ClusterOverutilizedSignal


@pytest.fixture
def mock_config():
    return {
        'name': 'ClusterOverutilizedSignal',
        'priority': 0,
        'query_period_minutes': 13,
        'units_to_add': 500,
        'scale_up_threshold': 0.71,
    }


@mock.patch('clusterman.autoscaler.signals.upscale.cluster_overutilized.get_average_cpu_util', autospec=True)
class TestClusterOverutilizedSignal:
    def test_signal(self, mock_cpu_util, mock_config):
        mock_cpu_util.return_value = mock_config['scale_up_threshold'] + 0.1

        signal = ClusterOverutilizedSignal('foo', 'bar', mock_config)
        assert signal.delta() == mock_config['units_to_add']
        assert signal.active

    def test_signal_ignored(self, mock_cpu_util, mock_config):
        mock_cpu_util.return_value = mock_config['scale_up_threshold'] - 0.1

        signal = ClusterOverutilizedSignal('foo', 'bar', mock_config)
        assert signal.delta() == 0
        assert not signal.active
