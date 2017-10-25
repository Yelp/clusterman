import mock
import pytest
import staticconf

from clusterman.autoscaler.signals.base_signal import BaseSignal


class DummySignal(BaseSignal):
    def delta(self):
        pass


@mock.patch('clusterman.autoscaler.signals.base_signal.logger', autospec=True)
def test_base_signal_init(mock_logger, mock_autoscaler_config):
    config = staticconf.NamespaceReaders('bar_config')
    role_config = config.read_list('autoscale_signals')[0]
    a = DummySignal('foo', 'bar', role_config)
    assert a.priority == 1
    assert mock_logger.warn.call_count == 0


@mock.patch('clusterman.autoscaler.signals.base_signal.logger', autospec=True)
def test_base_signal_init_no_priority(mock_logger, mock_autoscaler_config):
    config = staticconf.NamespaceReaders('bar_config')
    role_config = config.read_list('autoscale_signals')[1]
    a = DummySignal('foo', 'bar', role_config)
    assert a.priority == 0
    assert mock_logger.warn.call_count == 1


@pytest.mark.parametrize('active', [True, False])
def test_base_signal_reset_active(active, mock_autoscaler_config):
    config = staticconf.NamespaceReaders('bar_config')
    role_config = config.read_list('autoscale_signals')[0]
    a = DummySignal('foo', 'bar', role_config)
    a._active = active
    assert a.active == active
    assert not a.active
