import mock
import staticconf

from clusterman.autoscaler.signals.base_signal import BaseSignal


class DummySignal(BaseSignal):
    def __call__(self):
        pass


@mock.patch('clusterman.autoscaler.signals.base_signal.logger', autospec=True)
def test_base_signal_init(mock_logger):
    config = staticconf.NamespaceReaders('bar_config')
    signal_config = config.read_list('autoscale_signals')[0]
    a = DummySignal('foo', 'bar', signal_config)
    assert a.priority == 1
    assert mock_logger.warn.call_count == 0


@mock.patch('clusterman.autoscaler.signals.base_signal.logger', autospec=True)
def test_base_signal_init_no_priority(mock_logger):
    config = staticconf.NamespaceReaders('bar_config')
    signal_config = config.read_list('autoscale_signals')[1]
    a = DummySignal('foo', 'bar', signal_config)
    assert a.priority == 0
    assert mock_logger.warn.call_count == 1
