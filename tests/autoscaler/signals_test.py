import math
import os

import arrow
import mock
import pytest
import simplejson as json
from clusterman_metrics import APP_METRICS
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS

from clusterman.autoscaler.config import MetricConfig
from clusterman.autoscaler.config import SignalConfig
from clusterman.autoscaler.signals import _get_local_signal_directory
from clusterman.autoscaler.signals import ACK
from clusterman.autoscaler.signals import Signal
from clusterman.exceptions import MetricsError
from clusterman.exceptions import SignalConnectionError


@pytest.fixture
def local_signal_dir_patches():
    with mock.patch('clusterman.autoscaler.signals.os.path.exists') as mock_exists, \
            mock.patch('clusterman.autoscaler.signals.logger') as mock_logger, \
            mock.patch('clusterman.autoscaler.signals.subprocess.run') as mock_run, \
            mock.patch('clusterman.autoscaler.signals.sha_from_branch_or_tag') as mock_sha, \
            mock.patch('clusterman.autoscaler.signals._get_cache_location') as mock_cache:
        mock_sha.return_value = 'abcdefabcdefabcdefabcdefabcdefabcdefabcd'
        mock_cache.return_value = '/foo'
        yield mock_exists, mock_logger, mock_run


def test_signal_already_built(local_signal_dir_patches):
    mock_exists, mock_logger, mock_run = local_signal_dir_patches
    mock_exists.return_value = True
    _get_local_signal_directory('repo', 'a_branch')
    assert mock_exists.call_args == \
        mock.call(os.path.join('/', 'foo', 'clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd'))
    assert mock_run.call_count == 2  # make clean; make prod
    assert mock_logger.debug.call_count == 1  # already cloned repo


def test_signal_not_present(local_signal_dir_patches):
    mock_exists, mock_logger, mock_run = local_signal_dir_patches
    mock_exists.return_value = False
    with mock.patch('clusterman.autoscaler.signals.os.makedirs'):
        _get_local_signal_directory('repo', 'a_branch')
    assert mock_exists.call_args == \
        mock.call(os.path.join('/', 'foo', 'clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd'))
    assert mock_run.call_count == 3  # git clone; make clean; make prod
    assert mock_logger.debug.call_count == 0


@pytest.fixture
def mock_signal():
    with mock.patch('clusterman.autoscaler.signals._load_signal_connection'), \
            mock.patch('clusterman.autoscaler.signals.get_signal_config') as mock_signal_config:
        mock_signal_config.return_value = SignalConfig(
            'BarSignal3',
            'repo',
            'v42',
            7,
            [MetricConfig('cpus_allocated', SYSTEM_METRICS, 10), MetricConfig('cost', APP_METRICS, 30)],
            {},
        )
        return Signal('foo', 'bar', 'app1', 'bar_namespace', mock.Mock())


@pytest.mark.parametrize('conn_response', [['foo'], [ACK, 'foo']])
def test_evaluate_signal_connection_errors(mock_signal, conn_response):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = conn_response
    with pytest.raises(SignalConnectionError):
        mock_signal.evaluate(arrow.get(12345678))
    assert mock_signal._signal_conn.send.call_count == len(conn_response)
    assert mock_signal._signal_conn.recv.call_count == len(conn_response)


@mock.patch('clusterman.autoscaler.signals.SOCKET_MESG_SIZE', 2)
@pytest.mark.parametrize('signal_recv', [
    [ACK, ACK, b'{"Resources": {"cpus": 5.2}}'],
    [ACK, b'\x01{"Resources": {"cpus": 5.2}}'],
])
def test_evaluate_signal_sending_message(mock_signal, signal_recv):
    metrics = {'cpus_allocated': [(1234, 3.5), (1235, 6)]}
    mock_signal._get_metrics = mock.Mock(return_value=metrics)
    num_messages = math.ceil(len(json.dumps({'metrics': metrics, 'timestamp': 12345678})) / 2) + 1
    mock_signal._signal_conn = mock.Mock()
    mock_signal._signal_conn.recv.side_effect = signal_recv
    resp = mock_signal.evaluate(arrow.get(12345678))
    assert mock_signal._signal_conn.send.call_count == num_messages
    assert mock_signal._signal_conn.recv.call_count == len(signal_recv)
    assert resp == {'cpus': 5.2}


@pytest.mark.parametrize('end_time', [arrow.get(3600), arrow.get(10000), arrow.get(35000)])
def test_get_metrics(mock_signal, end_time):
    mock_signal.metrics_client.get_metric_values.side_effect = [
        ('cpus_allocated|cluster=foo,pool=bar', [(1, 2), (3, 4)]),
        ('cost', [(1, 2.5), (3, 4.5)]),
    ]
    with mock.patch('clusterman.autoscaler.signals.get_required_metric_configs') as mock_get_configs:
        mock_get_configs.return_value = [
            MetricConfig('cpus_allocated', SYSTEM_METRICS, 10),
            MetricConfig('cost', APP_METRICS, 30),
        ]
        metrics = mock_signal._get_metrics(end_time)
    assert mock_signal.metrics_client.get_metric_values.call_args_list == [
        mock.call(
            'cpus_allocated|cluster=foo,pool=bar',
            SYSTEM_METRICS,
            end_time.shift(minutes=-10).timestamp,
            end_time.timestamp,
        ),
        mock.call(
            'app1,cost',
            APP_METRICS,
            end_time.shift(minutes=-30).timestamp,
            end_time.timestamp,
        )
    ]
    assert 'cpus_allocated' in metrics
    assert 'cost' in metrics


def test_get_metadata_metrics(mock_signal):
    with mock.patch('clusterman.autoscaler.signals.get_required_metric_configs') as mock_get_configs, \
            pytest.raises(MetricsError):
        mock_get_configs.return_value = [MetricConfig('total_cpus', METADATA, 10)]
        mock_signal._get_metrics(arrow.get(0))
