import math
import os

import arrow
import mock
import pytest
import simplejson as json
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS
from simplejson.errors import JSONDecodeError

from clusterman.autoscaler.config import MetricConfig
from clusterman.autoscaler.signals import _get_local_signal_directory
from clusterman.autoscaler.signals import ACK
from clusterman.autoscaler.signals import Signal
from clusterman.autoscaler.signals import SignalConfig
from clusterman.autoscaler.signals import SIGNALS_REPO
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import MetricsError
from clusterman.exceptions import NoSignalConfiguredException
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
def signal_config_base():
    return {'autoscale_signal': {
        'name': 'BarSignal3',
        'branch_or_tag': 'v42',
        'period_minutes': 7,
    }}


@pytest.fixture
def mock_signal():
    with mock.patch('clusterman.autoscaler.signals.Signal._load_signal_connection'), \
            mock.patch('clusterman.autoscaler.signals.Signal._get_signal_config') as mock_signal_config:
        mock_signal_config.return_value = SignalConfig(
            'BarSignal3',
            'repo',
            'v42',
            7,
            [MetricConfig('cpus_allocated', SYSTEM_METRICS, 10), MetricConfig('cost', APP_METRICS, 30)],
            {},
        )
        mock_signal = Signal('foo', 'bar', 'app1', 'bar_namespace', mock.Mock())
        return mock_signal


@pytest.mark.parametrize('conn_response', [['foo'], [ACK, 'foo']])
def test_evaluate_signal_connection_errors(mock_signal, conn_response):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = conn_response
    with pytest.raises(SignalConnectionError):
        mock_signal.evaluate(arrow.get(12345678))
    assert type(mock_signal.exception) == SignalConnectionError
    assert mock_signal._signal_conn.send.call_count == len(conn_response)
    assert mock_signal._signal_conn.recv.call_count == len(conn_response)


def test_evaluate_broken_signal(mock_signal):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [ACK, ACK, 'error']
    with pytest.raises(JSONDecodeError):
        mock_signal.evaluate(arrow.get(12345678))
    assert type(mock_signal.exception) == ClustermanSignalError


def test_evaluate_restart_dead_signal(mock_signal):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [BrokenPipeError, ACK, ACK, '{"Resources": {"cpus": 1}}']
    with mock.patch('clusterman.autoscaler.signals.Signal._load_signal_connection') as mock_load:
        mock_load.return_value = mock_signal._signal_conn
        assert mock_signal.evaluate(arrow.get(12345678)) == {'cpus': 1}
        assert mock_load.call_count == 1
    assert mock_signal.error_state is None


@pytest.mark.parametrize('error', [BrokenPipeError, 'error'])
def test_evaluate_restart_dead_signal_fails(mock_signal, error):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [BrokenPipeError, ACK, ACK, error]
    with mock.patch('clusterman.autoscaler.signals.Signal._load_signal_connection') as mock_load, \
            pytest.raises(JSONDecodeError if isinstance(error, str) else BrokenPipeError):
        mock_load.return_value = mock_signal._signal_conn
        mock_signal.evaluate(arrow.get(12345678))
        assert mock_load.call_count == 1
    assert type(mock_signal.exception) == ClustermanSignalError


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


def test_get_signal_config(mock_signal):
    with staticconf.testing.MockConfiguration({}, namespace='util_testing'), pytest.raises(NoSignalConfiguredException):
        mock_signal._get_signal_config('util_testing')


def test_get_signal_config_optional_values(mock_signal):
    config_dict = signal_config_base()
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = mock_signal._get_signal_config('util_testing')

    assert config == SignalConfig('BarSignal3', SIGNALS_REPO, 'v42', 7, [], {})


def test_get_signal_config_valid_values(mock_signal):
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
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = mock_signal._get_signal_config('util_testing')

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
            mock_signal._get_signal_config('util_testing', {})


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
