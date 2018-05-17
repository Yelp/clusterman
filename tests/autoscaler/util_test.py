import math
import os

import mock
import pytest
import simplejson as json
import staticconf.testing

from clusterman.autoscaler.util import _get_local_signal_directory
from clusterman.autoscaler.util import _sha_from_branch_or_tag
from clusterman.autoscaler.util import ACK
from clusterman.autoscaler.util import evaluate_signal
from clusterman.autoscaler.util import MetricConfig
from clusterman.autoscaler.util import read_signal_config
from clusterman.autoscaler.util import SignalConfig
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError


@pytest.fixture
def signal_config_base():
    return {'autoscale_signal': {
        'name': 'BarSignal3',
        'branch_or_tag': 'v42',
        'period_minutes': 7,
    }}


@pytest.fixture
def mock_sha():
    with mock.patch('clusterman.autoscaler.util._sha_from_branch_or_tag') as m:
        m.return_value = 'abcdefabcdefabcdefabcdefabcdefabcdefabcd'
        yield


@pytest.fixture
def mock_cache():
    with mock.patch('clusterman.autoscaler.util._get_cache_location') as m:
        m.return_value = '/foo'
        yield


def test_read_config_none():
    with staticconf.testing.MockConfiguration({}, namespace='util_testing'), pytest.raises(NoSignalConfiguredException):
        read_signal_config('util_testing')


def test_read_config_optional_values():
    config_dict = signal_config_base()
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = read_signal_config('util_testing')

    assert config == SignalConfig('BarSignal3', 'v42', 7, [], {})


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
        'parameters': [
            {'paramA': 'abc'},
            {'otherParam': 18},
        ],
    })
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        config = read_signal_config('util_testing')

    assert config == SignalConfig(
        'BarSignal3',
        'v42',
        7,
        mock.ANY,
        {'paramA': 'abc', 'otherParam': 18},
    )
    assert config.required_metrics == sorted(
        [MetricConfig('metricB', 'app_metrics', 1), MetricConfig('metricEE', 'system_metrics', 12)]
    )


@pytest.mark.parametrize('period_minutes', [1, -1])
def test_read_signal_invalid_metrics(period_minutes):
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
        'period_minutes': period_minutes,
    })
    with staticconf.testing.MockConfiguration(config_dict, namespace='util_testing'):
        with pytest.raises(Exception):
            read_signal_config('util_testing')


@mock.patch('clusterman.autoscaler.util.subprocess.run')
def test_sha_from_branch_or_tag(mock_run):
    mock_run.return_value.returncode = 0
    mock_run.return_value.stdout = 'abcdefabcdefabcdefabcdefabcdefabcdefabcd\trefs/heads/a_branch'.encode()
    assert _sha_from_branch_or_tag('a_branch') == 'abcdefabcdefabcdefabcdefabcdefabcdefabcd'


@mock.patch('clusterman.autoscaler.util.os.path.exists')
@mock.patch('clusterman.autoscaler.util.logger')
@mock.patch('clusterman.autoscaler.util.subprocess.run')
class TestMakeVenv:
    def test_already_built(self, mock_run, mock_logger, mock_exists, mock_sha, mock_cache):
        mock_exists.return_value = True
        _get_local_signal_directory('a_branch')
        assert mock_exists.call_args == \
            mock.call(os.path.join('/', 'foo', 'clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd'))
        assert mock_run.call_count == 2
        assert mock_logger.debug.call_count == 1

    def test_not_present(self, mock_run, mock_logger, mock_exists, mock_sha, mock_cache):
        mock_exists.return_value = False
        with mock.patch('clusterman.autoscaler.util.os.makedirs'):
            _get_local_signal_directory('a_branch')
        assert mock_exists.call_args == \
            mock.call(os.path.join('/', 'foo', 'clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd'))
        assert mock_run.call_count == 3
        assert mock_logger.debug.call_count == 0


@pytest.mark.parametrize('conn_response', [['foo'], [ACK, 'foo']])
def test_evaluate_signal_connection_errors(conn_response):
    mock_signal_conn = mock.Mock()
    mock_signal_conn.recv.side_effect = conn_response
    with pytest.raises(SignalConnectionError):
        evaluate_signal({}, mock_signal_conn)
    assert mock_signal_conn.send.call_count == len(conn_response)
    assert mock_signal_conn.recv.call_count == len(conn_response)


@mock.patch('clusterman.autoscaler.util.SOCK_MESG_SIZE', 2)
@pytest.mark.parametrize('signal_recv', [
    [ACK, ACK, b'{"Resources": {"cpus": 5.2}}'],
    [ACK, b'\x01{"Resources": {"cpus": 5.2}}'],
])
def test_evaluate_sending_message(signal_recv):
    metrics = {'cpus_allocated': [(1234, 3.5), (1235, 6)]}
    num_messages = math.ceil(len(json.dumps({'metrics': metrics})) / 2) + 1
    mock_signal_conn = mock.Mock()
    mock_signal_conn.recv.side_effect = signal_recv
    resp = evaluate_signal(metrics, mock_signal_conn)
    assert mock_signal_conn.send.call_count == num_messages
    assert mock_signal_conn.recv.call_count == len(signal_recv)
    assert resp == {'cpus': 5.2}
