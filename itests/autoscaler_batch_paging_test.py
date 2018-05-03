from argparse import ArgumentParser
from contextlib import ExitStack

import mock
import pytest
import staticconf
from pysensu_yelp import Status
from simplejson import JSONDecodeError

from clusterman.autoscaler.autoscaler import SIGNAL_LOAD_CHECK_NAME
from clusterman.autoscaler.util import ACK
from clusterman.batch.autoscaler import AutoscalerBatch
from clusterman.batch.autoscaler import SERVICE_CHECK_NAME
from clusterman.batch.autoscaler import SIGNAL_CHECK_NAME
from clusterman.exceptions import AutoscalerError
from tests.batch.conftest import mock_setup_config_directory
from tests.conftest import clusterman_pool_config
from tests.conftest import main_clusterman_config

pytest.mark.usefixtures(main_clusterman_config, clusterman_pool_config, mock_setup_config_directory)


def check_sensu_args(call_args, *, name=None, app_name=None, status=Status.OK):
    __, args = call_args
    signal_sensu_config = staticconf.read_list(
        'sensu_config', [{}],
        namespace='bar_config',
    ).pop()
    service_sensu_config = staticconf.read_list('sensu_config', [{}]).pop()
    if app_name:
        name = name or SIGNAL_CHECK_NAME
        team = signal_sensu_config['team'] if signal_sensu_config else service_sensu_config['team']
        runbook = signal_sensu_config['runbook'] if signal_sensu_config else service_sensu_config['runbook']
    else:
        name = name or SERVICE_CHECK_NAME
        team = service_sensu_config['team']
        runbook = service_sensu_config['runbook']

    assert args['name'] == name
    assert args['status'] == status
    assert args['team'] == team
    assert args['runbook'] == runbook


@pytest.fixture(autouse=True)
def autoscaler_batch_patches():
    with mock.patch('clusterman.batch.autoscaler.setup_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.MesosPoolManager'), \
            mock.patch('clusterman.autoscaler.autoscaler.yelp_meteorite'), \
            mock.patch('clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient'), \
            mock.patch('clusterman.batch.autoscaler.splay_time_start') as mock_splay, \
            mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', mock.PropertyMock(
                side_effect=[True, False],
            )):
        mock_splay.return_value = 0
        yield


@pytest.fixture
def autoscaler_batch():
    args = ['--cluster', 'mesos-test', '--pool', 'bar']
    parser = ArgumentParser()
    batch = AutoscalerBatch()
    batch.parse_args(parser)
    batch.options = parser.parse_args(args)
    batch.options.instance_name = 'foo'
    return batch


@pytest.mark.parametrize('signal_type', ['default', 'client'])
def test_signal_setup_fallback(signal_type, autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config') as mock_signal_config, \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        # Autoscaler reads the "default" signal config first and then the client signal config
        mock_signal_config.side_effect = [mock.MagicMock(), ValueError] if signal_type == 'client' else [ValueError]
        with (pytest.raises(AutoscalerError) if signal_type == 'default' else ExitStack()):
            autoscaler_batch.configure_initial()

        i = 0
        if signal_type == 'client':
            check_sensu_args(
                mock_sensu.call_args_list[i],
                name=SIGNAL_LOAD_CHECK_NAME,
                app_name='bar',
                status=(Status.OK if signal_type == 'default' else Status.WARNING),
            )
            i += 1
        check_sensu_args(
            mock_sensu.call_args_list[i],
            app_name='bar',
            status=Status.OK,
        )
        check_sensu_args(
            mock_sensu.call_args_list[i + 1],
            status=(Status.OK if signal_type == 'client' else Status.CRITICAL),
        )


def test_signal_connection_failed(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection') as mock_conn, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        mock_conn.side_effect = ValueError
        with pytest.raises(AutoscalerError):
            autoscaler_batch.configure_initial()

        check_sensu_args(mock_sensu.call_args_list[0], app_name='bar', status=Status.CRITICAL)
        check_sensu_args(mock_sensu.call_args_list[1])


def test_signal_broke(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.autoscaler.autoscaler.evaluate_signal') as mock_evaluate, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu, \
            mock.patch('clusterman.autoscaler.autoscaler.logger.error') as mock_logger_error, \
            mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', mock.PropertyMock(
                side_effect=[True, True, False],
            )):

        mock_evaluate.side_effect = [JSONDecodeError('foo', 'bar', 3), BrokenPipeError]
        autoscaler_batch.configure_initial()
        autoscaler_batch.autoscaler.signal_conn = mock.Mock(return_value=ACK)
        autoscaler_batch.run()

        # sensu is called twice for configure but we care about checks 3 and 4
        check_sensu_args(mock_sensu.call_args_list[0], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], app_name='bar', status=Status.CRITICAL)
        check_sensu_args(mock_sensu.call_args_list[3])
        check_sensu_args(mock_sensu.call_args_list[2], app_name='bar', status=Status.CRITICAL)
        check_sensu_args(mock_sensu.call_args_list[3])

        assert autoscaler_batch.autoscaler._last_signal_traceback == 'bar'
        assert all(['bar' in call[0][0] for call in mock_logger_error.call_args_list])


def test_evaluate_signal_broke(autoscaler_batch):
    with staticconf.testing.MockConfiguration({}, namespace='bar_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.autoscaler.autoscaler.evaluate_signal') as mock_evaluate, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        mock_evaluate.side_effect = ValueError
        autoscaler_batch.configure_initial()
        autoscaler_batch.autoscaler.signal_conn = mock.Mock(return_value=ACK)
        autoscaler_batch.run()

        # sensu is called twice for configure but we care about checks 3 and 4
        check_sensu_args(mock_sensu.call_args_list[0], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[3], status=Status.CRITICAL)


def test_service_broke(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._compute_target_capacity') as mock_capacity, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        mock_capacity.side_effect = ValueError
        autoscaler_batch.configure_initial()
        autoscaler_batch.autoscaler.signal_conn = mock.Mock()
        autoscaler_batch.run()

        # sensu is called twice for configure but we care about checks 3 and 4
        check_sensu_args(mock_sensu.call_args_list[0], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[3], status=Status.CRITICAL)


def test_everything_is_fine(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        autoscaler_batch.configure_initial()
        autoscaler_batch.autoscaler.signal_conn = mock.Mock()
        autoscaler_batch.autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": null}}']
        autoscaler_batch.run()

        check_sensu_args(mock_sensu.call_args_list[0], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], app_name='bar')
        check_sensu_args(mock_sensu.call_args_list[3])
