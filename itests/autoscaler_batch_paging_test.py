from argparse import ArgumentParser

import mock
import pytest
import staticconf
from pysensu_yelp import Status

from clusterman.autoscaler.util import ACK
from clusterman.batch.autoscaler import AutoscalerBatch
from clusterman.batch.autoscaler import SERVICE_CHECK_NAME
from clusterman.batch.autoscaler import SIGNAL_CHECK_NAME
from tests.batch.conftest import mock_setup_config_directory
from tests.conftest import clusterman_role_config
from tests.conftest import main_clusterman_config

pytest.mark.usefixtures(main_clusterman_config, clusterman_role_config, mock_setup_config_directory)


def check_sensu_args(call_args, *, signal_role=None, status=Status.OK):
    __, args = call_args
    signal_sensu_config = staticconf.read_list(
        'sensu_config', [{}],
        namespace='bar_config',
    ).pop()
    service_sensu_config = staticconf.read_list('sensu_config', [{}]).pop()
    if signal_role:
        name = SIGNAL_CHECK_NAME
        team = signal_sensu_config['team'] if signal_sensu_config else service_sensu_config['team']
        runbook = signal_sensu_config['runbook'] if signal_sensu_config else service_sensu_config['runbook']
    else:
        name = SERVICE_CHECK_NAME
        team = service_sensu_config['team']
        runbook = service_sensu_config['runbook']

    assert args['name'] == name
    assert args['status'] == status
    assert args['team'] == team
    assert args['runbook'] == runbook


@pytest.fixture(autouse=True)
def autoscaler_batch_patches():
    with mock.patch('clusterman.batch.autoscaler.setup_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.MesosRoleManager'), \
            mock.patch('clusterman.autoscaler.autoscaler.yelp_meteorite'), \
            mock.patch('clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient'), \
            mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', mock.PropertyMock(
                side_effect=[True, False],
            )):
        yield


@pytest.fixture
def autoscaler_batch():
    args = ['--cluster', 'mesos-test']
    parser = ArgumentParser()
    batch = AutoscalerBatch()
    batch.parse_args(parser)
    batch.options = parser.parse_args(args)
    batch.options.instance_name = 'foo'
    return batch


@pytest.mark.parametrize('signal_type', ['default', 'client'])
def test_signal_setup(signal_type, autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config') as mock_signal_config, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        # Autoscaler reads the "default" signal config first and then the client signal config
        mock_signal_config.side_effect = [{}, ValueError] if signal_type == 'client' else [ValueError]
        autoscaler_batch.configure_initial()

        check_sensu_args(
            mock_sensu.call_args_list[0],
            signal_role='bar',
            status=(Status.OK if signal_type == 'default' else Status.CRITICAL),
        )
        check_sensu_args(
            mock_sensu.call_args_list[1],
            status=(Status.OK if signal_type == 'client' else Status.CRITICAL),
        )


def test_signal_connection_failed(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection') as mock_conn, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        mock_conn.side_effect = ValueError
        autoscaler_batch.configure_initial()

        check_sensu_args(mock_sensu.call_args_list[0], signal_role='bar', status=Status.CRITICAL)
        check_sensu_args(mock_sensu.call_args_list[1])


def test_signal_broke(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.autoscaler.autoscaler.evaluate_signal') as mock_evaluate, \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        mock_evaluate.side_effect = ValueError
        autoscaler_batch.configure_initial()
        autoscaler_batch.autoscaler.signal_conn = mock.Mock(return_value=ACK)
        autoscaler_batch.run()

        # sensu is called twice for configure but we care about checks 3 and 4
        check_sensu_args(mock_sensu.call_args_list[0], signal_role='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], signal_role='bar', status=Status.CRITICAL)
        check_sensu_args(mock_sensu.call_args_list[3])


def test_no_signal_config_fallback(autoscaler_batch):
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
        check_sensu_args(mock_sensu.call_args_list[0], signal_role='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], signal_role='bar', status=Status.CRITICAL)
        check_sensu_args(mock_sensu.call_args_list[3])


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
        check_sensu_args(mock_sensu.call_args_list[0], signal_role='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], signal_role='bar')
        check_sensu_args(mock_sensu.call_args_list[3], status=Status.CRITICAL)


def test_everything_is_fine(autoscaler_batch):
    with mock.patch('clusterman.autoscaler.autoscaler.read_signal_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection'), \
            mock.patch('clusterman.util.pysensu_yelp.send_event') as mock_sensu:

        autoscaler_batch.configure_initial()
        autoscaler_batch.autoscaler.signal_conn = mock.Mock()
        autoscaler_batch.autoscaler.signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": {"cpus": null}}']
        autoscaler_batch.run()

        check_sensu_args(mock_sensu.call_args_list[0], signal_role='bar')
        check_sensu_args(mock_sensu.call_args_list[1])
        check_sensu_args(mock_sensu.call_args_list[2], signal_role='bar')
        check_sensu_args(mock_sensu.call_args_list[3])
