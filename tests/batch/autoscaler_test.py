import argparse

import mock
import pytest
import staticconf.testing

from clusterman.batch.autoscaler import AutoscalerBatch


@pytest.fixture
def batch(args=None):
    batch = AutoscalerBatch()
    args = args or ['--cluster', 'mesos-test']
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    batch.options = parser.parse_args(args)
    batch.configure_initial()
    return batch


@pytest.fixture(autouse=True)
def mock_logger():
    with mock.patch('clusterman.batch.autoscaler.logger', autospec=True) as mock_logger:
        yield mock_logger


@pytest.fixture
def mock_setup_config():
    with mock.patch('clusterman.batch.autoscaler.setup_config', autospec=True) as mock_setup:
        yield mock_setup


@pytest.fixture(autouse=True)
def mock_watcher():
    with mock.patch('staticconf.config.ConfigurationWatcher', autospec=True):
        yield


@pytest.fixture
def mock_autoscaler():
    with mock.patch('clusterman.batch.autoscaler.Autoscaler', autospec=True) as autoscaler:
        yield autoscaler


@pytest.mark.parametrize('roles_in_cluster', [[], ['role_A', 'role_B']])
def test_invalid_role_numbers(roles_in_cluster, batch):
    with staticconf.testing.PatchConfiguration(
        {'cluster_roles': roles_in_cluster}
    ):
        with pytest.raises(Exception):
            batch.run()


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', new_callable=mock.PropertyMock)
@pytest.mark.parametrize('dry_run', [True, False])
def test_run(mock_running, mock_time, mock_sleep, dry_run, mock_autoscaler, mock_setup_config):
    args = ['--cluster', 'mesos-test']
    if dry_run:
        args.append('--dry-run')
    batch_obj = batch(args)

    assert mock_setup_config.call_count == 1
    mock_running.side_effect = [True, True, True, False]
    # batch interval is configured to be 600
    mock_time.side_effect = [101, 913, 2000]

    batch_obj.run()
    assert mock_autoscaler.call_args_list == [mock.call('mesos-test', 'bar')]
    assert mock_autoscaler.return_value.run.call_args_list == [mock.call(dry_run=dry_run) for i in range(3)]
    assert mock_sleep.call_args_list == [mock.call(499), mock.call(287), mock.call(400)]
