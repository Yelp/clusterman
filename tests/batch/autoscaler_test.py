import argparse
from functools import partial

import mock
import pytest
import staticconf.testing

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.batch.autoscaler import AutoscalerBatch


@pytest.fixture
def batch(args=None):
    with mock.patch('clusterman.batch.autoscaler.setup_config'), \
            mock.patch('clusterman.batch.autoscaler.Autoscaler', signal=mock.Mock()) as autoscaler:
        autoscaler.signal_config.period_minutes = 10
        autoscaler.return_value.time_to_next_activation = partial(
            Autoscaler.time_to_next_activation,
            self=autoscaler,
        )
        batch = AutoscalerBatch()
        args = args or ['--cluster', 'mesos-test']
        parser = argparse.ArgumentParser()
        batch.parse_args(parser)
        batch.options = parser.parse_args(args)
        batch.configure_initial()
        batch.version_checker = mock.Mock(watchers=[])
        return batch


@pytest.fixture(autouse=True)
def mock_logger():
    with mock.patch('clusterman.batch.autoscaler.logger', autospec=True) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def mock_watcher():
    with mock.patch('staticconf.config.ConfigurationWatcher', autospec=True):
        yield


@pytest.mark.parametrize('roles_in_cluster', [[], ['role_A', 'role_B']])
def test_invalid_role_numbers(roles_in_cluster):
    with staticconf.testing.PatchConfiguration({'cluster_roles': roles_in_cluster}), \
            pytest.raises(Exception):
        batch()


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', new_callable=mock.PropertyMock)
@mock.patch('clusterman.batch.autoscaler.sensu_checkin', autospec=True)
@pytest.mark.parametrize('dry_run', [True, False])
def test_run(mock_sensu, mock_running, mock_time, mock_sleep, dry_run):
    args = ['--cluster', 'mesos-test']
    if dry_run:
        args.append('--dry-run')
    batch_obj = batch(args)

    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 913, 2000]

    batch_obj.run()
    assert batch_obj.autoscaler.run.call_args_list == [mock.call(dry_run=dry_run) for i in range(3)]
    assert mock_sleep.call_args_list == [mock.call(499), mock.call(287), mock.call(400)]
    assert mock_sensu.call_count == 3
