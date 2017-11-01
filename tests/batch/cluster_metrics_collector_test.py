import argparse
from contextlib import contextmanager

import mock
import pytest
from clusterman_metrics import SYSTEM_METRICS

from clusterman.batch.cluster_metrics_collector import ClusterMetricsCollector
from clusterman.mesos.mesos_role_manager import MesosRoleManager


@pytest.fixture
def batch(args=None):
    batch = ClusterMetricsCollector()
    args = args or ['--cluster', 'mesos-test']
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    batch.options = parser.parse_args(args)
    return batch


@mock.patch('clusterman.batch.cluster_metrics_collector.MesosRoleManager', autospec=True)
@mock.patch('clusterman.batch.cluster_metrics_collector.get_roles_in_cluster')
def test_configure_initial(mock_get_roles, mock_mesos_role_manager, batch):
    mock_get_roles.return_value = ['role-1', 'role-3']
    with mock.patch('clusterman.batch.cluster_metrics_collector.setup_config'):
        batch.configure_initial()

    assert batch.run_interval == 120
    assert batch.region == 'us-west-2'  # region from cluster configs
    assert mock_get_roles.call_args_list == [mock.call('mesos-test')]
    assert sorted(batch.mesos_managers.keys()) == ['role-1', 'role-3']
    for manager in batch.mesos_managers.values():
        assert isinstance(manager, MesosRoleManager)


@mock.patch('clusterman.batch.cluster_metrics_collector.ClustermanMetricsBotoClient', autospec=True)
def test_get_writer(mock_client_class, batch):
    batch.region = 'us-test-2'
    # yelp_batch will create the context manager because of @batch_context when running
    # but do it ourselves for the unit test
    context = contextmanager(batch.get_writer)()
    with context:
        assert mock_client_class.call_args_list == [mock.call(region_name='us-test-2')]
        mock_client = mock_client_class.return_value
        assert mock_client.get_writer.call_args_list == [mock.call(SYSTEM_METRICS)]
        writer_context = mock_client.get_writer.return_value
        assert batch.writer == writer_context.__enter__.return_value
    assert writer_context.__exit__.call_count == 1


def test_write_metrics(batch):
    batch.writer = mock.Mock()
    batch.mesos_managers = {
        'role_A': mock.Mock(spec_set=MesosRoleManager),
        'role_B': mock.Mock(spec_set=MesosRoleManager),
    }
    batch.write_metrics()

    for role, manager in batch.mesos_managers.items():
        assert manager.get_average_resource_utilization.call_args_list == [mock.call('cpus')]

    assert batch.writer.send.call_count == 2


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('clusterman.batch.cluster_metrics_collector.ClusterMetricsCollector.running', new_callable=mock.PropertyMock)
def test_run(mock_running, mock_time, mock_sleep, batch):
    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 113, 148]

    batch.run_interval = 10
    with mock.patch.object(batch, 'write_metrics', autospec=True) as write_prices:
        batch.run()
        assert write_prices.call_count == 3
    assert mock_sleep.call_args_list == [mock.call(9), mock.call(7), mock.call(2)]
