import argparse

import mock
import pytest
import staticconf.testing
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


@pytest.fixture
def mock_setup_config():
    with mock.patch('clusterman.batch.cluster_metrics_collector.setup_config', autospec=True) as mock_setup:
        with staticconf.testing.PatchConfiguration(
            {'cluster_roles': ['role-1', 'role-3']},
        ):
            yield mock_setup


@mock.patch('clusterman.batch.cluster_metrics_collector.ClustermanMetricsBotoClient', autospec=True)
@mock.patch('clusterman.batch.cluster_metrics_collector.MesosRoleManager', autospec=True)
def test_configure_initial(mock_mesos_role_manager, mock_client_class, batch, mock_setup_config):
    batch.configure_initial()

    assert batch.run_interval == 120
    assert mock_setup_config.call_count == 1
    assert batch.region == 'us-west-2'  # region from cluster configs
    assert sorted(batch.mesos_managers.keys()) == ['role-1', 'role-3']
    for manager in batch.mesos_managers.values():
        assert isinstance(manager, MesosRoleManager)

    assert mock_client_class.call_args_list == [mock.call(region_name='us-west-2')]
    assert batch.metrics_client == mock_client_class.return_value


def test_write_metrics(batch):
    batch.mesos_managers = {
        'role_A': mock.Mock(spec_set=MesosRoleManager),
        'role_B': mock.Mock(spec_set=MesosRoleManager),
    }
    writer = mock.Mock()
    batch.write_metrics(writer)

    for role, manager in batch.mesos_managers.items():
        assert manager.get_average_resource_allocation.call_args_list == [mock.call('cpus')]
        assert manager.get_resource_allocation.call_args_list == [mock.call('cpus')]

    assert writer.send.call_count == 4

    metric_names = [call[0][0][0] for call in writer.send.call_args_list]
    assert sorted(metric_names) == sorted([
        'cpu_allocation|cluster=mesos-test,role=role_A',
        'cpu_allocation|cluster=mesos-test,role=role_B',
        'cpu_allocation_percent|cluster=mesos-test,role=role_A',
        'cpu_allocation_percent|cluster=mesos-test,role=role_B',
    ])


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('clusterman.batch.cluster_metrics_collector.ClusterMetricsCollector.running', new_callable=mock.PropertyMock)
def test_run(mock_running, mock_time, mock_sleep, batch):
    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 113, 148]
    batch.run_interval = 10
    batch.metrics_client = mock.MagicMock()

    writer_context = batch.metrics_client.get_writer.return_value
    writer = writer_context.__enter__.return_value

    with mock.patch.object(batch, 'write_metrics', autospec=True) as write_metrics:
        batch.run()

        # Writing should have happened 3 times.
        # Each time, we create a new writer context and call write_metrics.
        assert batch.metrics_client.get_writer.call_args_list == [mock.call(SYSTEM_METRICS) for i in range(3)]
        assert write_metrics.call_args_list == [mock.call(writer) for i in range(3)]
        assert writer_context.__exit__.call_count == 3

    assert mock_sleep.call_args_list == [mock.call(9), mock.call(7), mock.call(2)]
