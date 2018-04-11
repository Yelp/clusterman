import argparse
import socket

import mock
import pytest
from clusterman_metrics import ClustermanMetricsBotoClient

from clusterman.batch.cluster_metrics_collector import ClusterMetricsCollector
from clusterman.batch.cluster_metrics_collector import METRICS_TO_WRITE
from clusterman.mesos.mesos_pool_manager import MesosPoolManager


@pytest.fixture
def batch(args=None):
    batch = ClusterMetricsCollector()
    args = args or ['--cluster', 'mesos-test']
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    batch.options = parser.parse_args(args)
    batch.options.instance_name = 'foo'
    batch.version_checker = mock.Mock(watchers=[])
    return batch


@pytest.fixture
def mock_setup_config():
    with mock.patch('clusterman.batch.cluster_metrics_collector.setup_config', autospec=True) as mock_setup:
        yield mock_setup


@mock.patch('clusterman.batch.cluster_metrics_collector.ClustermanMetricsBotoClient', autospec=True)
@mock.patch('clusterman.batch.cluster_metrics_collector.MesosPoolManager', autospec=True)
@mock.patch('os.listdir')
def test_configure_initial(mock_ls, mock_mesos_pool_manager, mock_client_class, batch, mock_setup_config):
    pools = ['pool-1', 'pool-3']
    mock_ls.return_value = [f'{p}.yaml' for p in pools]
    batch.configure_initial()

    assert batch.run_interval == 120
    assert mock_setup_config.call_count == 1
    assert batch.region == 'us-west-2'  # region from cluster configs
    assert sorted(batch.mesos_managers.keys()) == pools
    for manager in batch.mesos_managers.values():
        assert isinstance(manager, MesosPoolManager)

    assert mock_client_class.call_args_list == [mock.call(region_name='us-west-2')]
    assert batch.metrics_client == mock_client_class.return_value


def test_write_metrics(batch):
    batch.mesos_managers = {
        'pool_A': mock.Mock(spec_set=MesosPoolManager),
        'pool_B': mock.Mock(spec_set=MesosPoolManager),
    }
    writer = mock.Mock()
    metrics_to_write = [
        ('total', lambda manager: manager.get_resource_total('cpus')),
        ('allocated', lambda manager: manager.get_resource_allocation('cpus')),
    ]
    batch.write_metrics(writer, metrics_to_write)

    for pool, manager in batch.mesos_managers.items():
        assert manager.get_resource_total.call_args_list == [mock.call('cpus')]
        assert manager.get_resource_allocation.call_args_list == [mock.call('cpus')]

    assert writer.send.call_count == 4

    metric_names = [call[0][0][0] for call in writer.send.call_args_list]
    assert sorted(metric_names) == sorted([
        'total|cluster=mesos-test,pool=pool_A',
        'total|cluster=mesos-test,pool=pool_B',
        'allocated|cluster=mesos-test,pool=pool_A',
        'allocated|cluster=mesos-test,pool=pool_B',
    ])


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('clusterman.batch.cluster_metrics_collector.ClusterMetricsCollector.running', new_callable=mock.PropertyMock)
@mock.patch('clusterman.batch.cluster_metrics_collector.sensu_checkin', autospec=True)
def test_run(mock_sensu, mock_running, mock_time, mock_sleep, batch):
    mock_running.side_effect = [True, True, True, True, False]
    mock_time.side_effect = [101, 113, 148, 188]
    batch.run_interval = 10
    batch.metrics_client = mock.MagicMock(spec_set=ClustermanMetricsBotoClient)

    writer_context = batch.metrics_client.get_writer.return_value
    writer = writer_context.__enter__.return_value

    with mock.patch('builtins.hash') as mock_hash, \
            mock.patch.object(batch, 'write_metrics', autospec=True) as write_metrics, \
            mock.patch('clusterman.batch.cluster_metrics_collector.logger') as mock_logger:
        def mock_write_metrics(end_time, writer):
            if mock_time.call_count == 4:
                raise socket.timeout('timed out')
            else:
                return

        mock_hash.return_value = 0  # patch hash to avoid splaying
        write_metrics.side_effect = mock_write_metrics
        batch.run()

        # Writing should have happened 3 times, for each metric type.
        # Each time, we create a new writer context and call write_metrics.
        assert sorted(batch.metrics_client.get_writer.call_args_list) == sorted(
            [mock.call(metric_type) for metric_type in METRICS_TO_WRITE] * 4
        )
        assert sorted(write_metrics.call_args_list) == sorted(
            [mock.call(writer, metrics) for metrics in METRICS_TO_WRITE.values()] * 4
        )
        assert writer_context.__exit__.call_count == len(METRICS_TO_WRITE) * 4
        assert mock_sensu.call_count == 3
        assert mock_logger.warn.call_count == 2

    assert mock_sleep.call_args_list == [mock.call(9), mock.call(7), mock.call(2), mock.call(2)]
