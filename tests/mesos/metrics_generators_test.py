import mock
import pytest

from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.metrics_generators import ClusterMetric
from clusterman.mesos.metrics_generators import generate_framework_metadata
from clusterman.mesos.metrics_generators import generate_simple_metadata
from clusterman.mesos.metrics_generators import generate_system_metrics


@pytest.fixture
def mock_pool_manager():
    return mock.Mock(spec=MesosPoolManager)


def test_generate_system_metrics(mock_pool_manager):
    resources_allocated = {'cpus': 10, 'mem': 1000, 'disk': 10000}
    mock_pool_manager.get_resource_allocation.side_effect = resources_allocated.get

    expected_metrics = [
        ClusterMetric(metric_name='cpus_allocated', value=10, dimensions={}),
        ClusterMetric(metric_name='mem_allocated', value=1000, dimensions={}),
        ClusterMetric(metric_name='disk_allocated', value=10000, dimensions={}),
    ]
    assert sorted(generate_system_metrics(mock_pool_manager)) == sorted(expected_metrics)


def test_generate_simple_metadata(mock_pool_manager):
    resource_totals = {'cpus': 20, 'mem': 2000, 'disk': 20000}
    mock_pool_manager.get_resource_total.side_effect = lambda n, only_if_usable=False: resource_totals.get(n)

    market_capacities = {'market1': 15, 'market2': 25}
    mock_pool_manager.get_market_capacities.return_value = market_capacities

    expected_metrics = [
        ClusterMetric(metric_name='cpus_total', value=20, dimensions={}),
        ClusterMetric(metric_name='mem_total', value=2000, dimensions={}),
        ClusterMetric(metric_name='disk_total', value=20000, dimensions={}),
        ClusterMetric(metric_name='cpus_total_usable', value=20, dimensions={}),
        ClusterMetric(metric_name='mem_total_usable', value=2000, dimensions={}),
        ClusterMetric(metric_name='disk_total_usable', value=20000, dimensions={}),
        ClusterMetric(metric_name='target_capacity', value=mock_pool_manager.target_capacity, dimensions={}),
        ClusterMetric(metric_name='fulfilled_capacity', value=market_capacities, dimensions={}),
    ]
    assert sorted(generate_simple_metadata(mock_pool_manager)) == sorted(expected_metrics)


def test_generate_framework_metadata(mock_pool_manager):
    mock_pool_manager.frameworks = {
        'frameworks': [{
            'id': 'framework_1',
            'name': 'active',
            'active': True,
            'used_resources': {'cpus': 1, 'mem': 2, 'gpus': 3, 'disk': 4},
            'registered_time': 1111,
            'unregistered_time': 0,
            'tasks': [{'state': 'TASK_RUNNING'}, {'state': 'TASK_FINISHED'}],
        }],
        'completed_frameworks': [{
            'id': 'framework_2',
            'name': 'completed',
            'active': False,
            'used_resources': {'cpus': 0, 'mem': 0, 'gpus': 0, 'disk': 0},
            'registered_time': 123,
            'unregistered_time': 456,
            'tasks': [{'state': 'TASK_FINISHED'}, {'state': 'TASK_FAILED'}]
        }]
    }
    expected_metrics = [
        ClusterMetric(
            metric_name='framework',
            value={
                'cpus': 1, 'mem': 2, 'gpus': 3, 'disk': 4, 'registered_time': 1111, 'unregistered_time': 0,
                'running_task_count': 1
            },
            dimensions={'name': 'active', 'id': 'framework_1', 'active': True, 'completed': False},
        ),
        ClusterMetric(
            metric_name='framework',
            value={
                'cpus': 0, 'mem': 0, 'gpus': 0, 'disk': 0, 'registered_time': 123, 'unregistered_time': 456,
                'running_task_count': 0
            },
            dimensions={'name': 'completed', 'id': 'framework_2', 'active': False, 'completed': True},
        )
    ]
    sorted_expected_metrics = sorted(expected_metrics, key=lambda x: x.dimensions['id'])
    actual_metrics = generate_framework_metadata(mock_pool_manager)
    sorted_actual_metrics = sorted(actual_metrics, key=lambda x: x.dimensions['id'])
    assert sorted_actual_metrics == sorted_expected_metrics
