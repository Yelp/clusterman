from collections import namedtuple

ClusterMetric = namedtuple('ClusterMetric', ['metric_name', 'value', 'dimensions'])

SYSTEM_METRICS = {
    'cpus_allocated': lambda manager: manager.connector.get_resource_allocation('cpus'),
    'mem_allocated': lambda manager: manager.connector.get_resource_allocation('mem'),
    'disk_allocated': lambda manager: manager.connector.get_resource_allocation('disk'),
}

SIMPLE_METADATA = {
    'cpus_total': lambda manager: manager.connector.get_resource_total('cpus'),
    'mem_total': lambda manager: manager.connector.get_resource_total('mem'),
    'disk_total': lambda manager: manager.connector.get_resource_total('disk'),
    'target_capacity': lambda manager: manager.target_capacity,
    'fulfilled_capacity': lambda manager: {str(market): value for market,
                                           value in manager.get_market_capacities().items()},
    'non_orphan_fulfilled_capacity': lambda manager: manager.non_orphan_fulfilled_capacity,
}


def generate_system_metrics(manager):
    dimensions = {'cluster': manager.cluster, 'pool': manager.pool}
    for metric_name, value_method in SYSTEM_METRICS.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions=dimensions)


def generate_simple_metadata(manager):
    dimensions = {'cluster': manager.cluster, 'pool': manager.pool}
    for metric_name, value_method in SIMPLE_METADATA.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions=dimensions)


def _prune_resources_dict(resources_dict):
    return {resource: resources_dict[resource] for resource in ('cpus', 'mem', 'disk', 'gpus')}


def _get_framework_metadata_for_frameworks(cluster, frameworks, completed):
    for framework in frameworks:
        value = _prune_resources_dict(framework['used_resources'])
        value['registered_time'] = int(framework['registered_time'])
        value['unregistered_time'] = int(framework['unregistered_time'])
        value['running_task_count'] = len([
            task for task in framework['tasks'] if task['state'] == 'TASK_RUNNING'
        ])

        dimensions = {field: framework[field] for field in ('name', 'id', 'active')}
        dimensions['cluster'] = cluster
        dimensions['completed'] = completed

        yield ClusterMetric(metric_name='framework', value=value, dimensions=dimensions)


def generate_framework_metadata(manager):
    yield from _get_framework_metadata_for_frameworks(
        manager.cluster,
        manager.frameworks['frameworks'],
        completed=False,
    )
    yield from _get_framework_metadata_for_frameworks(
        manager.cluster,
        manager.frameworks['completed_frameworks'],
        completed=True,
    )
