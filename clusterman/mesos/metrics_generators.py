from collections import namedtuple

ClusterMetric = namedtuple('ClusterMetric', ['metric_name', 'value', 'dimensions'])

SYSTEM_METRIC_TYPES = {
    'cpus_allocated': lambda manager: manager.get_resource_allocation('cpus'),
    'mem_allocated': lambda manager: manager.get_resource_allocation('mem'),
    'disk_allocated': lambda manager: manager.get_resource_allocation('disk'),
}

SIMPLE_METADATA_TYPES = {
    'cpus_total': lambda manager: manager.get_resource_total('cpus'),
    'mem_total': lambda manager: manager.get_resource_total('mem'),
    'disk_total': lambda manager: manager.get_resource_total('disk'),
    'cpus_total_usable': lambda manager: manager.get_resource_total('cpus', only_if_usable=True),
    'mem_total_usable': lambda manager: manager.get_resource_total('mem', only_if_usable=True),
    'disk_total_usable': lambda manager: manager.get_resource_total('disk', only_if_usable=True),
    'target_capacity': lambda manager: manager.target_capacity,
    'fulfilled_capacity': lambda manager: {str(market): value for market,
                                           value in manager.get_market_capacities().items()},
}


def generate_system_metrics(manager):
    for metric_name, value_method in SYSTEM_METRIC_TYPES.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions={})


def generate_simple_metadata(manager):
    for metric_name, value_method in SIMPLE_METADATA_TYPES.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions={})


def _prune_resources_dict(resources_dict):
    return {resource: resources_dict[resource] for resource in ('cpus', 'mem', 'disk', 'gpus')}


def _get_framework_metadata_for_frameworks(frameworks, completed):
    for framework in frameworks:
        value = _prune_resources_dict(framework['used_resources'])
        value['registered_time'] = int(framework['registered_time'])
        value['unregistered_time'] = int(framework['unregistered_time'])
        value['running_task_count'] = len([
            task for task in framework['tasks'] if task['state'] == 'TASK_RUNNING'
        ])

        dimensions = {field: framework[field] for field in ('name', 'id', 'active')}
        dimensions['completed'] = completed

        yield ClusterMetric(metric_name='framework', value=value, dimensions=dimensions)


def generate_framework_metadata(manager):
    yield from _get_framework_metadata_for_frameworks(manager.frameworks['frameworks'], completed=False)
    yield from _get_framework_metadata_for_frameworks(manager.frameworks['completed_frameworks'], completed=True)
