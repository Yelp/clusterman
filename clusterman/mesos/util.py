import random

from clusterman.exceptions import MesosRoleManagerError


def get_resource_value(resources, resource_name):
    """Helper to get the value of the given resource, from a list of resources returned by Mesos."""
    for resource in resources:
        if resource['name'] == resource_name:
            if resource['type'] != "SCALAR":
                raise MesosRoleManagerError('Only scalar resource types are supported.')
            return resource['scalar']['value']
    return 0


def allocated_cpu_resources(agent):
    return get_resource_value(
        agent.get('allocated_resources', []),
        'cpus'
    )


def find_largest_capacity_market(markets):
    try:
        return max(
            ((m, c) for m, c in markets.items()),
            key=lambda mc: (mc[1], random.random()),
        )
    except ValueError:
        return None, 0
