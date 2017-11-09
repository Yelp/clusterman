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


def get_total_resource_value(agents, value_name, resource_name):
    """
    Get the total value of a resource type from the list of agents.

    :param agents: list of agents from Mesos
    :param value_name: desired resource value (e.g. total_resources, allocated_resources)
    :param resource_name: name of resource recognized by Mesos (e.g. cpus, memory, disk)
    """
    total = 0
    for agent in agents:
        total += get_resource_value(agent.get(value_name, []), resource_name)
    return total


def allocated_cpu_resources(agent):
    return get_resource_value(
        agent['agent_info'].get('allocated_resources', []),
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
