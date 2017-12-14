import random

import requests

from clusterman.exceptions import MesosRoleManagerError


class MesosAgentState:
    IDLE = 'no tasks'
    ORPHANED = 'orphan'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


def get_mesos_state(instance, mesos_agents):
    try:
        instance_ip = instance['PrivateIpAddress']
    except KeyError:
        return MesosAgentState.UNKNOWN
    else:
        if instance_ip not in mesos_agents:
            return MesosAgentState.ORPHANED
        elif allocated_cpu_resources(mesos_agents[instance_ip]) == 0:
            return MesosAgentState.IDLE
        else:
            return MesosAgentState.RUNNING


def get_resource_value(resources, resource_name):
    """Helper to get the value of the given resource, from a list of resources returned by Mesos."""
    return resources.get(resource_name, 0)


def get_total_resource_value(agents, value_name, resource_name):
    """
    Get the total value of a resource type from the list of agents.

    :param agents: list of agents from Mesos
    :param value_name: desired resource value (e.g. total_resources, allocated_resources)
    :param resource_name: name of resource recognized by Mesos (e.g. cpus, memory, disk)
    """
    return sum(
        get_resource_value(agent.get(value_name, {}), resource_name)
        for agent in agents
    )


def allocated_cpu_resources(agent):
    return get_resource_value(agent.get('used_resources', {}), 'cpus')


def find_largest_capacity_market(markets):
    try:
        return max(
            ((m, c) for m, c in markets.items()),
            key=lambda mc: (mc[1], random.random()),
        )
    except ValueError:
        return None, 0


def mesos_post(url, endpoint):
    response = requests.post(
        url + endpoint,
        headers={'user-agent': 'clusterman'},
    )
    if not response.ok:
        raise MesosRoleManagerError(f'Could not read from Mesos master:\n{response.text}')
    return response.json()
