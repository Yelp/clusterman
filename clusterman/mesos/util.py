import random
import socket

import requests

from clusterman.exceptions import MesosRoleManagerError
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


class MesosAgentState:
    IDLE = 'no tasks'
    ORPHANED = 'orphan'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


def get_agent_by_ip(ip, mesos_agents):
    try:
        return next(agent for agent in mesos_agents if socket.gethostbyname(agent['hostname']) == ip)
    except StopIteration:
        return None


def get_mesos_state(instance, mesos_agents):
    try:
        instance_ip = instance['PrivateIpAddress']
    except KeyError:
        return MesosAgentState.UNKNOWN
    else:
        agent = get_agent_by_ip(instance_ip, mesos_agents)
        if not agent:
            return MesosAgentState.ORPHANED
        elif allocated_cpu_resources(agent) == 0:
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
    master_url = url if endpoint == 'redirect' else mesos_post(url, 'redirect').url + '/'
    request_url = master_url + endpoint
    response = None
    try:
        response = requests.post(
            request_url,
            headers={'user-agent': 'clusterman'},
        )
        response.raise_for_status()
    except Exception as e:  # there's no one exception class to check for problems with the request :(
        log_message = (
            f'Mesos is unreachable:\n\n'
            f'{str(e)}\n'
            f'Querying Mesos URL: {request_url}\n'
        )
        if response is not None:
            log_message += (
                f'Response Code: {response.status_code}\n'
                f'Response Text: {response.text}\n'
            )
        logger.critical(log_message)
        raise MesosRoleManagerError(f'Mesos master unreachable: check the logs for details') from e

    return response
