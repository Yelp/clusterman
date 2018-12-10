import enum
import os
import re
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Type

import arrow
import colorlog
import requests
import staticconf
from mypy_extensions import TypedDict
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.aws.markets import InstanceMarket
from clusterman.config import get_cluster_config_directory
from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup

logger = colorlog.getLogger(__name__)
MesosResources = NamedTuple('MesosResources', [('cpus', float), ('mem', float), ('disk', float)])
MesosAgentDict = TypedDict(
    'MesosAgentDict',
    {
        'id': str,
        'used_resources': dict,
        'resources': dict,
        'hostname': str,
    },
)
RESOURCE_GROUPS: Mapping[
    str,
    Type[MesosPoolResourceGroup]
] = {
    'sfr': SpotFleetResourceGroup,
    'asg': AutoScalingResourceGroup,
}
RESOURCE_GROUPS_REV: Mapping[
    Type[MesosPoolResourceGroup],
    str
] = {v: k for k, v in RESOURCE_GROUPS.items()}


class MesosAgentState(enum.Enum):
    IDLE = 'idle'
    ORPHANED = 'orphaned'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


class InstanceMetadata(NamedTuple):
    allocated_resources: MesosResources
    aws_state: str
    group_id: str
    hostname: str
    instance_id: str
    instance_ip: Optional[str]
    is_resource_group_stale: bool
    is_usable: bool
    market: InstanceMarket
    mesos_state: MesosAgentState
    batch_task_count: int
    task_count: int
    total_resources: MesosResources
    uptime: arrow.Arrow
    weight: float


def agent_pid_to_ip(slave_pid):
    """Convert the agent PID from Mesos into an IP address

    :param: agent pid (this is in the format 'slave(1)@10.40.31.172:5051')
    :returns: ip address
    """
    regex = re.compile(r'.+?@([\d\.]+):\d+')
    return regex.match(slave_pid).group(1)


def has_usable_resources(
    agent: Optional[MesosAgentDict],
    usable_resource_threshold: float
) -> bool:
    """Determine if the agent is usable, e.g. if all of its resources have some spare capacity

    :param agent: the Mesos agent under question
    :param usable_resource_threshold: threshold under which a particular resource is considered 'usable'
    :returns: True if all percent utilized resources on the agent are under the threshold, False otherwise
    """
    if not agent:
        return False

    allocated_resources = allocated_agent_resources(agent)
    total_resources = total_agent_resources(agent)
    return all(
        getattr(allocated_resources, res) / getattr(total_resources, res) < usable_resource_threshold
        for res in MesosResources._fields
    )


def get_resource_value(resources, resource_name):
    """Helper to get the value of the given resource, from a list of resources returned by Mesos."""
    return resources.get(resource_name, 0)


def allocated_agent_resources(agent: Optional[MesosAgentDict]) -> MesosResources:
    return MesosResources(
        get_resource_value(agent.get('used_resources', {}), 'cpus'),
        get_resource_value(agent.get('used_resources', {}), 'mem'),
        get_resource_value(agent.get('used_resources', {}), 'disk'),
    ) if agent else MesosResources(0, 0, 0)


def total_agent_resources(agent: Optional[MesosAgentDict]) -> MesosResources:
    return MesosResources(
        get_resource_value(agent.get('resources', {}), 'cpus'),
        get_resource_value(agent.get('resources', {}), 'mem'),
        get_resource_value(agent.get('resources', {}), 'disk'),
    ) if agent else MesosResources(0, 0, 0)


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
        raise MesosPoolManagerError(f'Mesos master unreachable: check the logs for details') from e

    return response


def get_cluster_name_list(config_namespace=DEFAULT_NAMESPACE):
    namespace = staticconf.config.get_namespace(config_namespace)
    return namespace.get_config_dict().get('mesos_clusters', {}).keys()


def get_pool_name_list(cluster_name):
    cluster_config_directory = get_cluster_config_directory(cluster_name)
    return [
        f[:-5] for f in os.listdir(cluster_config_directory)
        if f[0] != '.' and f[-5:] == '.yaml'  # skip dotfiles and only read yaml-files
    ]
