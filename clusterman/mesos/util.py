import os
import re

import colorlog
import requests
import staticconf
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.config import get_cluster_config_directory
from clusterman.exceptions import PoolManagerError
from clusterman.interfaces.cluster_connector import ClustermanResources
from clusterman.mesos.mesos_cluster_connector import MesosAgentDict

logger = colorlog.getLogger(__name__)


def agent_pid_to_ip(agent_pid: str) -> str:
    """Convert the agent PID from Mesos into an IP address

    :param: agent pid (this is in the format 'slave(1)@10.40.31.172:5051')
    :returns: ip address
    """
    m = re.match(r'.+?@([\d\.]+):\d+', agent_pid)
    assert m
    return m.group(1)


def allocated_agent_resources(agent_dict: MesosAgentDict) -> ClustermanResources:
    used_resources = agent_dict.get('used_resources', {})
    return ClustermanResources(
        cpus=used_resources.get('cpus', 0),
        mem=used_resources.get('mem', 0),
        disk=used_resources.get('disk', 0),
    )


def get_cluster_name_list(config_namespace=DEFAULT_NAMESPACE):
    namespace = staticconf.config.get_namespace(config_namespace)
    return namespace.get_config_dict().get('mesos_clusters', {}).keys()


def get_pool_name_list(cluster_name):
    cluster_config_directory = get_cluster_config_directory(cluster_name)
    return [
        f[:-5] for f in os.listdir(cluster_config_directory)
        if f[0] != '.' and f[-5:] == '.yaml'  # skip dotfiles and only read yaml-files
    ]


def mesos_post(url: str, endpoint: str) -> requests.Response:
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
        raise PoolManagerError(f'Mesos master unreachable: check the logs for details') from e

    return response


def total_agent_resources(agent: MesosAgentDict) -> ClustermanResources:
    resources = agent.get('resources', {})
    return ClustermanResources(
        cpus=resources.get('cpus', 0),
        mem=resources.get('mem', 0),
        disk=resources.get('disk', 0),
    )
