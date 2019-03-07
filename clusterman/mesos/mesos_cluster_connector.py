from collections import defaultdict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Sequence

import colorlog
import staticconf

from clusterman.config import POOL_NAMESPACE
from clusterman.interfaces.cluster_connector import Agent
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.interfaces.cluster_connector import ClustermanResources
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_agent_resources
from clusterman.mesos.util import mesos_post
from clusterman.mesos.util import MesosAgentDict
from clusterman.mesos.util import MesosAgents
from clusterman.mesos.util import MesosFrameworkDict
from clusterman.mesos.util import MesosFrameworks
from clusterman.mesos.util import MesosTaskDict
from clusterman.mesos.util import total_agent_resources

logger = colorlog.getLogger(__name__)


class MesosClusterConnector(ClusterConnector):

    _agents: Mapping[str, MesosAgentDict]
    _frameworks: Mapping[str, MesosFrameworkDict]
    _tasks: Sequence[MesosTaskDict]
    _task_count_per_agent: Mapping[str, int]
    _batch_task_count_per_agent: Mapping[str, int]

    def __init__(self, cluster: str, pool: str) -> None:
        self.cluster = cluster
        self.pool = pool
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool))

        mesos_master_fqdn = staticconf.read_string(f'mesos_clusters.{self.cluster}.fqdn')
        self.non_batch_framework_prefixes = self.pool_config.read_list(
            'non_batch_framework_prefixes',
            default=['marathon'],
        )
        self.api_endpoint = f'http://{mesos_master_fqdn}:5050/'
        logger.info(f'Connecting to Mesos masters at {self.api_endpoint}')

    def reload_state(self) -> None:
        self._agents = self._get_agents()
        self._frameworks = self._get_frameworks()
        self._tasks = self._get_tasks()
        self._task_count_per_agent = self._count_tasks_per_agent()
        self._batch_task_count_per_agent = self._count_batch_tasks_per_agent()

    def get_agent_by_ip(self, instance_ip: Optional[str]) -> Agent:
        if not instance_ip:
            return Agent(
                '',
                AgentState.UNKNOWN,
                ClustermanResources(0, 0, 0),
                0,
                0,
                ClustermanResources(0, 0, 0),
            )

        agent_dict = self._agents.get(instance_ip)
        if not agent_dict:
            return Agent(
                '',
                AgentState.ORPHANED,
                ClustermanResources(0, 0, 0),
                0,
                0,
                ClustermanResources(0, 0, 0),
            )

        allocated_resources = allocated_agent_resources(agent_dict)
        return Agent(
            agent_id=agent_dict['id'],
            allocated_resources=allocated_agent_resources(agent_dict),
            batch_task_count=self._batch_task_count_per_agent[agent_dict['id']],
            agent_state=(AgentState.RUNNING if any(allocated_resources) else AgentState.IDLE),
            task_count=self._task_count_per_agent[agent_dict['id']],
            total_resources=total_agent_resources(agent_dict),
        )

    def get_resource_allocation(self, resource_name: str) -> float:
        """Get the total amount of the given resource currently allocated for this Mesos pool.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: the allocated resources in the Mesos cluster for the specified resource
        """
        return sum(
            getattr(allocated_agent_resources(agent), resource_name)
            for agent in self._agents.values()
        )

    def get_resource_total(self, resource_name: str) -> float:
        """Get the total amount of the given resource for this Mesos pool.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: the total resources in the Mesos cluster for the specified resource
        """
        return sum(
            getattr(total_agent_resources(agent), resource_name)
            for agent in self._agents.values()
        )

    def get_percent_resource_allocation(self, resource_name: str) -> float:
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: the percentage allocated for the specified resource
        """
        total = self.get_resource_total(resource_name)
        used = self.get_resource_allocation(resource_name)
        return used / total if total else 0

    def _count_tasks_per_agent(self) -> Mapping[str, int]:
        """Given a list of mesos tasks, return a count of tasks per agent"""
        instance_id_to_task_count: MutableMapping[str, int] = defaultdict(int)
        for task in self._tasks:
            if task['state'] == 'TASK_RUNNING':
                instance_id_to_task_count[task['slave_id']] += 1
        return instance_id_to_task_count

    def _count_batch_tasks_per_agent(self) -> MutableMapping[str, int]:
        """Given a list of mesos tasks, return a count of tasks per agent
        filtered by frameworks that we consider to be used for batch tasks
        which we prefer not to interrupt"""
        instance_id_to_task_count: MutableMapping[str, int] = defaultdict(int)
        for task in self._tasks:
            framework_name = self._frameworks[task['framework_id']]['name']
            if task['state'] == 'TASK_RUNNING' and self._is_batch_framework(framework_name):
                instance_id_to_task_count[task['slave_id']] += 1
        return instance_id_to_task_count

    def _get_agents(self) -> Mapping[str, MesosAgentDict]:
        response: MesosAgents = mesos_post(self.api_endpoint, 'slaves').json()
        return {
            agent_pid_to_ip(agent_dict['pid']): agent_dict
            for agent_dict in response['slaves']
            if agent_dict.get('attributes', {}).get('pool', 'default') == self.pool
        }

    def _get_frameworks(self) -> Mapping[str, MesosFrameworkDict]:
        response: MesosFrameworks = mesos_post(self.api_endpoint, 'master/frameworks').json()
        return {
            framework['id']: framework
            for framework in response['frameworks']
        }

    def _get_tasks(self) -> Sequence[MesosTaskDict]:
        tasks: List[MesosTaskDict] = []
        for framework in self._frameworks.values():
            tasks.extend(framework['tasks'])
        return tasks

    def _is_batch_framework(self, framework_name: str) -> bool:
        """If the framework matches any of the prefixes in self.non_batch_framework_prefixes
        this will return False, otherwise we assume the task to be a batch task"""
        return not any([framework_name.startswith(prefix) for prefix in self.non_batch_framework_prefixes])
