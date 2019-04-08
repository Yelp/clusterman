import enum
from collections import defaultdict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Sequence
from typing import Tuple

import colorlog
import staticconf
from mypy_extensions import TypedDict

from clusterman.config import POOL_NAMESPACE
from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_agent_resources
from clusterman.mesos.util import mesos_post
from clusterman.mesos.util import MesosAgentDict
from clusterman.mesos.util import MesosAgents
from clusterman.mesos.util import MesosFrameworkDict
from clusterman.mesos.util import MesosFrameworks
from clusterman.mesos.util import MesosTaskDict
from clusterman.mesos.util import total_agent_resources
from clusterman.util import ClustermanResources

logger = colorlog.getLogger(__name__)


class TaskCount(TypedDict):
    all_tasks: int
    batch_tasks: int


class FrameworkState(enum.Enum):
    RUNNING = 'frameworks'
    COMPLETED = 'completed_frameworks'


class MesosClusterConnector(ClusterConnector):

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

        # Note that order matters here: we can't map tasks to agents until we've calculated
        # all of the tasks and agents
        self._agents_by_ip = self._get_agents_by_ip()
        self._tasks, self._frameworks, self._completed_frameworks = self._get_tasks_and_frameworks()
        self._task_count_per_agent = self._count_tasks_per_agent()

    def get_resource_allocation(self, resource_name: str) -> float:
        return sum(
            getattr(allocated_agent_resources(agent), resource_name)
            for agent in self._agents_by_ip.values()
        )

    def get_resource_total(self, resource_name: str) -> float:
        return sum(
            getattr(total_agent_resources(agent), resource_name)
            for agent in self._agents_by_ip.values()
        )

    def get_framework_list(self, framework_state: FrameworkState) -> Sequence[MesosFrameworkDict]:
        return [
            framework for framework in (
                self._frameworks.values()
                if framework_state == FrameworkState.RUNNING
                else self._completed_frameworks.values()
            )
        ]

    def _get_agent_metadata(self, instance_ip: str) -> AgentMetadata:
        agent_dict = self._agents_by_ip.get(instance_ip)
        if not agent_dict:
            return AgentMetadata(
                agent_id='',
                allocated_resources=ClustermanResources(0, 0, 0),
                batch_task_count=0,
                state=AgentState.ORPHANED,
                task_count=0,
                total_resources=ClustermanResources(0, 0, 0),
            )

        allocated_resources = allocated_agent_resources(agent_dict)
        return AgentMetadata(
            agent_id=agent_dict['id'],
            allocated_resources=allocated_agent_resources(agent_dict),
            batch_task_count=self._task_count_per_agent[agent_dict['id']]['batch_tasks'],
            state=(AgentState.RUNNING if any(allocated_resources) else AgentState.IDLE),
            task_count=self._task_count_per_agent[agent_dict['id']]['all_tasks'],
            total_resources=total_agent_resources(agent_dict),
        )

    def _count_tasks_per_agent(self) -> Mapping[str, TaskCount]:
        """Given a list of mesos tasks, return a count of tasks per agent"""
        instance_id_to_task_count: MutableMapping[str, TaskCount] = defaultdict(
            lambda: TaskCount(all_tasks=0, batch_tasks=0),
        )

        for task in self._tasks:
            if task['state'] == 'TASK_RUNNING':
                instance_id_to_task_count[task['slave_id']]['all_tasks'] += 1
                framework_name = self._frameworks[task['framework_id']]['name']
                if self._is_batch_framework(framework_name):
                    instance_id_to_task_count[task['slave_id']]['batch_tasks'] += 1
        return instance_id_to_task_count

    def _get_agents_by_ip(self) -> Mapping[str, MesosAgentDict]:
        response: MesosAgents = mesos_post(self.api_endpoint, 'slaves').json()
        return {
            agent_pid_to_ip(agent_dict['pid']): agent_dict
            for agent_dict in response['slaves']
            if agent_dict.get('attributes', {}).get('pool', 'default') == self.pool
        }

    def _get_tasks_and_frameworks(
        self
    ) -> Tuple[Sequence[MesosTaskDict], Mapping[str, MesosFrameworkDict], Mapping[str, MesosFrameworkDict]]:

        response: MesosFrameworks = mesos_post(self.api_endpoint, 'master/frameworks').json()
        running_frameworks = {
            framework['id']: framework
            for framework in response['frameworks']
        }
        completed_frameworks = {
            framework['id']: framework
            for framework in response['completed_frameworks']
        }

        tasks: List[MesosTaskDict] = []
        for framework in running_frameworks.values():
            tasks.extend(framework['tasks'])

        return tasks, running_frameworks, completed_frameworks

    def _is_batch_framework(self, framework_name: str) -> bool:
        """If the framework matches any of the prefixes in self.non_batch_framework_prefixes
        this will return False, otherwise we assume the task to be a batch task"""
        return not any([framework_name.startswith(prefix) for prefix in self.non_batch_framework_prefixes])
