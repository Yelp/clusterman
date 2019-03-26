import enum
from abc import ABCMeta
from abc import abstractmethod
from typing import NamedTuple
from typing import Optional

from clusterman.util import ClustermanResources


class AgentState(enum.Enum):
    IDLE = 'idle'
    ORPHANED = 'orphaned'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


class AgentMetadata(NamedTuple):
    agent_id: str
    allocated_resources: ClustermanResources
    batch_task_count: int
    state: AgentState
    task_count: int
    total_resources: ClustermanResources


class ClusterConnector(metaclass=ABCMeta):

    def __init__(self, cluster: str, pool: str) -> None:
        pass

    @abstractmethod
    def reload_state(self) -> None:
        """ Refresh any state that needs to be stored at the start of an autoscaling run """
        pass

    def get_agent_metadata(self, ip_address: Optional[str]) -> AgentMetadata:
        """ Get metadata about a cluster agent given an IP address

        :param ip_address: the IP address of the agent in question; it's possible for this IP value to
            be None, which will return an object with UNKNOWN state.
        :returns: whatever information the cluster connector can determine about the state of the agent
        """
        if not ip_address:
            return AgentMetadata(
                agent_id='',
                allocated_resources=ClustermanResources(0, 0, 0),
                batch_task_count=0,
                state=AgentState.UNKNOWN,
                task_count=0,
                total_resources=ClustermanResources(0, 0, 0),
            )
        else:
            return self._get_agent_metadata(ip_address)

    @abstractmethod
    def get_resource_allocation(self, resource_name: str) -> float:
        """Get the total amount of the given resource currently allocated for this pool.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the allocated resources in the cluster for the specified resource
        """
        pass

    @abstractmethod
    def get_resource_total(self, resource_name: str) -> float:
        """Get the total amount of the given resource for this pool.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the total resources in the cluster for the specified resource
        """
        pass

    def get_percent_resource_allocation(self, resource_name: str) -> float:
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the percentage allocated for the specified resource
        """
        total = self.get_resource_total(resource_name)
        used = self.get_resource_allocation(resource_name)
        return used / total if total else 0

    @abstractmethod
    def _get_agent_metadata(self, ip_address: str) -> AgentMetadata:
        pass

    @staticmethod
    def load(cluster: str, pool: str) -> 'ClusterConnector':
        """ Load the cluster connector for the given cluster and pool """
        # TODO (CLUSTERMAN-377) read config and identify the type of cluster connector
        from clusterman.mesos.mesos_cluster_connector import MesosClusterConnector
        return MesosClusterConnector(cluster, pool)
