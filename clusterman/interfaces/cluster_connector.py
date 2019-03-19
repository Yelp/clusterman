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
        pass

    @abstractmethod
    def get_agent_metadata(self, instance_ip: Optional[str]) -> AgentMetadata:
        pass

    @abstractmethod
    def get_resource_allocation(self, resource_name: str) -> float:
        pass

    @abstractmethod
    def get_resource_total(self, resource_name: str) -> float:
        pass

    @abstractmethod
    def get_percent_resource_allocation(self, resource_name: str) -> float:
        pass
