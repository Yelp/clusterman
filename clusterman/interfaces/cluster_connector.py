import enum
from abc import ABCMeta
from abc import abstractmethod
from typing import NamedTuple
from typing import Optional


class ClustermanResources(NamedTuple):
    cpus: float = 0
    mem: float = 0
    disk: float = 0


class AgentState(enum.Enum):
    IDLE = 'idle'
    ORPHANED = 'orphaned'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


class Agent(NamedTuple):
    agent_id: str = ''
    allocated_resources: ClustermanResources = ClustermanResources()
    batch_task_count: int = 0
    state: AgentState = AgentState.UNKNOWN
    task_count: int = 0
    total_resources: ClustermanResources = ClustermanResources()


class ClusterConnector(metaclass=ABCMeta):

    def __init__(self, cluster: str, pool: str) -> None:
        pass

    @abstractmethod
    def reload_state(self) -> None:
        pass

    @abstractmethod
    def get_agent_by_ip(self, instance_ip: Optional[str]) -> Agent:
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
