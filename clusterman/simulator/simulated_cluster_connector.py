import uuid
from typing import Optional

import staticconf

from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.simulator import simulator
from clusterman.util import ClustermanResources


class SimulatedClusterConnector(ClusterConnector):

    def __init__(self, cluster: str, pool: str, simulator: 'simulator.Simulator') -> None:
        self.cluster = cluster
        self.pool = pool
        self.simulator = simulator

    def reload_state(self) -> None:
        pass

    def get_agent_metadata(self, instance_ip: Optional[str]) -> AgentMetadata:
        for c in self.simulator.aws_clusters:
            for i in c.instances.values():
                if instance_ip == i.ip_address:
                    return AgentMetadata(
                        agent_id=str(uuid.uuid4()),
                        allocated_resources=ClustermanResources(0, 0, 0),
                        batch_task_count=0,
                        state=(
                            AgentState.ORPHANED
                            if self.simulator.current_time < i.join_time
                            else AgentState.IDLE
                        ),
                        task_count=0,
                        total_resources=ClustermanResources(
                            cpus=i.resources.cpus,
                            mem=i.resources.mem * 1000,
                            disk=(i.resources.disk or staticconf.read_int('ebs_volume_size', 0)) * 1000,
                        )
                    )

        return AgentMetadata(
            agent_id='',
            allocated_resources=ClustermanResources(0, 0, 0),
            batch_task_count=0,
            state=AgentState.UNKNOWN,
            task_count=0,
            total_resources=ClustermanResources(0, 0, 0),
        )

    def get_resource_allocation(self, resource_name: str) -> float:
        return 0

    def get_resource_total(self, resource_name: str) -> float:
        total = 0
        for c in self.simulator.aws_clusters:
            for i in c.instances.values():
                if self.simulator.current_time < i.join_time:
                    continue

                total += getattr(i.resources, resource_name)
        return total

    def get_percent_resource_allocation(self, resource_name: str) -> float:
        return 0
