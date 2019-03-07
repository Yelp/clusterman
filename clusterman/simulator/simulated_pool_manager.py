import uuid
from typing import cast
from typing import Collection
from typing import Optional
from typing import Sequence

import staticconf

from clusterman.aws.aws_pool_manager import AWSPoolManager
from clusterman.config import POOL_NAMESPACE
from clusterman.interfaces.cluster_connector import Agent
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClustermanResources
from clusterman.interfaces.pool_manager import InstanceMetadata
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup
from clusterman.util import read_int_or_inf


def _make_agent(instance, current_time):
    return Agent(
        agent_id=uuid.uuid4(),
        state=(
            AgentState.ORPHANED
            if current_time < instance.join_time
            else AgentState.RUNNING
        ),
        total_resources=ClustermanResources(
            cpus=instance.resources.cpus,
            mem=instance.resources.mem * 1000,
            disk=(instance.resources.disk or staticconf.read_int('ebs_volume_size', 0)) * 1000,
        )
    )


class SimulatedPoolManager(AWSPoolManager):

    def __init__(self, cluster, pool, configs, simulator):
        self.draining_enabled = False
        self.cluster = cluster
        self.pool = pool
        self.simulator = simulator
        groups = [
            SimulatedSpotFleetResourceGroup(config, self.simulator)
            for config in configs
        ]
        self.resource_groups = {group.id: group for group in groups}
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool))
        self.min_capacity = self.pool_config.read_int('scaling_limits.min_capacity')
        self.max_capacity = self.pool_config.read_int('scaling_limits.max_capacity')
        self.max_tasks_to_kill = read_int_or_inf(self.pool_config, 'scaling_limits.max_tasks_to_kill')

    def reload_state(self) -> None:
        pass

    def get_instance_metadatas(self, aws_state_filter: Optional[Collection[str]] = None) -> Sequence[InstanceMetadata]:
        agent_metadatas = []
        for group in self.resource_groups.values():
            for instance in cast(SimulatedAWSCluster, group).instances.values():
                if aws_state_filter and 'running' not in aws_state_filter:
                    continue

                agent = self.connector.get_agent_by_ip(instance.ip_address)
                metadata = InstanceMetadata(
                    agent=agent,
                    group_id=group.id,
                    hostname=f'{instance.id}.com',
                    instance_id=instance.id,
                    instance_ip=instance.ip_address,
                    instance_state='running',
                    is_resource_group_stale=group.is_stale,
                    market=instance.market,
                    uptime=(self.simulator.current_time - instance.start_time),
                    weight=group.market_weight(instance.market),
                )
                agent_metadatas.append(metadata)

        return agent_metadatas
