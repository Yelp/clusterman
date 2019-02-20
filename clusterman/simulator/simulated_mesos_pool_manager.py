from typing import cast
from typing import Collection
from typing import Dict
from typing import Optional
from typing import Sequence

import staticconf

from clusterman.config import POOL_NAMESPACE
from clusterman.mesos.mesos_pool_manager import InstanceMetadata
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_agent_resources
from clusterman.mesos.util import MesosAgentDict
from clusterman.mesos.util import MesosAgentState
from clusterman.mesos.util import total_agent_resources
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup
from clusterman.util import read_int_or_inf


def _make_agent(instance):
    return {
        'resources': {
            'cpus': instance.resources.cpus,
            'mem': instance.resources.mem * 1000,
            'disk': (instance.resources.disk or staticconf.read_int('ebs_volume_size', 0)) * 1000,
        },
        'used_resources': {
            # TODO CLUSTERMAN-145 - at some point we should track task start and end time, as well as
            # resource usage; then we can start simulating allocated resources as well.  But that is a longer
            # term goal, for right now the simulator just pretends that all agents are idle all the time.
        },
        '_aws_instance': instance,
        'pid': f'agent(1)@{instance.ip_address}:5051',
    }


class SimulatedMesosPoolManager(MesosPoolManager):

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
        ip_to_agent: Dict[Optional[str], MesosAgentDict] = {
            agent_pid_to_ip(agent['pid']): agent for agent in self.agents
        }
        for group in self.resource_groups.values():
            for instance in cast(SimulatedAWSCluster, group).instances.values():
                if aws_state_filter and 'running' not in aws_state_filter:
                    continue

                agent = ip_to_agent.get(instance.ip_address)
                metadata = InstanceMetadata(
                    hostname='host123',
                    allocated_resources=allocated_agent_resources(agent),
                    aws_state='running',
                    group_id=group.id,
                    instance_id=instance.id,
                    instance_ip=instance.ip_address,
                    is_resource_group_stale=group.is_stale,
                    market=instance.market,
                    mesos_state=(
                        MesosAgentState.ORPHANED
                        if self.simulator.current_time < instance.join_time
                        else MesosAgentState.RUNNING
                    ),
                    task_count=0,  # CLUSTERMAN-145
                    batch_task_count=0,
                    total_resources=total_agent_resources(agent),
                    uptime=(self.simulator.current_time - instance.start_time),
                    weight=group.market_weight(instance.market),
                )
                agent_metadatas.append(metadata)

        return agent_metadatas

    @property
    def non_orphan_fulfilled_capacity(self):
        return self._calculate_non_orphan_fulfilled_capacity()

    @property
    def agents(self):
        return [
            _make_agent(group.instances[instance_id])
            for group in self.resource_groups.values()
            for instance_id in group.instances
        ]

    @property
    def frameworks(self):
        return []

    @property
    def tasks(self):
        return []
