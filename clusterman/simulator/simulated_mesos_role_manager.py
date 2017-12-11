from collections import defaultdict

from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup


def _make_resource_block(resource_name, value):
    return {
        'name': resource_name,
        'scalar': {'value': value},
        'type': 'SCALAR',
    }


def _make_agent(instance):
    return {
        'total_resources': [
            _make_resource_block('cpus', instance.resources.cpus),
            _make_resource_block('mem', instance.resources.mem),
            _make_resource_block('disk', instance.resources.disk),
        ],
        'allocated_resources': {
            # TODO CLUSTERMAN-145 - at some point we should track task start and end time, as well as
            # resource usage; then we can start simulating allocated resources as well.  But that is a longer
            # term goal, for right now the simulator just pretends that all agents are idle all the time.
        },
        '_aws_instance': instance,
    }


class SimulatedMesosRoleManager(MesosRoleManager):

    def __init__(self, cluster, role, configs, simulator):
        self.cluster = cluster
        self.role = role
        self.resource_groups = [
            SimulatedSpotFleetResourceGroup(config, simulator)
            for config in configs
        ]

    def _idle_agents_by_market(self):
        idle_agents = [agent for agent in self.agents if allocated_cpu_resources(agent) == 0]

        idle_agents_by_market = defaultdict(list)
        for agent in idle_agents:
            aws_instance = agent['_aws_instance']
            idle_agents_by_market[aws_instance.market].append(aws_instance.id)
        return idle_agents_by_market

    @property
    def agents(self):
        return [
            _make_agent(group.instances[instance_id])
            for group in self.resource_groups
            for instance_id in group.instances
        ]
