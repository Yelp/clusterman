from clusterman.mesos.mesos_role_manager import MesosRoleManager
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
            # TODO CLUSTERMAN-145
        },
    }
    pass


class SimulatedMesosRoleManager(MesosRoleManager):

    def __init__(self, cluster, role, config, simulator):
        self.cluster = cluster
        self.role = role
        self.resource_groups = [SimulatedSpotFleetResourceGroup(config, simulator)]  # TODO more resource groups?

    def _idle_agents_by_market(self):
        # TODO CLUSTERMAN-145
        pass

    @property
    def agents(self):
        agents = []
        for group in self.resource_groups:
            for instance in self.instances:
                agents.append(_make_agent(instance))
