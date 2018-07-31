import staticconf

from clusterman.config import POOL_NAMESPACE
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup


def _make_agent(instance):
    return {
        'resources': {
            'cpus': instance.resources.cpus,
            'mem': instance.resources.mem,
            'disk': instance.resources.disk,
        },
        'used_resources': {
            # TODO CLUSTERMAN-145 - at some point we should track task start and end time, as well as
            # resource usage; then we can start simulating allocated resources as well.  But that is a longer
            # term goal, for right now the simulator just pretends that all agents are idle all the time.
        },
        '_aws_instance': instance,
    }


class SimulatedMesosPoolManager(MesosPoolManager):

    def __init__(self, cluster, pool, configs, simulator):
        self.cluster = cluster
        self.pool = pool
        self.simulator = simulator
        groups = [
            SimulatedSpotFleetResourceGroup(config, self.simulator)
            for config in configs
        ]
        self.resource_groups = {group.id: group for group in groups}
        pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool))
        self.min_capacity = pool_config.read_int('scaling_limits.min_capacity')
        self.max_capacity = pool_config.read_int('scaling_limits.max_capacity')

    @property
    def agents(self):
        return [
            _make_agent(group.instances[instance_id])
            for group in self.resource_groups.values()
            for instance_id in group.instances
        ]
