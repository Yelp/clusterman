from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.mesos_pool_resource_group import protect_unowned_instances


class DummyResourceGroup(MesosPoolResourceGroup):
    def market_weight(self, market):
        pass

    def modify_target_capacity(self, new_target_capacity, should_terminate):
        pass

    @protect_unowned_instances
    def terminate_instances_by_id(self, instance_ids):
        return instance_ids

    @property
    def id(self):
        return 'fake-resource-group'

    @property
    def instances(self):
        return ['fake-1', 'fake-2', 'fake-3']

    @property
    def market_capacities(self):
        pass

    @property
    def target_capacity(self):
        pass

    @property
    def status(self):
        pass


def test_protect_unowned_instances():
    assert DummyResourceGroup().terminate_instances_by_id(['fake-1', 'fake-4']) == ['fake-1']
