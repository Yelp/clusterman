from clusterman.mesos.mesos_role_resource_group import MesosRoleResourceGroup
from clusterman.mesos.mesos_role_resource_group import protect_unowned_instances


class DummyResourceGroup(MesosRoleResourceGroup):
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
    def fulfilled_capacity(self):
        pass

    @property
    def status(self):
        pass


def test_protect_unowned_instances():
    assert DummyResourceGroup().terminate_instances_by_id(['fake-1', 'fake-4']) == ['fake-1']


def test_protect_unowned_instances_noop():
    assert sorted(DummyResourceGroup().terminate_instances_by_id(['fake-1', 'fake-2', 'fake-3'])) == \
        ['fake-1', 'fake-2', 'fake-3']
