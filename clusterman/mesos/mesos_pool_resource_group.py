from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty

from clusterman.util import get_clusterman_logger


logger = get_clusterman_logger(__name__)


def protect_unowned_instances(func):
    # Make sure that we only operate on instances that are a part of this resource group
    def wrapper(self, instance_ids, *args, **kwargs):
        resource_group_instances = list(set(instance_ids) & set(self.instances))
        invalid_instances = set(instance_ids) - set(self.instances)
        if invalid_instances:
            logger.warn(f'Some instances are not part of this resource group ({self.id}):\n{invalid_instances}')
        return func(self, resource_group_instances, *args, **kwargs)
    return wrapper


class MesosPoolResourceGroup(metaclass=ABCMeta):

    @abstractmethod
    def market_weight(self, market):
        pass

    @abstractmethod
    def modify_target_capacity(self, new_target_capacity, should_terminate):
        pass

    @abstractmethod
    def terminate_instances_by_id(self, instance_ids):
        pass

    @abstractproperty
    def id(self):
        pass

    @abstractproperty
    def instances(self):
        pass

    @abstractproperty
    def market_capacities(self):
        pass

    @abstractproperty
    def target_capacity(self):
        pass

    @abstractproperty
    def status(self):
        pass
