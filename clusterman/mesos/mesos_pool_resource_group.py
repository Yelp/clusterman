from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from abc import abstractstaticmethod
from typing import Any
from typing import Mapping
from typing import Sequence

import colorlog

from clusterman.aws.markets import InstanceMarket


logger = colorlog.getLogger(__name__)


def protect_unowned_instances(func):
    """ A decorator that protects instances that are not owned by a particular ResourceGroup from being modified

    It is assumed that the decorated function takes a list of instance IDs as its first argument; this list
    is modified before the decorated function is called to strip out any unowned instances.  In this case a warning
    is logged.
    """

    def wrapper(self, instance_ids, *args, **kwargs):
        resource_group_instances = list(set(instance_ids) & set(self.instance_ids))
        invalid_instances = set(instance_ids) - set(self.instance_ids)
        if invalid_instances:
            logger.warn(f'Some instances are not part of this resource group ({self.id}):\n{invalid_instances}')
        return func(self, resource_group_instances, *args, **kwargs)
    return wrapper


class MesosPoolResourceGroup(metaclass=ABCMeta):
    """
    The MesosPoolResourceGroup is an abstract object codifying the interface that objects belonging to a Mesos
    cluster are expected to adhere to.  In general, a "ResourceGroup" object should represent a collection of machines
    that are a part of a Mesos cluster, and should have an API for adding and removing hosts from the ResourceGroup,
    as well as querying the state of the resource group.
    """

    def __init__(self, group_id: str) -> None:
        pass

    def market_weight(self, market: InstanceMarket) -> float:  # pragma: no cover
        """ Return the weighted capacity assigned to a particular EC2 market by this resource group

        The weighted capacity is a SpotFleet concept but for consistency we assume other resource group types will also
        have weights assigned to them; this will allow the MesosPool to operate on a variety of different resource types

        Note that market_weight is compared to fulfilled_capacity when scaling down a pool, so it must return the same
        units.

        :param market: the InstanceMarket to get the weighted capacity for
        :returns: the weighted capacity of the market (defaults to 1 unless overridden)
        """
        return 1

    @abstractmethod
    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool,
        dry_run: bool,
    ) -> None:  # pragma: no cover
        """ Modify the target capacity for the resource group

        :param target_capacity: the (weighted) new target capacity for the resource group
        :param terminate_excess_capacity: boolean indicating whether to terminate instances if the
            new target capacity is less than the current capacity
        :param dry_run: boolean indicating whether to take action or just write to stdout
        """
        pass

    @abstractmethod
    def terminate_instances_by_id(self, instance_ids: Sequence[str]) -> Sequence[str]:  # pragma: no cover
        """ Terminate instances in this resource group

        Subclasses should _always_ decorate this method with the @protect_unowned_instances decorator to prevent
        termination of instances that do not belong to this ResourceGroup

        :param instance_ids: a list of instance IDs to terminate
        :returns: a list of terminated instance IDs
        """
        pass

    @abstractproperty
    def id(self) -> str:  # pragma: no cover
        """ A unique identifier for this ResourceGroup """
        pass

    @abstractproperty
    def instance_ids(self) -> Sequence[str]:  # pragma: no cover
        """ The list of instance IDs belonging to this ResourceGroup """
        pass

    @abstractproperty
    def market_capacities(self) -> Mapping[InstanceMarket, float]:  # pragma: no cover
        """ A dictionary of InstanceMarket -> total (fulfilled) capacity values """
        pass

    @abstractproperty
    def target_capacity(self) -> float:  # pragma: no cover
        """ The target (or desired) weighted capacity for this ResourceGroup

        Note that the actual weighted capacity in the ResourceGroup may be smaller or larger than the
        target capacity, depending on the state of the ResourceGroup, available instance types, and
        previous operations; use self.fulfilled_capacity to get the actual capacity
        """
        pass

    @abstractproperty
    def fulfilled_capacity(self) -> float:  # pragma: no cover
        """ The actual weighted capacity for this ResourceGroup """
        pass

    @abstractproperty
    def status(self) -> str:  # pragma: no cover
        """ The status of the ResourceGroup (e.g., running, modifying, terminated, etc.) """
        pass

    @abstractproperty
    def is_stale(self) -> bool:  # pragma: no cover
        """Whether this ResourceGroup is stale."""
        pass

    @staticmethod
    @abstractstaticmethod
    def load(cluster: str, pool: str, config: Any) -> Mapping[str, 'MesosPoolResourceGroup']:
        """ Load a list of corresponding resource groups

        :param cluster: a cluster name
        :param pool: a pool name
        :param config: a config specific to a resource group type
        :returns: a dictionary of resource groups, indexed by id
        """
        pass
