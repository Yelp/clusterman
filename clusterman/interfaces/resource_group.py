from abc import ABCMeta
from abc import abstractclassmethod
from abc import abstractmethod
from abc import abstractproperty
from typing import Any
from typing import Collection
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence

import arrow

from clusterman.aws.markets import InstanceMarket


class InstanceMetadata(NamedTuple):
    group_id: str
    hostname: Optional[str]
    instance_id: str
    ip_address: Optional[str]
    is_resource_group_stale: bool
    market: InstanceMarket
    state: str
    uptime: arrow.Arrow
    weight: float


class ResourceGroup(metaclass=ABCMeta):
    """
    The ResourceGroup is an abstract object codifying the interface that objects belonging to a Mesos
    cluster are expected to adhere to.  In general, a "ResourceGroup" object should represent a collection of
    machines that are a part of a Mesos cluster, and should have an API for adding and removing hosts from the
    ResourceGroup, as well as querying the state of the resource group.
    """

    @abstractmethod
    def __init__(self, group_id: str) -> None:
        pass

    @abstractmethod
    def get_instance_metadatas(self, state_filter: Optional[Collection[str]] = None) -> Sequence[InstanceMetadata]:
        pass

    @abstractmethod
    def market_weight(self, market: InstanceMarket) -> float:
        """ Return the weighted capacity assigned to a particular market by this resource group

        .. note:: market_weight is compared to fulfilled_capacity when scaling down a pool, so it must
        return the same units.

        :param market: the :py:class:`.InstanceMarket` to get the weighted capacity for
        :returns: the weighted capacity of the market
        """
        pass

    @abstractmethod
    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool,
        dry_run: bool,
    ) -> None:
        """ Modify the target capacity for the resource group

        :param target_capacity: the (weighted) new target capacity for the resource group
        :param terminate_excess_capacity: boolean indicating whether to terminate instances if the
            new target capacity is less than the current capacity
        :param dry_run: boolean indicating whether to take action or just write to stdout
        """
        pass

    @abstractmethod
    def terminate_instances_by_id(self, instance_ids: List[str], batch_size: int = 500) -> Sequence[str]:
        """ Terminate instances in this resource group

        :param instance_ids: a list of instance IDs to terminate
        :param batch_size: number of instances to terminate at one time
        :returns: a list of terminated instance IDs
        """
        pass

    @abstractproperty
    def id(self) -> str:
        """ A unique identifier for this ResourceGroup """
        pass

    @abstractproperty
    def instance_ids(self) -> Sequence[str]:
        """ The list of instance IDs belonging to this ResourceGroup """
        pass

    @abstractproperty
    def market_capacities(self) -> Mapping[InstanceMarket, float]:
        """ The (weighted) capacities of each market in the resource group """
        pass

    @abstractproperty
    def target_capacity(self) -> float:
        """ The target (or desired) weighted capacity for this ResourceGroup

        Note that the actual weighted capacity in the ResourceGroup may be smaller or larger than the
        target capacity, depending on the state of the ResourceGroup, available instance types, and
        previous operations; use self.fulfilled_capacity to get the actual capacity
        """
        pass

    @abstractproperty
    def fulfilled_capacity(self) -> float:
        """ The actual weighted capacity for this ResourceGroup """
        pass

    @abstractproperty
    def status(self) -> str:
        """ The status of the ResourceGroup (e.g., running, modifying, terminated, etc.) """
        pass

    @abstractproperty
    def is_stale(self) -> bool:
        """Whether this ResourceGroup is stale."""
        pass

    @abstractclassmethod
    def load(cls, cluster: str, pool: str, config: Any) -> Mapping[str, 'ResourceGroup']:
        """ Load a list of corresponding resource groups

        :param cluster: a cluster name
        :param pool: a pool name
        :param config: a config specific to a resource group type
        :returns: a dictionary of resource groups, indexed by id
        """
        pass
