import enum
from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from typing import Collection
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence

import arrow

from clusterman.aws.markets import InstanceMarket
from clusterman.interfaces.resource_group import ResourceGroup

ClustermanResources = NamedTuple('ClustermanResources', [('cpus', float), ('mem', float), ('disk', float)])


class AgentState(enum.Enum):
    IDLE = 'idle'
    ORPHANED = 'orphaned'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


class InstanceMetadata(NamedTuple):
    hostname: str
    allocated_resources: ClustermanResources
    aws_state: str
    group_id: str
    instance_id: str
    instance_ip: Optional[str]
    is_resource_group_stale: bool
    market: InstanceMarket
    state: AgentState
    batch_task_count: int
    task_count: int
    total_resources: ClustermanResources
    uptime: arrow.Arrow
    weight: float


class PoolManager(metaclass=ABCMeta):
    """ The ``PoolManager`` object provides a consistent interface to the infrastructure that underpins a particular
    pool of machines.  Specifically, it allows users to easily modify the capacity available to the pool without having
    to worry about the underlying makeup or API calls needed to modify the pool.  Since many different types of hosts
    may be present in a cluster, this object refers to a list of abstract :py:class:`.ResourceGroup` objects to modify
    the underlying infrastructure.

    One major assumption the ``PoolManager`` makes currently is that the underlying infrastructure for a particular
    pool belongs completely to that pool; in other words, at present no pools are co-located on the same physical
    hardware.  This assumption is subject to change in the future.
    """
    resource_groups: MutableMapping[str, ResourceGroup] = dict()

    @abstractmethod
    def __init__(self, cluster: str, pool: str, *, fetch_state: bool = True) -> None:
        pass

    @abstractmethod
    def reload_state(self) -> None:
        """ Fetch any state that may have changed behind our back, but which we do not want to change during an
        ``Autoscaler.run()``.
        """

    @abstractmethod
    def modify_target_capacity(
        self,
        new_target_capacity: float,
        dry_run: bool = False,
        force: bool = False,
    ) -> float:
        """ Change the desired :attr:`target_capacity` of the resource groups belonging to this pool.

        Capacity changes are roughly evenly distributed across the resource groups to ensure that
        instances are diversified in the cluster

        :param new_target_capacity: the desired target capacity for the cluster and pool
        :param dry_run: boolean indicating whether the cluster should actually be modified
        :param force: boolean indicating whether to override the scaling limits
        :returns: the (set) new target capacity

        .. note:: It may take some time (up to a few minutes) for changes in the target capacity to be reflected in
           :attr:`fulfilled_capacity`.  Once the capacity has equilibrated, the fulfilled capacity and the target
           capacity may not exactly match, but the fulfilled capacity will never be under the target (for example, if
           there is no combination of instances that evenly sum to the desired target capacity, the final fulfilled
           capacity will be slightly above the target capacity)
        """
        pass

    @abstractmethod
    def prune_excess_fulfilled_capacity(
        self,
        new_target_capacity: float,
        group_targets: Optional[Mapping[str, float]] = None,
        dry_run: bool = False,
    ) -> None:
        """ Decrease the capacity in the cluster

        The number of tasks killed is limited by ``self.max_tasks_to_kill``, and the instances are terminated in an
        order which (hopefully) reduces the impact on jobs running on the cluster.

        :param group_targets: a list of new resource group target_capacities; if None, use the existing
            target_capacities (this parameter is necessary in order for dry runs to work correctly)
        :param dry_run: if True, do not modify the state of the cluster, just log actions
        """
        pass

    @abstractmethod
    def get_instance_metadatas(self, aws_state_filter: Optional[Collection[str]] = None) -> Sequence[InstanceMetadata]:
        """ Get a list of metadata about the instances currently in the pool

        :param aws_state_filter: only return instances matching a particular AWS state ('running', 'cancelled', etc)
        :returns: a list of InstanceMetadata objects
        """
        pass

    @abstractmethod
    def get_resource_allocation(self, resource_name: str) -> float:
        """Get the total amount of the given resource currently allocated for this pool.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the allocated resources in the pool for the specified resource
        """
        pass

    @abstractmethod
    def get_resource_total(self, resource_name: str) -> float:
        """Get the total amount of the given resource for this pool.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the total resources in the pool for the specified resource
        """
        pass

    @abstractmethod
    def get_percent_resource_allocation(self, resource_name: str) -> float:
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the percentage allocated for the specified resource
        """
        total = self.get_resource_total(resource_name)
        used = self.get_resource_allocation(resource_name)
        return used / total if total else 0

    @abstractmethod
    def get_market_capacities(
        self,
        market_filter: Optional[Collection[InstanceMarket]] = None
    ) -> Mapping[InstanceMarket, float]:
        """ Return the total (fulfilled) capacities in the pool across all resource groups

        :param market_filter: a set of :py:class:`.InstanceMarket` to filter by
        :returns: the total capacity in each of the specified markets
        """
        pass

    @abstractproperty
    def target_capacity(self) -> float:
        """ The target capacity is the *desired* weighted capacity for the given Mesos cluster pool.  There is no
        guarantee that the actual capacity will equal the target capacity.
        """
        pass

    @abstractproperty
    def fulfilled_capacity(self) -> float:
        """ The fulfilled capacity is the *actual* weighted capacity for the given Mesos cluster pool at a particular
        point in time.  This may be equal to, above, or below the :attr:`target_capacity`, depending on the availability
        and state of AWS at the time.  In general, once the cluster has reached equilibrium, the fulfilled capacity will
        be greater than or equal to the target capacity.
        """
        pass
