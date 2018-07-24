from bisect import bisect
from collections import defaultdict
from typing import Collection
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type

import arrow
import staticconf
from cached_property import timed_cached_property

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import AllResourceGroupsAreStaleError
from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.mesos.spotinst_resource_group import SpotInstResourceGroup
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_agent_resources
from clusterman.mesos.util import get_total_resource_value
from clusterman.mesos.util import mesos_post
from clusterman.mesos.util import MesosAgentDict
from clusterman.mesos.util import MesosAgentState
from clusterman.mesos.util import total_agent_resources
from clusterman.util import get_clusterman_logger


AWS_RUNNING_STATES = ('running',)
MIN_CAPACITY_PER_GROUP = 1
logger = get_clusterman_logger(__name__)


class InstanceMetadata(NamedTuple):
    allocated_resources: Tuple[float, float, float]
    aws_state: str
    group_id: str
    instance_id: str
    instance_ip: Optional[str]
    is_stale: bool
    market: InstanceMarket
    mesos_state: MesosAgentState
    task_count: int
    total_resources: Tuple[float, float, float]
    uptime: arrow.Arrow
    weight: float


RESOURCE_GROUPS: Dict[
    str,
    Type[MesosPoolResourceGroup]
] = {
    'sfr': SpotFleetResourceGroup,
    'spotinst': SpotInstResourceGroup,
}


class MesosPoolManager:
    """ The MesosPoolManager object provides a consistent interface to the infrastructure that underpins a particular
    Mesos pool.  Specifically, it allows users to interact with the Mesos master (querying the number of agents in the
    cluster, and what resources are available/allocated, for example) as well as to modify the capacity available to the
    Mesos pool.  Since many different types of hosts may be present in a Mesos cluster, this object refers to a list of
    abstract :class:`MesosPoolResourceGroup <clusterman.mesos.mesos_pool_resource_group.MesosPoolResourceGroup>` objects
    to modify the underlying infrastructure.

    One major assumption the MesosPoolManager makes currently is that the underlying infrastructure for a particular
    pool belongs completely to that pool; in other words, at present no pools are co-located on the same physical
    hardware.  This assumption is subject to change in the future.

    .. note:: Values returned from MesosPoolManager functions may be cached to limit requests made to the Mesos masters
       or AWS API endpoints.
    """

    def __init__(self, cluster: str, pool: str) -> None:
        self.cluster = cluster
        self.pool = pool

        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool))

        mesos_master_fqdn = staticconf.read_string(f'mesos_clusters.{self.cluster}.fqdn')
        self.min_capacity = self.pool_config.read_int('scaling_limits.min_capacity')
        self.max_capacity = self.pool_config.read_int('scaling_limits.max_capacity')
        self.max_tasks_to_kill = self.pool_config.read_int('scaling_limits.max_tasks_to_kill', default=0)

        self.api_endpoint = f'http://{mesos_master_fqdn}:5050/'
        logger.info(f'Connecting to Mesos masters at {self.api_endpoint}')
        self.resource_groups: Dict[str, MesosPoolResourceGroup] = dict()
        self.reload_resource_groups()

    def reload_state(self) -> None:
        """ Fetch any state that may have changed behind our back, but which we do not want to change during an
        Autoscaler.run().
        """
        self.reload_resource_groups()

    def reload_resource_groups(self) -> None:
        resource_groups: Dict[str, MesosPoolResourceGroup] = {}
        for resource_group_conf in self.pool_config.read_list('resource_groups'):
            if not isinstance(resource_group_conf, dict) or len(resource_group_conf) != 1:
                logger.error(f'Malformed config: {resource_group_conf}')
                continue
            resource_group_type = list(resource_group_conf.keys())[0]
            resource_group_cls = RESOURCE_GROUPS.get(resource_group_type)
            if resource_group_cls is None:
                logger.warn(f'Unknown resource group {resource_group_type}')
                continue

            resource_groups.update(resource_group_cls.load(
                cluster=self.cluster,
                pool=self.pool,
                config=list(resource_group_conf.values())[0],
            ))
        self.resource_groups = resource_groups
        logger.info(f'Loaded resource groups: {list(resource_groups)}')

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

        .. note:: It may take some time (up to a few minutes) for changes in the target capacity to be reflected in
           :attr:`fulfilled_capacity`.  Once the capacity has equilibrated, the fulfilled capacity and the target
           capacity may not exactly match, but the fulfilled capacity will never be under the target (for example, if
           there is no combination of instances that evenly sum to the desired target capacity, the final fulfilled
           capacity will be slightly above the target capacity)
        """
        if dry_run:
            logger.warn('Running in "dry-run" mode; cluster state will not be modified')
        if not self.resource_groups:
            raise MesosPoolManagerError('No resource groups available')

        orig_target_capacity = self.target_capacity
        new_target_capacity = self._constrain_target_capacity(new_target_capacity, force)

        res_group_targets = self._compute_new_resource_group_targets(new_target_capacity)
        for group_id, target in res_group_targets.items():
            self.resource_groups[group_id].modify_target_capacity(
                target,
                terminate_excess_capacity=False,
                dry_run=dry_run,
            )

        self.prune_excess_fulfilled_capacity(new_target_capacity, res_group_targets, dry_run)
        logger.info(f'Target capacity for {self.pool} changed from {orig_target_capacity} to {new_target_capacity}')
        return new_target_capacity

    def prune_excess_fulfilled_capacity(
        self,
        new_target_capacity: float,
        group_targets: Optional[Dict[str, float]] = None,
        dry_run: bool = False,
    ) -> Sequence[str]:
        """ Decrease the capacity in the cluster

        The number of tasks killed is limited by self.max_tasks_to_kill, and the instances are terminated in an order
        which (hopefully) reduces the impact on jobs running on the cluster.

        :param group_targets: a list of new resource group target_capacities; if None, use the existing
            target_capacities (this parameter is necessary in order for dry runs to work correctly)
        :param dry_run: if True, do not modify the state of the cluster, just log actions
        :returns: a list of terminated instance ids
        """

        marked_instance_ids = self._choose_instances_to_prune(new_target_capacity, group_targets)

        # Terminate the marked instances; it's possible that not all instances will be terminated
        all_terminated_instance_ids: List[str] = []
        if not dry_run:
            for group_id, instance_ids in marked_instance_ids.items():
                terminated_instance_ids = self.resource_groups[group_id].terminate_instances_by_id(instance_ids)
                all_terminated_instance_ids.extend(terminated_instance_ids)
        else:
            all_terminated_instance_ids = [i for instances in marked_instance_ids.values() for i in instances]

        logger.info(f'The following instances have been terminated: {all_terminated_instance_ids}')
        return all_terminated_instance_ids

    def get_instance_metadatas(self, aws_state_filter: Optional[Collection[str]] = None) -> Sequence[InstanceMetadata]:
        """ Get a list of metadata about the instances currently in the pool

        :param aws_state_filter: only return instances matching a particular AWS state ('running', 'cancelled', etc)
        :returns: a list of InstanceMetadata objects
        """
        instance_id_to_task_count = self._count_tasks_per_agent()
        ip_to_agent: Dict[Optional[str], MesosAgentDict] = {
            agent_pid_to_ip(agent['pid']): agent for agent in self._agents
        }
        agent_metadatas = []

        for group in self.resource_groups.values():
            for instance_dict in ec2_describe_instances(instance_ids=group.instance_ids):
                aws_state = instance_dict['State']['Name']
                if aws_state_filter and aws_state not in aws_state_filter:
                    continue

                instance_market = get_instance_market(instance_dict)
                instance_ip = instance_dict.get('PrivateIpAddress')
                agent = ip_to_agent.get(instance_ip)

                metadata = InstanceMetadata(
                    allocated_resources=allocated_agent_resources(agent),
                    aws_state=aws_state,
                    group_id=group.id,
                    instance_id=instance_dict['InstanceId'],
                    instance_ip=instance_ip,
                    is_stale=group.is_stale,
                    market=instance_market,
                    mesos_state=self._get_agent_state(instance_ip, agent),
                    task_count=instance_id_to_task_count[agent['id']] if agent else 0,
                    total_resources=total_agent_resources(agent),
                    uptime=(arrow.now() - arrow.get(instance_dict['LaunchTime'])),
                    weight=group.market_weight(instance_market),
                )
                agent_metadatas.append(metadata)

        return agent_metadatas

    def get_resource_allocation(self, resource_name: str) -> float:
        """Get the total amount of the given resource currently allocated for this Mesos pool.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        return get_total_resource_value(self._agents, 'used_resources', resource_name)

    def get_resource_total(self, resource_name: str) -> float:
        """Get the total amount of the given resource for this Mesos pool.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        return get_total_resource_value(self._agents, 'resources', resource_name)

    def get_percent_resource_allocation(self, resource_name: str) -> float:
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        total = self.get_resource_total(resource_name)
        used = self.get_resource_allocation(resource_name)
        return used / total if total else 0

    def _constrain_target_capacity(
        self,
        requested_target_capacity: float,
        force: bool = False,
    ) -> float:
        """ Signals can return arbitrary values, so make sure we don't add or remove too much capacity """

        # TODO (CLUSTERMAN-126) max_weight_to_add and max_weight_to_remove are clusterwide settings,
        # not per-pool settings.  Right now we read them from the cluster-wide srv-configs, but the only
        # place to apply the limits are in the pool-manager.  When we start to support multiple pools
        # per cluster this will need to change.
        max_weight_to_add = staticconf.read_int(f'mesos_clusters.{self.cluster}.max_weight_to_add')
        max_weight_to_remove = staticconf.read_int(f'mesos_clusters.{self.cluster}.max_weight_to_remove')

        requested_delta = requested_target_capacity - self.target_capacity
        if requested_delta > 0:
            delta = min(self.max_capacity - self.target_capacity, max_weight_to_add, requested_delta)
        elif requested_delta < 0:
            delta = max(self.min_capacity - self.target_capacity, -max_weight_to_remove, requested_delta)
        else:
            delta = 0

        constrained_target_capacity = self.target_capacity + delta
        if requested_delta != delta:
            if force:
                forced_target_capacity = self.target_capacity + requested_delta
                logger.warn(
                    f'Forcing target capacity to {forced_target_capacity} even though '
                    f'scaling limits would restrict to {constrained_target_capacity}.'
                )
                return forced_target_capacity
            else:
                logger.warn(
                    f'Requested target capacity {requested_target_capacity}; '
                    f'restricting to {constrained_target_capacity} due to scaling limits.'
                )
        return constrained_target_capacity

    def _choose_instances_to_prune(
        self,
        new_target_capacity: float,
        group_targets: Optional[Dict[str, float]],
    ) -> Dict[str, List[str]]:
        """ Choose instances to kill in order to decrease the capacity on the cluster.

        The number of tasks killed is limited by self.max_tasks_to_kill, and the instances are terminated in an order
        which (hopefully) reduces the impact on jobs running on the cluster.

        :param new_target_capacity: The total new target capacity for the pool. Most of the time, this is equal to
            self.target_capacity, but in some situations (such as when all resource groups are stale),
            modify_target_capacity cannot make self.target_capacity equal new_target_capacity. We'd rather this method
            aim for the actual target value.
        :param group_targets: a list of new resource group target_capacities; if None, use the existing
            target_capacities (this parameter is necessary in order for dry runs to work correctly)
        :returns: a dict of resource group -> list of ids to terminate
        """

        # If dry_run is True in modify_target_capacity, the resource group target_capacity values will not have changed,
        # so this function would not choose to terminate any instances (see case #2 in the while loop below).  So
        # instead we take a list of new target capacities to use in this computation.
        #
        # We leave the option for group_targets to be None in the event that we want to call
        # prune_excess_fulfilled_capacity outside the context of a modify_target_capacity call
        if not group_targets:
            group_targets = {group_id: rg.target_capacity for group_id, rg in self.resource_groups.items()}

        curr_capacity = self.fulfilled_capacity
        # we use new_target_capacity instead of self.target_capacity here in case they are different (see docstring)
        if curr_capacity <= new_target_capacity:
            return {}

        prioritized_killable_instances = self._get_prioritized_killable_instances()
        logger.info('Killable instance IDs in kill order:\n{instances}'.format(
            instances=[instance.instance_id for instance in prioritized_killable_instances],
        ))

        if not prioritized_killable_instances:
            return {}
        rem_group_capacities = {group_id: rg.fulfilled_capacity for group_id, rg in self.resource_groups.items()}

        # How much capacity is actually up and available in Mesos.
        remaining_non_orphan_capacity = self.non_orphan_fulfilled_capacity

        # Iterate through all of the idle agents and mark one at a time for removal until we reach our target capacity
        # or have reached our limit of tasks to kill.
        marked_instance_ids: Dict[str, List[str]] = defaultdict(list)
        killed_task_count = 0
        for instance in prioritized_killable_instances:
            # Try to mark the instance for removal; this could fail in a few different ways:
            #  1) The resource group the instance belongs to can't be reduced further.
            #  2) Killing the instance's tasks would take over the maximum number of tasks we are willing to kill.
            #  3) Killing the instance would bring us under our target_capacity of non-orphaned instances.
            # In each of the cases, the instance has been removed from consideration and we jump to the next iteration.

            # Make sure we don't make a resource group go below its target capacity
            if rem_group_capacities[instance.group_id] - instance.weight < group_targets[instance.group_id]:  # case 1
                logger.info(
                    f'Resource group {instance.group_id} is at target capacity; skipping {instance.instance_id}'
                )
                continue

            if killed_task_count + instance.task_count > self.max_tasks_to_kill:  # case 2
                logger.info(
                    f'Killing instance {instance.instance_id} with {instance.task_count} tasks would take us over our '
                    f'max_tasks_to_kill of {self.max_tasks_to_kill}. Skipping this instance.'
                )
                continue

            if instance.mesos_state != MesosAgentState.ORPHANED:
                if (remaining_non_orphan_capacity - instance.weight < new_target_capacity):  # case 3
                    logger.info(
                        f'Killing instance {instance.instance_id} with weight {instance.weight} would take us under '
                        f'our target_capacity for non-orphan boxes. Skipping this instance.'
                    )
                    continue

            logger.info(f'marking {instance.instance_id} for termination')
            marked_instance_ids[instance.group_id].append(instance.instance_id)
            rem_group_capacities[instance.group_id] -= instance.weight
            curr_capacity -= instance.weight
            killed_task_count += instance.task_count
            if instance.mesos_state != MesosAgentState.ORPHANED:
                remaining_non_orphan_capacity -= instance.weight

            if curr_capacity <= new_target_capacity:
                logger.info("Seems like we've picked enough instances to kill; finishing")
                break

        return marked_instance_ids

    def _compute_new_resource_group_targets(self, new_target_capacity: float) -> Dict[str, float]:
        """ Compute a balanced distribution of target capacities for the resource groups in the cluster

        :param new_target_capacity: the desired new target capacity that needs to be distributed
        :returns: A list of target_capacity values, sorted in order of resource groups
        """

        stale_groups = [group for group in self.resource_groups.values() if group.is_stale]
        non_stale_groups = [group for group in self.resource_groups.values() if not group.is_stale]

        # If we're scaling down the logic is identical but reversed, so we multiply everything by -1
        coeff = -1 if new_target_capacity < self.target_capacity else 1

        groups_to_change = sorted(
            non_stale_groups,
            key=lambda g: coeff * g.target_capacity,
        )

        targets_to_change = [coeff * g.target_capacity for g in groups_to_change]
        num_groups_to_change = len(groups_to_change)

        if targets_to_change:
            while True:
                # If any resource groups are currently above the new target "uniform" capacity, we need to recompute
                # the target while taking into account the over-supplied resource groups.  We never decrease the
                # capacity of a resource group here, so we just find the first index is above the desired target
                # and remove those from consideration.  We have to repeat this multiple times, as new resource
                # groups could be over the new "uniform" capacity after we've subtracted the overage value
                #
                # (For scaling down, apply the same logic for resource groups below the target "uniform" capacity
                # instead; i.e., instances will below the target capacity will not be increased)
                capacity_per_group, remainder = divmod(new_target_capacity, num_groups_to_change)
                pos = bisect(targets_to_change, coeff * capacity_per_group)
                residual = sum(targets_to_change[pos:num_groups_to_change])

                if residual == 0:
                    for i in range(num_groups_to_change):
                        targets_to_change[i] = coeff * (capacity_per_group + (1 if i < remainder else 0))
                    break

                new_target_capacity -= coeff * residual
                num_groups_to_change = pos
        else:
            logger.info('Cannot set target capacity as all of our known resource groups are stale.')

        targets: Dict[MesosPoolResourceGroup, float] = {}

        # For stale groups, we set target_capacity to 0. This is a noop on SpotFleetResourceGroup.
        for stale_group in stale_groups:
            targets[stale_group] = 0

        for group_to_change, new_target in zip(groups_to_change, targets_to_change):
            targets[group_to_change] = new_target / coeff

        return {group_id: targets[group] for group_id, group in self.resource_groups.items()}

    def _get_market_capacities(
        self,
        market_filter: Optional[Collection[InstanceMarket]] = None
    ) -> Dict[InstanceMarket, float]:
        """ Return the total (fulfilled) capacities in the cluster across all resource groups """
        total_market_capacities: Dict[InstanceMarket, float] = defaultdict(float)
        for group in self.resource_groups.values():
            for market, capacity in group.market_capacities.items():
                if not market_filter or market in market_filter:
                    total_market_capacities[market] += capacity
        return total_market_capacities

    def _get_prioritized_killable_instances(self) -> List[InstanceMetadata]:
        """Get a list of killable instances in the cluster in the order in which they should be considered for
        termination.
        """
        killable_instances = [
            metadata for metadata in self.get_instance_metadatas(AWS_RUNNING_STATES)
            if self._is_instance_killable(metadata)
        ]
        return self._prioritize_killable_instances(killable_instances)

    def _is_instance_killable(self, metadata: InstanceMetadata) -> bool:
        if metadata.mesos_state == MesosAgentState.UNKNOWN:
            return False
        elif self.max_tasks_to_kill > 0:
            return True
        else:
            return metadata.task_count == 0

    def _prioritize_killable_instances(self, killable_instances: List[InstanceMetadata]) -> List[InstanceMetadata]:
        """Returns killable_instances sorted with most-killable things first."""
        def sort_key(killable_instance: InstanceMetadata) -> Tuple[int, int, int, int]:
            return (
                0 if killable_instance.mesos_state == MesosAgentState.ORPHANED else 1,
                0 if killable_instance.mesos_state == MesosAgentState.IDLE else 1,
                0 if killable_instance.is_stale else 1,
                killable_instance.task_count,
            )
        return sorted(
            killable_instances,
            key=sort_key,
        )

    def _get_agent_state(self, instance_ip: Optional[str], agent: Optional[MesosAgentDict]) -> MesosAgentState:
        if not instance_ip:
            return MesosAgentState.UNKNOWN
        elif not agent:
            return MesosAgentState.ORPHANED
        elif allocated_agent_resources(agent)[0] == 0:
            return MesosAgentState.IDLE
        else:
            return MesosAgentState.RUNNING

    def _count_tasks_per_agent(self) -> DefaultDict[str, int]:
        """Given a list of mesos tasks, return a count of tasks per agent"""
        instance_id_to_task_count: DefaultDict[str, int] = defaultdict(int)
        for task in self._tasks:
            if task['state'] == 'TASK_RUNNING':
                instance_id_to_task_count[task['slave_id']] += 1
        return instance_id_to_task_count

    @property
    def target_capacity(self) -> float:
        """ The target capacity is the *desired* weighted capacity for the given Mesos cluster pool.  There is no
        guarantee that the actual capacity will equal the target capacity.
        """
        non_stale_groups = [group for group in self.resource_groups.values() if not group.is_stale]
        if not non_stale_groups:
            raise AllResourceGroupsAreStaleError()
        return sum(group.target_capacity for group in non_stale_groups)

    @property
    def fulfilled_capacity(self) -> float:
        """ The fulfilled capacity is the *actual* weighted capacity for the given Mesos cluster pool at a particular
        point in time.  This may be equal to, above, or below the :attr:`target_capacity`, depending on the availability
        and state of AWS at the time.  In general, once the cluster has reached equilibrium, the fulfilled capacity will
        be greater than or equal to the target capacity.
        """
        return sum(group.fulfilled_capacity for group in self.resource_groups.values())

    @property
    def non_orphan_fulfilled_capacity(self) -> float:
        return sum(
            metadata.weight for metadata in self.get_instance_metadatas(AWS_RUNNING_STATES)
            if metadata.mesos_state != MesosAgentState.ORPHANED
        )

    # TODO (CLUSTERMAN-278): fetch this in reload_state
    @timed_cached_property(CACHE_TTL_SECONDS)
    def _agents(self) -> Sequence[MesosAgentDict]:
        response = mesos_post(self.api_endpoint, 'slaves').json()
        return [
            agent
            for agent in response['slaves']
            if agent.get('attributes', {}).get('pool', 'default') == self.pool
        ]

    # TODO (CLUSTERMAN-278): fetch this in reload_state
    @timed_cached_property(CACHE_TTL_SECONDS)
    def _frameworks(self) -> Sequence[Dict]:
        response = mesos_post(self.api_endpoint, 'master/frameworks').json()
        return response['frameworks']

    @property
    def _tasks(self) -> Sequence[Dict]:
        tasks: List[Dict] = []
        for framework in self._frameworks:
            tasks.extend(framework['tasks'])
        return tasks
