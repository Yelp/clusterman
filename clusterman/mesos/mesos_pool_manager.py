from bisect import bisect
from collections import defaultdict
from collections import namedtuple
from pprint import pformat
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type

import staticconf
from cached_property import timed_cached_property

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.mesos.spotinst_resource_group import SpotInstResourceGroup
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.mesos.util import get_total_resource_value
from clusterman.mesos.util import mesos_post
from clusterman.mesos.util import MesosAgentState
from clusterman.util import get_clusterman_logger


MIN_CAPACITY_PER_GROUP = 1
logger = get_clusterman_logger(__name__)


PoolInstance = namedtuple(
    'PoolInstance',
    ['instance_id', 'state', 'task_count', 'market', 'instance_dict', 'agent']
)

RESOURCE_GROUPS: Dict[
    str,
    Type[MesosPoolResourceGroup]
] = {
    "sfr": SpotFleetResourceGroup,
    "spotinst": SpotInstResourceGroup,
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

        pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool))

        mesos_master_fqdn = staticconf.read_string(f'mesos_clusters.{self.cluster}.fqdn')
        self.min_capacity = pool_config.read_int('scaling_limits.min_capacity')
        self.max_capacity = pool_config.read_int('scaling_limits.max_capacity')
        self.max_tasks_to_kill = pool_config.read_int('scaling_limits.max_tasks_to_kill', default=0)

        self.api_endpoint = f'http://{mesos_master_fqdn}:5050/'
        logger.info(f'Connecting to Mesos masters at {self.api_endpoint}')

        self.resource_groups: List[MesosPoolResourceGroup] = list()
        resource_groups = pool_config.read_list("resource_groups2")
        for resource_group in resource_groups:
            if not isinstance(resource_group, dict) or len(resource_group) != 1:
                logger.error(f"Malformed config: {resource_group}")
                continue
            resource_group_type = list(resource_group.keys())[0]
            resource_group_cls = RESOURCE_GROUPS.get(resource_group_type)
            if resource_group_cls is None:
                logger.warn(f"Unknown resource group {resource_group_type}")
                continue

            resource_group_config = list(resource_group.values())[0]

            self.resource_groups.extend(resource_group_cls.load(
                cluster=self.cluster,
                pool=self.pool,
                config=resource_group_config,
            ))

        logger.info('Loaded resource groups: {ids}'.format(ids=[group.id for group in self.resource_groups]))

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
        for i, target in enumerate(res_group_targets):
            self.resource_groups[i].modify_target_capacity(
                target,
                terminate_excess_capacity=False,
                dry_run=dry_run,
            )
        if new_target_capacity <= orig_target_capacity:
            self.prune_excess_fulfilled_capacity(res_group_targets, dry_run)
        logger.info(f'Target capacity for {self.pool} changed from {orig_target_capacity} to {new_target_capacity}')
        return new_target_capacity

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

    def prune_excess_fulfilled_capacity(
        self,
        group_targets: Optional[Sequence[float]] = None,
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

        # If dry_run is True, the resource group target_capacity values will not have changed, so this function will not
        # terminate any instances (see case #2 in the while loop below).  So instead we pass in a list of new target
        # capacities to use in that computation.
        #
        # We leave the option for group_targets to be None in the event that we want to call
        # prune_excess_fulfilled_capacity outside the context of a modify_target_capacity call
        if not group_targets:
            group_targets = [rg.target_capacity for rg in self.resource_groups]
        target_capacity = sum(group_targets)

        curr_capacity = self.fulfilled_capacity
        if curr_capacity <= target_capacity:
            return []

        prioritized_killable_instances = self.get_prioritized_killable_instances()
        logger.debug('Killable instances in kill order:\n{instances}'.format(
            # `ignore` is a workaround for the bug of typeshed doesn't contain `compact` for `pformat`
            instances=pformat(prioritized_killable_instances, compact=True, width=100)  # type: ignore
        ))

        if not prioritized_killable_instances:
            return []
        rem_group_capacities = {group.id: group.fulfilled_capacity for group in self.resource_groups}

        # Iterate through all of the idle agents and mark one at a time for removal until we reach our target capacity
        # or have reached our limit of tasks to kill
        marked_instance_ids: Dict[MesosPoolResourceGroup, List[str]] = defaultdict(list)
        killed_task_count = 0
        for instance in prioritized_killable_instances:
            # Try to mark the instance for removal; this could fail in a few different ways:
            #  1) Something is wrong with the instance itself (e.g., it's not actually in a cluster)
            #  2) The resource group the instance belongs to can't be reduced further
            #  3) The killing the instance's tasks would take over the maximum number of tasks we are willing to kill
            # In each of the cases, the instance has been removed from consideration and we jump to the next iteration

            group_index, instance_group = self._find_resource_group(instance.instance_id)

            if not instance_group:  # case 1
                logger.warn(f'Could not find instance {instance.instance_id} in any resource group')
                continue

            instance_weight = instance_group.market_weight(instance.market)

            # Make sure we don't make a resource group go below its target capacity
            if rem_group_capacities[instance_group.id] - instance_weight < group_targets[group_index]:  # case 2
                logger.debug(
                    f'Resource group {instance_group.id} is at target capacity; skipping {instance.instance_id}'
                )
                continue

            if killed_task_count + instance.task_count > self.max_tasks_to_kill:  # case 3
                logger.info(
                    f'Killing instance {instance.instance_id} with {instance.task_count} tasks would take us over our '
                    f'max_tasks_to_kill of {self.max_tasks_to_kill}. Skipping this instance.'
                )
                continue

            marked_instance_ids[instance_group].append(instance.instance_id)
            rem_group_capacities[instance_group.id] -= instance_weight
            curr_capacity -= instance_weight
            killed_task_count += instance.task_count

            if curr_capacity <= target_capacity:
                break

        # Terminate the marked instances; it's possible that not all instances will be terminated
        all_terminated_instance_ids: List[str] = []
        if not dry_run:
            for group, instance_ids in marked_instance_ids.items():
                terminated_instance_ids = group.terminate_instances_by_id(instance_ids)
                all_terminated_instance_ids.extend(terminated_instance_ids)
        else:
            all_terminated_instance_ids = [i for instances in marked_instance_ids.values() for i in instances]

        logger.info(f'The following instances have been terminated: {all_terminated_instance_ids}')
        return all_terminated_instance_ids

    def _compute_new_resource_group_targets(self, new_target_capacity: float) -> Sequence[float]:
        """ Compute a balanced distribution of target capacities for the resource groups in the cluster

        :param new_target_capacity: the desired new target capacity that needs to be distributed
        :returns: A list of target_capacity values, sorted in order of resource groups
        """
        if new_target_capacity == self.target_capacity:
            return [group.target_capacity for group in self.resource_groups]

        # If we're scaling down the logic is identical but reversed, so we multiply everything by -1
        coeff = -1 if new_target_capacity < self.target_capacity else 1
        new_targets_with_indices = sorted(
            [(i, coeff * group.target_capacity) for i, group in enumerate(self.resource_groups)],
            key=lambda x: (x[1], x[0]),
        )

        original_indices, new_targets = [list(a) for a in zip(*new_targets_with_indices)]
        num_groups_to_change = len(self.resource_groups)
        while True:
            # If any resource groups are currently above the new target "uniform" capacity, we need to recompute
            # the target while taking into account the over-supplied resource groups.  We never decrease the
            # capacity of a resource group here, so we just find the first index is above the desired target
            # and remove those from consideration.  We have to repeat this multiple times, as new resource
            # groups could be over the new "uniform" capacity after we've subtracted the overage value
            #
            # (For scaling down, apply the same logic for resource groups below the target "uniform" capacity instead;
            # i.e., instances will below the target capacity will not be increased)
            capacity_per_group, remainder = divmod(new_target_capacity, num_groups_to_change)
            pos = bisect(new_targets, coeff * capacity_per_group)
            residual = sum(new_targets[pos:num_groups_to_change])

            if residual == 0:
                for i in range(num_groups_to_change):
                    new_targets[i] = coeff * (capacity_per_group + (1 if i < remainder else 0))
                break

            new_target_capacity -= coeff * residual
            num_groups_to_change = pos

        return [
            target
            for __, target in sorted(zip(original_indices, [coeff * target for target in new_targets]))
        ]

    def _find_resource_group(self, instance_id: str) -> Tuple[int, Optional[MesosPoolResourceGroup]]:
        """ Find the resource group that an instance belongs to """
        for i, group in enumerate(self.resource_groups):
            if instance_id in group.instance_ids:
                return i, group
        return -1, None

    def _get_market_capacities(self, market_filter=None) -> Dict[InstanceMarket, float]:
        """ Return the total (fulfilled) capacities in the cluster across all resource groups """
        total_market_capacities: Dict[InstanceMarket, float] = defaultdict(float)
        for group in self.resource_groups:
            for market, capacity in group.market_capacities.items():
                if not market_filter or market in market_filter:
                    total_market_capacities[market] += capacity
        return total_market_capacities

    def get_prioritized_killable_instances(self) -> List[PoolInstance]:
        """Get a list of killable instances in the cluster in the order in which they should be considered for
        termination.
        """
        killable_instances = self._get_killable_instances()
        return self._prioritize_killable_instances(killable_instances)

    def _get_killable_instances(self):
        return [instance for instance in self.get_instances() if self._is_instance_killable(instance)]

    def _is_instance_killable(self, instance: PoolInstance) -> bool:
        if instance.state == MesosAgentState.UNKNOWN:
            return False
        elif self.max_tasks_to_kill > 0:
            return True
        else:
            return instance.task_count == 0

    def _prioritize_killable_instances(self, killable_instances: List[PoolInstance]) -> List[PoolInstance]:
        return sorted(
            killable_instances,
            key=lambda x: (
                0 if x.state == MesosAgentState.ORPHANED else 1,
                0 if x.state == MesosAgentState.IDLE else 1,
                x.task_count,
            )
        )

    @property
    def target_capacity(self) -> float:
        """ The target capacity is the *desired* weighted capacity for the given Mesos cluster pool.  There is no
        guarantee that the actual capacity will equal the target capacity.
        """
        return sum(group.target_capacity for group in self.resource_groups)

    @property
    def fulfilled_capacity(self) -> float:
        """ The fulfilled capacity is the *actual* weighted capacity for the given Mesos cluster pool at a particular
        point in time.  This may be equal to, above, or below the :attr:`target_capacity`, depending on the availability
        and state of AWS at the time.  In general, once the cluster has reached equilibrium, the fulfilled capacity will
        be greater than or equal to the target capacity.
        """
        return sum(group.fulfilled_capacity for group in self.resource_groups)

    @timed_cached_property(CACHE_TTL_SECONDS)
    def _agents(self) -> Sequence[Dict]:
        response = mesos_post(self.api_endpoint, 'slaves').json()
        return [
            agent
            for agent in response['slaves']
            if agent.get('attributes', {}).get('pool', 'default') == self.pool
        ]

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

    def get_instances_by_resource_group(self) -> DefaultDict[str, List[PoolInstance]]:
        agent_id_to_task_count = self._count_tasks_per_agent()
        ip_to_agent = {agent_pid_to_ip(agent['pid']): agent for agent in self._agents}
        resource_group_to_instances: DefaultDict[str, List[PoolInstance]] = defaultdict(list)

        for group in self.resource_groups:
            for instance_dict in ec2_describe_instances(instance_ids=group.instance_ids):
                instance_ip = instance_dict.get('PrivateIpAddress')
                agent = ip_to_agent.get(instance_ip)

                pool_instance = PoolInstance(
                    instance_id=instance_dict['InstanceId'],
                    state=self._get_instance_state(instance_ip, agent),
                    task_count=agent_id_to_task_count[agent['id']] if agent else 0,
                    market=get_instance_market(instance_dict),
                    instance_dict=instance_dict,
                    agent=agent,
                )
                resource_group_to_instances[group.id].append(pool_instance)

        return resource_group_to_instances

    def get_instances(self) -> List[PoolInstance]:
        return [instance for instances in self.get_instances_by_resource_group().values() for instance in instances]

    def _get_instance_state(self, instance_ip: Optional[str], agent: Optional[Dict]) -> str:
        if not instance_ip:
            return MesosAgentState.UNKNOWN
        elif not agent:
            return MesosAgentState.ORPHANED
        elif allocated_cpu_resources(agent) == 0:
            return MesosAgentState.IDLE
        else:
            return MesosAgentState.RUNNING

    def _count_tasks_per_agent(self):
        """Given a list of mesos tasks, return a count of tasks per agent"""
        agent_id_to_task_count = defaultdict(int)
        for task in self._tasks:
            if task['state'] == 'TASK_RUNNING':
                agent_id_to_task_count[task['slave_id']] += 1
        return agent_id_to_task_count
