from bisect import bisect
from collections import defaultdict

import staticconf
from cached_property import timed_cached_property

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import MesosRoleManagerError
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.constants import ROLE_NAMESPACE
from clusterman.mesos.spot_fleet_resource_group import load_spot_fleets_from_s3
from clusterman.mesos.util import find_largest_capacity_market
from clusterman.mesos.util import get_mesos_state
from clusterman.mesos.util import get_total_resource_value
from clusterman.mesos.util import mesos_post
from clusterman.mesos.util import MesosAgentState
from clusterman.util import get_clusterman_logger


MIN_CAPACITY_PER_GROUP = 1
logger = get_clusterman_logger(__name__)


class MesosRoleManager:
    """ The MesosRoleManager object provides a consistent interface to the infrastructure that underpins a particular
    Mesos role.  Specifically, it allows users to interact with the Mesos master (querying the number of agents in the
    cluster, and what resources are available/allocated, for example) as well as to modify the capacity available to the
    Mesos role.  Since many different types of hosts may be present in a Mesos cluster, this object refers to a list of
    abstract :class:`MesosRoleResourceGroup <clusterman.mesos.mesos_role_resource_group.MesosRoleResourceGroup>` objects
    to modify the underlying infrastructure.

    One major assumption the MesosRoleManager makes currently is that the underlying infrastructure for a particular
    role belongs completely to that role; in other words, at present no roles are co-located on the same physical
    hardware.  This assumption is subject to change in the future.

    .. note:: Values returned from MesosRoleManager functions may be cached to limit requests made to the Mesos masters
       or AWS API endpoints.
    """

    def __init__(self, cluster, role):
        self.cluster = cluster
        self.role = role

        role_config = staticconf.NamespaceReaders(ROLE_NAMESPACE.format(role=self.role))

        mesos_master_fqdn = staticconf.read_string(f'mesos_clusters.{cluster}.fqdn')
        self.min_capacity = role_config.read_int('scaling_limits.min_capacity')
        self.max_capacity = role_config.read_int('scaling_limits.max_capacity')

        self.api_endpoint = f'http://{mesos_master_fqdn}:5050/'
        logger.info(f'Connecting to Mesos masters at {self.api_endpoint}')

        self.resource_groups = load_spot_fleets_from_s3(
            role_config.read_string('mesos.resource_groups.s3.bucket'),
            role_config.read_string('mesos.resource_groups.s3.prefix'),
            role=self.role,
        )

        logger.info('Loaded resource groups: {ids}'.format(ids=[group.id for group in self.resource_groups]))

    def modify_target_capacity(self, new_target_capacity, dry_run=False):
        """ Change the desired :attr:`target_capacity` of the resource groups belonging to this role.

        Capacity changes are roughly evenly distributed across the resource groups to ensure that
        instances are diversified in the cluster

        :param new_target_capacity: the desired target capacity for the cluster and role

        .. note:: It may take some time (up to a few minutes) for changes in the target capacity to be reflected in
           :attr:`fulfilled_capacity`.  Once the capacity has equilibrated, the fulfilled capacity and the target
           capacity may not exactly match, but the fulfilled capacity will never be under the target (for example, if
           there is no combination of instances that evenly sum to the desired target capacity, the final fulfilled
           capacity will be slightly above the target capacity)
        """
        if dry_run:
            logger.warn('Running in "dry-run" mode; cluster state will not be modified')
        if not self.resource_groups:
            raise MesosRoleManagerError('No resource groups available')

        orig_target_capacity = self.target_capacity
        new_target_capacity = self._constrain_target_capacity(new_target_capacity)

        for i, target in self._compute_new_resource_group_targets(new_target_capacity):
            self.resource_groups[i].modify_target_capacity(target, dry_run=dry_run)
        if new_target_capacity <= orig_target_capacity:
            self.prune_excess_fulfilled_capacity(dry_run)
        return new_target_capacity

    def get_resource_allocation(self, resource_name):
        """Get the total amount of the given resource currently allocated for this Mesos role.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        return get_total_resource_value(self.agents, 'used_resources', resource_name)

    def get_resource_total(self, resource_name):
        """Get the total amount of the given resource for this Mesos role.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        return get_total_resource_value(self.agents, 'resources', resource_name)

    def get_percent_resource_allocation(self, resource_name):
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        total = self.get_resource_total(resource_name)
        used = self.get_resource_allocation(resource_name)
        return used / total if total else 0

    def _constrain_target_capacity(self, target_capacity):
        """ Ensure that the desired target capacity is within the specified bounds for the cluster """
        min_capacity = max(self.min_capacity, MIN_CAPACITY_PER_GROUP * len(self.resource_groups))
        if target_capacity > self.max_capacity:
            new_target_capacity = self.max_capacity
        elif target_capacity < min_capacity:
            new_target_capacity = min_capacity
        else:
            new_target_capacity = target_capacity

        if target_capacity != new_target_capacity:
            logger.warn(f'Requested target capacity {target_capacity}; constraining to {new_target_capacity}')
        return new_target_capacity

    def prune_excess_fulfilled_capacity(self, dry_run=False):
        """ Decrease the capacity in the cluster; we only remove idle instances (i.e., instances that have
        no resources allocated to tasks).  We remove instances from the markets that have the largest fulfilled
        capacity first, so as to maintain balance across all the different spot groups.
        """
        curr_capacity, target_capacity = self.fulfilled_capacity, self.target_capacity
        if curr_capacity <= target_capacity:
            return []

        idle_agents = self._idle_agents_by_market()
        logger.debug(f'Idle agents found: {dict(idle_agents)}')

        # We can only reduce markets that have idle agents, so filter by the list of idle_agent keys
        if not idle_agents:
            return []
        idle_market_capacities = self._get_market_capacities(market_filter=idle_agents.keys())
        rem_group_capacities = {group.id: group.fulfilled_capacity for group in self.resource_groups}

        # Iterate through all of the idle agents and mark one at a time for removal; we remove an arbitrary idle
        # instance from the available market with the largest weight
        marked_instances = defaultdict(list)
        while curr_capacity > target_capacity:
            market_to_shrink, available_capacity = find_largest_capacity_market(idle_market_capacities)
            # It's possible too many agents have allocated resources, so we conservatively do not kill any running jobs
            if available_capacity == 0:
                logger.debug('No idle instances left to remove; aborting')
                break

            # Try to mark the instance for removal; this could fail in a few different ways:
            #  1) The market we want to shrink can't be reduced further
            #  2) Something is wrong with the instance itself (e.g., it's not actually in a cluster)
            #  3) The resource group the instance belongs to can't be reduced further
            # In each of the cases, the instance has been removed from consideration and we jump to the next iteration
            if not idle_agents[market_to_shrink]:  # case 1
                del idle_market_capacities[market_to_shrink]
                continue

            instance = idle_agents[market_to_shrink].pop()
            instance_group = self._find_resource_group(instance)
            if not instance_group:  # case 2
                logger.warn(f'Could not find instance {instance} in any resource group')
                continue
            instance_weight = instance_group.market_weight(market_to_shrink)

            # Make sure we don't make a resource group go below its target capacity
            if rem_group_capacities[instance_group.id] - instance_weight < instance_group.target_capacity:  # case 3
                continue

            marked_instances[instance_group].append(instance)
            rem_group_capacities[instance_group.id] -= instance_weight
            idle_market_capacities[market_to_shrink] -= instance_weight
            curr_capacity -= instance_weight

        # Terminate the marked instances; it's possible that not all instances will be terminated
        all_terminated_instance_ids = []
        if not dry_run:
            for group, instances in marked_instances.items():
                terminated_instances = group.terminate_instances_by_id(instances)
                all_terminated_instance_ids.extend(terminated_instances)
        else:
            all_terminated_instance_ids = [i for instances in marked_instances.values() for i in instances]

        logger.info(f'The following instances have been terminated: {all_terminated_instance_ids}')
        return all_terminated_instance_ids

    def _compute_new_resource_group_targets(self, new_target_capacity):
        """ Compute a balanced distribution of target capacities for the resource groups in the cluster

        :param new_target_capacity: the desired new target capacity that needs to be distributed
        :returns: A list of (resource group index, target_capacity) pairs
        """

        # If we're scaling down the logic is identical but reversed, so we multiply everything by -1
        coeff = -1 if new_target_capacity < self.target_capacity else 1
        new_targets_with_indices = sorted(
            [(i, coeff * group.target_capacity) for i, group in enumerate(self.resource_groups)],
            key=lambda x: (x[1], x[0]),
        )
        if new_target_capacity == self.target_capacity:
            return new_targets_with_indices

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

        return zip(original_indices, [coeff * target for target in new_targets])

    def _find_resource_group(self, instance):
        """ Find the resource group that an instance belongs to """
        for group in self.resource_groups:
            if instance in group.instance_ids:
                return group
        return None

    def _get_market_capacities(self, market_filter=None):
        """ Return the total (fulfilled) capacities in the cluster across all resource groups """
        total_market_capacities = defaultdict(float)
        for group in self.resource_groups:
            for market, capacity in group.market_capacities.items():
                if not market_filter or market in market_filter:
                    total_market_capacities[market] += capacity
        return total_market_capacities

    def _idle_agents_by_market(self):
        """ Find a list of idle agents, grouped by the market they belong to """
        idle_agents_by_market = defaultdict(list)
        for group in self.resource_groups:
            for instance in ec2_describe_instances(instance_ids=group.instance_ids):
                mesos_state = get_mesos_state(instance, self.agents)
                if mesos_state in {MesosAgentState.ORPHANED, MesosAgentState.IDLE}:
                    idle_agents_by_market[get_instance_market(instance)].append(instance['InstanceId'])
        return idle_agents_by_market

    @property
    def target_capacity(self):
        """ The target capacity is the *desired* weighted capacity for the given Mesos cluster role.  There is no
        guarantee that the actual capacity will equal the target capacity.
        """
        return sum(group.target_capacity for group in self.resource_groups)

    @property
    def fulfilled_capacity(self):
        """ The fulfilled capacity is the *actual* weighted capacity for the given Mesos cluster role at a particular
        point in time.  This may be equal to, above, or below the :attr:`target_capacity`, depending on the availability
        and state of AWS at the time.  In general, once the cluster has reached equilibrium, the fulfilled capacity will
        be greater than or equal to the target capacity.
        """
        return sum(group.fulfilled_capacity for group in self.resource_groups)

    @timed_cached_property(CACHE_TTL_SECONDS)
    def agents(self):
        response = mesos_post(self.api_endpoint, 'slaves').json()
        return [
            agent
            for agent in response['slaves']
            if agent.get('attributes', {}).get('role', 'default') == self.role
        ]
