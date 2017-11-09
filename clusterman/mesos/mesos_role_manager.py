import os
import socket
from bisect import bisect
from collections import defaultdict

import requests
import staticconf
import yaml
from cached_property import timed_cached_property

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import MarketProtectedException
from clusterman.exceptions import MesosRoleManagerError
from clusterman.exceptions import ResourceGroupProtectedException
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.constants import ROLE_NAMESPACE
from clusterman.mesos.spot_fleet_resource_group import load_spot_fleets_from_s3
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.mesos.util import find_largest_capacity_market
from clusterman.mesos.util import get_resource_value
from clusterman.util import get_clusterman_logger


# TODO (CLUSTERMAN-112) these should be customizable
ROLE_CONFIG_DIR = '/nail/srv/configs/clusterman-roles'
DEFAULT_ROLE_CONFIG = ROLE_CONFIG_DIR + '/{role}/config.yaml'
SERVICES_FILE = '/nail/etc/services/services.yaml'
MIN_CAPACITY_PER_GROUP = 1
logger = get_clusterman_logger(__name__)


def get_roles_in_cluster(cluster):
    all_roles = os.listdir(ROLE_CONFIG_DIR)
    cluster_roles = []
    for role in all_roles:
        role_file = DEFAULT_ROLE_CONFIG.format(role=role)
        with open(role_file) as f:
            config = yaml.load(f)
            if cluster in config['mesos']:
                cluster_roles.append(role)
    return cluster_roles


def load_configs_for_cluster(cluster, role):
    role_config_file = DEFAULT_ROLE_CONFIG.format(role=role)
    with open(role_config_file) as f:
        all_configs = yaml.load(f)
    role_namespace = ROLE_NAMESPACE.format(role=role)
    staticconf.DictConfiguration(all_configs['mesos'][cluster], namespace=role_namespace)
    del all_configs['mesos']
    staticconf.DictConfiguration(all_configs, namespace=role_namespace)


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
        load_configs_for_cluster(self.cluster, self.role)

        role_config = staticconf.NamespaceReaders(ROLE_NAMESPACE.format(role=self.role))

        mesos_master_discovery_label = staticconf.read_string(f'mesos_clusters.{cluster}.leader_service')
        self.min_capacity = role_config.read_int('defaults.min_capacity')
        self.max_capacity = role_config.read_int('defaults.max_capacity')

        with open(SERVICES_FILE) as f:
            services = yaml.load(f)
        self.api_endpoint = 'http://{host}:{port}/api/v1'.format(
            host=services[mesos_master_discovery_label]['host'],
            port=services[mesos_master_discovery_label]['port'],
        )

        self.resource_groups = load_spot_fleets_from_s3(
            role_config.read_string('resource_groups.s3.bucket'),
            role_config.read_string('resource_groups.s3.prefix'),
            role=self.role,
        )

    def modify_target_capacity(self, new_target_capacity):
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
        if not self.resource_groups:
            raise MesosRoleManagerError('No resource groups available')
        orig_target_capacity = self.target_capacity
        new_target_capacity = self._constrain_target_capacity(new_target_capacity)

        for i, target in self._compute_new_resource_group_targets(new_target_capacity):
            self.resource_groups[i].modify_target_capacity(target)
        if new_target_capacity <= orig_target_capacity:
            self.prune_excess_fulfilled_capacity(new_target_capacity)
        return new_target_capacity

    def get_resource_utilization(self, resource_name):
        """Get the current amount of the given resource in use on each agent with this Mesos role.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: dict of agent_id -> float of resource utilization
        """
        resource_util = defaultdict(float)
        for agent in self._agents:
            agent_id = agent['agent_info']['id']['value']
            value = get_resource_value(agent.get('allocated_resources', []), resource_name)
            resource_util[agent_id] = value
        return resource_util

    def get_total_resources(self, resource_name):
        """Get the total amount of the given resource for this Mesos role.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        total = 0
        for agent in self._agents:
            total += get_resource_value(agent['total_resources'], resource_name)
        return total

    def get_average_resource_utilization(self, resource_name):
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns: float
        """
        total = self.get_total_resources(resource_name)
        if total == 0:
            return 0
        used = sum(self.get_resource_utilization(resource_name).values())
        return used / total

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

    def prune_excess_fulfilled_capacity(self, new_target_capacity=None):
        """ Decrease the capacity in the cluster; we only remove idle instances (i.e., instances that have
        no resources allocated to tasks).  We remove instances from the markets that have the largest fulfilled
        capacity first, so as to maintain balance across all the different spot groups.

        :param new_target_capacity: the desired target capacity for the cluster
        :raises MesosRoleManagerError: if the desired capacity is not in [self.min_capacity, self.target_capacity)
        """
        target_capacity = new_target_capacity or self.target_capacity
        if self.fulfilled_capacity <= target_capacity:
            return

        idle_agents = self._idle_agents_by_market()
        logger.info(f'Idle agents found: {list(idle_agents.values())}')
        # We can only reduce markets that have idle agents, so filter by the list of idle_agent keys
        idle_market_capacities = self._get_market_capacities(market_filter=idle_agents.keys())

        # Iterate through all of the idle agents and mark one at a time for removal; we remove an arbitrary idle
        # instance from the available market with the largest weight
        initial_capacity = self.fulfilled_capacity
        curr_capacity, marked_instances = initial_capacity, defaultdict(list)
        while curr_capacity > new_target_capacity:
            market_to_shrink, available_capacity = find_largest_capacity_market(idle_market_capacities)
            # It's possible too many agents have allocated resources, so we conservatively do not kill any running jobs
            if available_capacity == 0:
                logger.warn('No idle instances left to remove; aborting')
                break

            # Try to mark the instance for removal; this could fail in a few different ways:
            #  1) Something is wrong with the instance itself (e.g., it's not actually in a cluster)
            #  2) The instance market can't be reduced further
            #  3) The resource group the instance belongs to can't be reduced further
            # In each of the cases, the instance has been removed from consideration and we jump to the next iteration
            try:
                rem_capacity = curr_capacity - new_target_capacity
                weight = self._mark_instance_for_removal(idle_agents, marked_instances, market_to_shrink, rem_capacity)
            except MesosRoleManagerError as err:
                logger.warn(err)
                continue
            except MarketProtectedException:
                del idle_market_capacities[market_to_shrink]
                continue
            except ResourceGroupProtectedException:
                continue

            # Now that we've successfully marked the instance for removal, adjust our remaining weights
            idle_market_capacities[market_to_shrink] -= weight
            curr_capacity -= weight

        # Terminate the marked instances; it's possible that not all instances will be terminated
        all_terminated_instances = []
        for group, instances in marked_instances.items():
            terminated_instances = group.terminate_instances_by_id(instances)
            all_terminated_instances.extend(terminated_instances)
        logger.info(f'The following instances have been terminated: {all_terminated_instances}')

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

    def _mark_instance_for_removal(self, idle_agents, marked_instances, instance_market, rem_capacity):
        """ Attempt to mark an instance for removal from the cluster

        :param idle_agents: a mapping from market -> idle agents in that market
        :param marked_instances: a mapping from market -> current marked instances
        :param instance_market: the market we would like to remove the instance from
        :param curr_capacity: the current fulfilled capacity of the cluster, given the instances we've marked to remove
        :returns: the weight of the instance we've marked for removal
        :raises MesosRoleManagerError: if there is some problem removing the instance from its resource_group
        :raises MarketProtectedException: if we are unable to remove any further instances from that market
        :raises ResourceGroupProtectedException: if we are unable to remove any more instances from the resource group
        """

        # If the market has no further idle agents, remove it from consideration
        if not idle_agents[instance_market]:
            raise MarketProtectedException
        instance = idle_agents[instance_market].pop()
        instance_group = self._find_resource_group(instance)
        if not instance_group:
            raise MesosRoleManagerError('Could not find instance {instance} in any resource group')
        instance_weight = instance_group.market_weight(instance_market)

        # Make sure the instance we try to remove will not violate cluster bounds
        if instance_weight > rem_capacity:
            raise MarketProtectedException
        # Make sure we don't remove all instances from a resource group
        if len(marked_instances[instance_group]) == len(instance_group.instances) - 1:
            raise ResourceGroupProtectedException

        marked_instances[instance_group].append(instance)
        return instance_weight

    def _find_resource_group(self, instance):
        """ Find the resource group that an instance belongs to """
        for group in self.resource_groups:
            if instance in group.instances:
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
        idle_agents = [
            socket.gethostbyname(agent['agent_info']['hostname'])
            for agent in self._agents
            if allocated_cpu_resources(agent) == 0
        ]

        # Turn the IP address from the Mesos API into an AWS InstanceId
        idle_agents_by_market = defaultdict(list)
        for instance in ec2_describe_instances(filters=[{'Name': 'private-ip-address', 'Values': idle_agents}]):
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
    def _agents(self):
        response = requests.post(
            self.api_endpoint,
            json={'type': 'GET_AGENTS'},
            headers={'user-agent': 'clusterman'},
        )
        if not response.ok:
            raise MesosRoleManagerError(f'Could not get instances from Mesos master:\n{response.text}')

        agents = []
        for agent in response.json()['get_agents']['agents']:
            for attr in agent['agent_info'].get('attributes', []):
                if attr['name'] == 'role' and self.role == attr['text']['value']:
                    agents.append(agent)
                    break  # once we've generated a valid agent, don't need to loop through the rest of its attrs
        return agents
