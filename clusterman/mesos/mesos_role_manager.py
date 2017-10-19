import socket
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
from clusterman.mesos.constants import CACHE_TTL
from clusterman.mesos.spot_fleet_resource_group import load_spot_fleets_from_s3
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.mesos.util import find_largest_capacity_market
from clusterman.util import get_clusterman_logger


DEFAULT_ROLE_CONFIG = '/nail/srv/configs/clusterman-roles/{name}/config.yaml'
SERVICES_FILE = '/nail/etc/services/services.yaml'
NAMESPACE = 'role_config'
logger = get_clusterman_logger(__name__)


class MesosRoleManager:
    def __init__(self, name, role_config_file=None):
        self.name = name
        role_config_file = role_config_file or DEFAULT_ROLE_CONFIG.format(name=self.name)
        staticconf.YamlConfiguration(role_config_file, namespace=NAMESPACE)

        self.min_capacity = staticconf.read_int('defaults.min_capacity', namespace=NAMESPACE)
        self.max_capacity = staticconf.read_int('defaults.max_capacity', namespace=NAMESPACE)

        mesos_master_discovery_label = staticconf.read_string('mesos.master_discovery', namespace=NAMESPACE)
        with open(SERVICES_FILE) as f:
            services = yaml.load(f)
        self.api_endpoint = 'http://{host}:{port}/api/v1'.format(
            host=services[mesos_master_discovery_label]['host'],
            port=services[mesos_master_discovery_label]['port'],
        )

        self.resource_groups = load_spot_fleets_from_s3(
            staticconf.read_string('mesos.resource_groups.s3.bucket', namespace=NAMESPACE),
            staticconf.read_string('mesos.resource_groups.s3.prefix', namespace=NAMESPACE),
            role=self.name,
        )

    def modify_target_capacity(self, new_target_capacity):
        """ Change the target capacity of the resource groups belonging to this role.

        Capacity changes are roughly evenly distributed across the resource groups to ensure that
        instances are diversified in the cluster

        :param new_target_capacity: the desired target capacity for the cluster
            NOTE: the final cluster state may not exactly match the desired capacity, but it should never be less
        """
        if not self.resource_groups:
            raise MesosRoleManagerError('No resource groups available')
        new_target_capacity = self._constrain_target_capacity(new_target_capacity)

        # We have different scaling behavior based on whether we're increasing or decreasing the cluster size
        if new_target_capacity > self.target_capacity:
            self._increase_capacity(new_target_capacity)
        elif new_target_capacity < self.target_capacity:
            self._decrease_capacity(new_target_capacity)

    def _constrain_target_capacity(self, target_capacity):
        """ Ensure that the desired target capacity is within the specified bounds for the cluster """
        if target_capacity > self.max_capacity:
            new_target_capacity = self.max_capacity
        elif target_capacity < self.min_capacity:
            new_target_capacity = self.min_capacity
        else:
            new_target_capacity = target_capacity

        if target_capacity != new_target_capacity:
            logger.warn(f'Requested target capacity {target_capacity}; constraining to {new_target_capacity}')
        return new_target_capacity

    def _increase_capacity(self, new_target_capacity):
        """ Increase the capacity in the cluster; compute the capacity to add to each resource group
        to ensure that the cluster is as balanced as possible

        :param new_target_capacity: the desired target capacity for the cluster
        :raises MesosRoleManagerError: if the desired capacity is not in (self.target_capacity, self.max_capacity]
        """
        if not self.target_capacity < new_target_capacity <= self.max_capacity:
            raise MesosRoleManagerError(f'{new_target_capacity} is not in range '
                                        '({self.target_capacity}, {self.max_capacity}]')
        unfilled_capacity = new_target_capacity - self.target_capacity
        for i, target in self._compute_new_resource_group_targets(unfilled_capacity):
            self.resource_groups[i].modify_target_capacity(target)

    def _decrease_capacity(self, new_target_capacity):
        """ Decrease the capacity in the cluster; we only remove idle instances (i.e., instances that have
        no resources allocated to tasks).  We remove instances from the markets that have the largest fulfilled
        capacity first, so as to maintain balance across all the different spot groups.

        NOTE: there is a disparity between _increase_capacity and _decrease_capacity; in particular, _increase_capacity
        is based off target_capacity, but _decrease_capacity is based on fulfilled_capacity.  This is because the list
        of idle agents is drawn from the fulfilled capacity, so we cannot make decisions based on just the target
        capacity alone.  It also leads to some potential unpleasant race conditions, depending on how the fulfilled
        capacity changes between the idle_agents computation and the actual cluster modification calls.  The code has
        been architechted (as best as possible) to minimize effects from this disparity

        :param new_target_capacity: the desired target capacity for the cluster
        :raises MesosRoleManagerError: if the desired capacity is not in [self.min_capacity, self.target_capacity)
        """
        if not self.min_capacity <= new_target_capacity < self.target_capacity:
            raise MesosRoleManagerError(f'{new_target_capacity} is not in range '
                                        '({self.min_capacity}, {self.target_capacity}]')

        idle_agents = self._idle_agents_by_market()
        logger.info(f'Idle agents found: {list(idle_agents.values())}')
        # We can only reduce markets that have idle agents, so filter by the list of idle_agent keys
        idle_market_capacities = self._get_market_capacities(market_filter=idle_agents.keys())

        # Iterate through all of the idle agents and mark one at a time for removal; we remove an arbitrary idle
        # instance from the available market with the largest weight
        curr_capacity, marked_instances = self.fulfilled_capacity, defaultdict(list)
        while curr_capacity > new_target_capacity:
            market_to_shrink, available_capacity = find_largest_capacity_market(
                idle_market_capacities,
                threshold=curr_capacity - new_target_capacity,
            )
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
                weight = self._mark_instance_for_removal(idle_agents, marked_instances, market_to_shrink, curr_capacity)
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

        if self.target_capacity != new_target_capacity:
            logger.warn(f'New target capacity is {self.target_capacity} instead of requested {new_target_capacity}')
        logger.info(f'The following instances have been terminated: {all_terminated_instances}')

    def _compute_new_resource_group_targets(self, unfilled_capacity):
        """ Compute a balanced distribution of target capacities for the resource groups in the cluster

        :param unfilled_capacity: the additional capacity we need to distribute
        :returns: A (sorted) list of (resource group index, target_capacity) pairs
        """
        new_targets = [[i, group.target_capacity] for i, group in enumerate(self.resource_groups)]

        # Each iteration of the loop increases the target capacity of the resource group(s) with the lowest target
        # capacity to match the next-lowest target capacity.  This continues until all of the unfilled_capacity has
        # been distributed.
        for i in range(len(self.resource_groups)):
            if unfilled_capacity == 0:
                break

            # We sort resource groups first by target capacity, and then by index (sorting by index isn't strictly
            # necessary but it makes book-keeping and testing a little bit easier).  This operation is safe to do here
            # because our loop invariant is that all resource groups less than the current position have the same
            # target capacity
            new_targets.sort(key=lambda x: (x[1], x[0]))
            try:
                desired_delta = new_targets[i + 1][1] - new_targets[i][1]
            except IndexError:
                # If we've reached the end of the list of resource groups and still have unfilled capacity, we need to
                # distribute that evenly among the resource groups
                desired_delta = unfilled_capacity

            if desired_delta > 0:
                # Make sure we have enough unfilled capacity to bring up all of the resource groups to the next
                # desired capacity.  If we do not, then we just distribute the rest of the unfilled capacity
                # among these resource groups.
                increase_per_group, remainder = divmod(min((i + 1) * desired_delta, unfilled_capacity), (i + 1))
                for j in range(i + 1):
                    delta = increase_per_group + (1 if j < remainder else 0)
                    new_targets[j][1] += delta
                    unfilled_capacity -= delta
        # Return the list of target_capacities, now sorted in order of increasing index
        return sorted(new_targets)

    def _mark_instance_for_removal(self, idle_agents, marked_instances, instance_market, curr_capacity):
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
        if curr_capacity - instance_weight < self.min_capacity:
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
        return sum(group.target_capacity for group in self.resource_groups)

    @property
    def fulfilled_capacity(self):
        return sum(group.fulfilled_capacity for group in self.resource_groups)

    @timed_cached_property(ttl=CACHE_TTL)
    def _agents(self):
        response = requests.post(
            self.api_endpoint,
            data='{"type": "GET_AGENTS"}',
            headers={'user-agent': 'clusterman', 'Content-Type': 'application/json'},
        )
        if not response.ok:
            raise MesosRoleManagerError(f'Could not get instances from Mesos master:\n{response.text}')

        agents = []
        for agent in response.json()['get_agents']['agents']:
            for attr in agent['agent_info'].get('attributes', []):
                if attr['name'] == 'role' and self.name == attr['text']['value']:
                    agents.append(agent)
                    break  # once we've generated a valid agent, don't need to loop through the rest of its attrs
        return agents
