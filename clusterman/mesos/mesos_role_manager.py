import os
import socket
from collections import defaultdict

import requests
import staticconf
import yaml
from cached_property import timed_cached_property
from sortedcontainers import SortedList

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


ROLE_CONFIG_DIR = '/nail/srv/configs/clusterman-roles'
DEFAULT_ROLE_CONFIG = ROLE_CONFIG_DIR + '/{name}/config.yaml'
SERVICES_FILE = '/nail/etc/services/services.yaml'
logger = get_clusterman_logger(__name__)


def get_roles_in_cluster(cluster):
    all_roles = os.listdir(ROLE_CONFIG_DIR)
    cluster_roles = []
    for role in all_roles:
        role_file = DEFAULT_ROLE_CONFIG.format(name=role)
        with open(role_file) as f:
            config = yaml.load(f)
            if cluster in config['mesos']:
                cluster_roles.append(role)
    return cluster_roles


def load_configs_for_cluster(cluster, role):
    role_config_file = DEFAULT_ROLE_CONFIG.format(name=role)
    with open(role_config_file) as f:
        all_configs = yaml.load(f)
    role_namespace = ROLE_NAMESPACE.format(role=role)
    staticconf.DictConfiguration(all_configs['mesos'][cluster], namespace=role_namespace)
    del all_configs['mesos']
    staticconf.DictConfiguration(all_configs, namespace=role_namespace)


class MesosRoleManager:
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
            return self._increase_capacity(new_target_capacity)
        elif new_target_capacity < self.target_capacity:
            return self._decrease_capacity(new_target_capacity)
        else:
            return self.target_capacity

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
        for i, target in self._compute_new_resource_group_targets(new_target_capacity):
            self.resource_groups[i].modify_target_capacity(target)
        return new_target_capacity

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
        all_terminated_instances, total_terminated_weight = [], 0
        for group, instances in marked_instances.items():
            terminated_instances, terminated_weight = group.terminate_instances_by_id(instances)
            all_terminated_instances.extend(terminated_instances)
            total_terminated_weight += terminated_weight

        actual_new_target_capacity = initial_capacity - total_terminated_weight
        if actual_new_target_capacity != new_target_capacity:
            logger.warn(f'New target capacity is {actual_new_target_capacity} instead of {new_target_capacity}')
        logger.info(f'The following instances have been terminated: {all_terminated_instances}')
        return actual_new_target_capacity

    def _compute_new_resource_group_targets(self, new_target_capacity):
        """ Compute a balanced distribution of target capacities for the resource groups in the cluster

        :param new_target_capacity: the desired new target capacity that needs to be distributed
        :returns: A (sorted) list of (resource group index, target_capacity) pairs
        """
        new_targets = SortedList(
            [[i, group.target_capacity] for i, group in enumerate(self.resource_groups)],
            key=lambda x: (x[1], x[0]),
        )
        if new_target_capacity == self.target_capacity:
            return new_targets

        num_groups_to_increase = len(self.resource_groups)
        while True:
            # If any resource groups are currently above the new target "uniform" capacity, we need to recompute
            # the target while taking into account the over-supplied resource groups.  We never decrease the
            # capacity of a resource group here, so we just find the first index is above the desired target
            # and remove those from consideration.  We have to repeat this multiple times, as new resource
            # groups could be over the new "uniform" capacity after we've subtracted the overage value
            capacity_per_group, remainder = divmod(new_target_capacity, num_groups_to_increase)
            pos = new_targets.bisect([len(self.resource_groups), capacity_per_group])
            overage = sum(new_targets[i][1] for i in range(pos, num_groups_to_increase))

            if overage == 0:
                for i in range(num_groups_to_increase):
                    new_targets[i][1] = capacity_per_group + (1 if i < remainder else 0)
                break

            new_target_capacity -= overage
            num_groups_to_increase = pos

        return new_targets

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
        return sum(group.target_capacity for group in self.resource_groups)

    @property
    def fulfilled_capacity(self):
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

    def get_resource_utilization(self, resource_name):
        """Get the current amount of the given resource in use on each agent with this Mesos role.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns dict of agent_id -> float of resource utilization
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
        :returns float
        """
        total = 0
        for agent in self._agents:
            total += get_resource_value(agent['total_resources'], resource_name)
        return total

    def get_average_resource_utilization(self, resource_name):
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Mesos (e.g. 'cpus', 'mem', 'disk')
        :returns float
        """
        total = self.get_total_resources(resource_name)
        if total == 0:
            return 0
        used = sum(self.get_resource_utilization(resource_name).values())
        return used / total
