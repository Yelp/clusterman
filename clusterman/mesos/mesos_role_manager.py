import socket
from collections import defaultdict

import requests
import staticconf
import yaml
from cachetools.func import ttl_cache

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import MarketProtectedException
from clusterman.exceptions import MesosRoleManagerError
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
        if not self.resource_groups:
            raise MesosRoleManagerError('No resource groups available')
        new_target_capacity = self._constrain_target_capacity(new_target_capacity)

        curr_target_capacity = self.target_capacity
        if new_target_capacity > curr_target_capacity:
            self._increase_capacity(new_target_capacity)
        elif new_target_capacity < curr_target_capacity:
            self._decrease_capacity(new_target_capacity)

    def _constrain_target_capacity(self, target_capacity):
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
        if not self.target_capacity < new_target_capacity <= self.max_capacity:
            raise MesosRoleManagerError(f'{new_target_capacity} is not in range '
                                        '({self.target_capacity}, {self.max_capacity}]')
        capacity_per_resource, remainder = divmod(new_target_capacity, len(self.resource_groups))

        for i, resource_group in enumerate(self.resource_groups):
            integer_resource_capacity = capacity_per_resource + (1 if i < remainder else 0)
            resource_group.modify_target_capacity(integer_resource_capacity)

    def _decrease_capacity(self, new_target_capacity):
        if not self.min_capacity <= new_target_capacity < self.target_capacity:
            raise MesosRoleManagerError(f'{new_target_capacity} is not in range '
                                        '({self.min_capacity}, {self.target_capacity}]')

        idle_agents = self._idle_agents_by_market()
        # We can only reduce markets that have idle agents, so filter by the list of idle_agent keys
        idle_market_capacities = self._get_market_capacities(market_filter=idle_agents.keys())

        curr_capacity = self.fulfilled_capacity
        marked_instances = defaultdict(list)
        while curr_capacity > new_target_capacity:
            market_to_shrink, available_capacity = find_largest_capacity_market(
                idle_market_capacities,
                threshold=curr_capacity - new_target_capacity,
            )
            if available_capacity == 0:
                logger.warn('No idle instances left to remove; aborting')
                break

            try:
                instance = idle_agents[market_to_shrink].pop()
                weight = self._mark_instance_for_removal(instance, marked_instances, market_to_shrink, curr_capacity)
            except MesosRoleManagerError as err:
                logger.warn(err)
                continue
            except MarketProtectedException:
                del idle_market_capacities[market_to_shrink]
                continue

            idle_market_capacities[market_to_shrink] -= weight
            curr_capacity -= weight

        all_terminated_instances = []
        for group, instances in marked_instances.items():
            terminated_instances = group.terminate_instances_by_id(instances)
            all_terminated_instances.extend(terminated_instances)

        if self.target_capacity != new_target_capacity:
            logger.warn(f'New target capacity is {self.target_capacity} instead of requested {new_target_capacity}')
        logger.info(f'The following instances have been terminated: {all_terminated_instances}')

    def _mark_instance_for_removal(self, instance, marked_instances, instance_market, curr_capacity):
        instance_group = self._find_resource_group(instance)
        if not instance_group:
            raise MesosRoleManagerError('Could not find instance {instance} in any resource group')
        instance_weight = instance_group.market_weight(instance_market)

        if curr_capacity - instance_weight < self.min_capacity:
            raise MarketProtectedException()
        if len(marked_instances[instance_group]) == len(instance_group.instances) - 1:
            raise MarketProtectedException()

        marked_instances[instance_group].append(instance)
        return instance_weight

    def _find_resource_group(self, instance):
        for group in self.resource_groups:
            if instance in group.instances:
                return group
        return None

    def _get_market_capacities(self, market_filter=None):
        total_market_capacities = defaultdict(float)
        for group in self.resource_groups:
            for market, capacity in group.market_capacities.items():
                if not market_filter or market in market_filter:
                    total_market_capacities[market] += capacity
        return total_market_capacities

    def _idle_agents_by_market(self):
        idle_agents = [
            socket.gethostbyname(agent['agent_info']['hostname'])
            for agent in self._agents
            if allocated_cpu_resources(agent) == 0
        ]

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

    @property
    @ttl_cache(ttl=CACHE_TTL)
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
