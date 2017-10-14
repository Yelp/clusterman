import random
import socket
from collections import defaultdict

import requests
import staticconf
import yaml
from cachetools.func import ttl_cache

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import MesosRoleManagerError
from clusterman.mesos.constants import CACHE_TTL
from clusterman.mesos.spot_fleet_resource_group import load_spot_fleets_from_s3
from clusterman.util import colored_status
from clusterman.util import get_clusterman_logger


DEFAULT_ROLE_CONFIG = '/nail/srv/configs/clusterman-roles/{name}/config.yaml'
SERVICES_FILE = '/nail/etc/services/services.yaml'
NAMESPACE = 'role_config'
logger = get_clusterman_logger(__name__)


def allocated_cpu_resources(agent):
    for resource in agent['agent_info'].get('allocated_resources', []):
        if resource['name'] == 'cpus':
            return resource['scalar']['value']
    return 0


def get_tag(instance, tag_name):
    for tag in instance['Tags']:
        if tag['Key'] == tag_name:
            return tag['Value']


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
        curr_target_capacity = self.target_capacity
        if new_target_capacity > curr_target_capacity:
            changed_target_capacity = self._increase_capacity(new_target_capacity)
        elif new_target_capacity < curr_target_capacity:
            changed_target_capacity = self._decrease_capacity(new_target_capacity)
        return changed_target_capacity

    def status(self, verbose):
        print('\n')
        print(f'Current status for the {self.name} cluster:\n')
        print('Resource groups:')
        for group in self.resource_groups:
            status_str = colored_status(
                group.status,
                active=('active',),
                changing=('modifying', 'submitted'),
                inactive=('cancelled', 'failed', 'cancelled_running', 'cancelled_terminating'),
            )
            print(f'\t{group.id}: {status_str} ({group.fulfilled_capacity} / {group.target_capacity})')
            if verbose:
                for instance in ec2_describe_instances(instance_ids=group.instances):
                    instance_status_str = colored_status(
                        instance['State']['Name'],
                        active=('running',),
                        changing=('pending',),
                        inactive=('shutting-down', 'terminated', 'stopping', 'stopped'),
                    )
                    instance_id = instance['InstanceId']
                    market = get_instance_market(instance)
                    try:
                        instance_ip = instance['PrivateIpAddress']
                    except KeyError:
                        instance_ip = None
                    print(f'\t - {instance_id} {market} ({instance_ip}): {instance_status_str}')
        print('\n')
        print(f'Total cluster capacity: {self.fulfilled_capacity} units out of {self.target_capacity}')
        print('\n')

    def _increase_capacity(self, new_target_capacity):
        if new_target_capacity > self.max_capacity:
            new_target_capacity = self.max_capacity
        capacity_per_resource, remainder = divmod(new_target_capacity, len(self.resource_groups))

        total_added_capacity = 0
        for i, resource_group in enumerate(self.resource_groups):
            integer_resource_capacity = capacity_per_resource + (1 if i < remainder else 0)
            total_added_capacity += integer_resource_capacity
            resource_group.modify_target_capacity(integer_resource_capacity)
        return total_added_capacity

    def _decrease_capacity(self, new_target_capacity):
        orig_fulfilled_capacity = self.fulfilled_capacity
        delta = orig_fulfilled_capacity - new_target_capacity
        idle_agents = self._idle_agents_by_market()
        available_market_capacities = defaultdict(float, {None: 0})
        for group in self.resource_groups:
            for market, capacity in group.market_capacities.items():
                if market in idle_agents:
                    available_market_capacities[market] += capacity

        remaining_delta = delta
        instances_to_remove = defaultdict(list)
        while remaining_delta > 0:
            market_to_reduce, available_capacity = max(
                ((m, c) for m, c in available_market_capacities.items() if c <= remaining_delta),
                key=lambda mc: (mc[1], random.random()),
            )

            if available_capacity == 0:
                logger.warn("No idle instances left to remove; aborting")
                break

            instance = idle_agents[market_to_reduce].pop()
            instance_group = self._find_resource_group(instance)
            instance_weight = instance_group.market_weight(market_to_reduce)
            available_market_capacities[market_to_reduce] -= instance_weight

            if self.target_capacity - instance_weight < self.min_capacity:
                continue
            if len(instances_to_remove[instance_group]) == len(instance_group.instances) - 1:
                continue

            instances_to_remove[instance_group].append(instance)
            remaining_delta -= instance_weight

        all_terminated_instances = []
        total_terminated_capacity = 0
        for group, instances in instances_to_remove.items():
            terminated_instances, terminated_capacity = group.terminate_instances_by_id(instances)
            total_terminated_capacity += terminated_capacity
            all_terminated_instances.append(terminated_instances)

        if total_terminated_capacity != delta:
            logger.warn(f'Only terminated {total_terminated_capacity} units out of {delta} requested')
        logger.info(f'The following instances have been terminated: {all_terminated_instances}')
        return orig_fulfilled_capacity - total_terminated_capacity

    def _find_resource_group(self, instance):
        for group in self.resource_groups:
            if instance in group.instances:
                return group

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
            for attr in agent['agent_info']['attributes']:
                if attr['name'] == 'role' and self.name == attr['text']['value']:
                    agents.append(agent)
                    break  # once we've generated a valid agent, don't need to loop through the rest of its attrs
        return agents
