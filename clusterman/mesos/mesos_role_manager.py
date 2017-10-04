from collections import defaultdict
from operator import itemgetter

import requests
import yaml
from cachetools.func import ttl_cache

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import MesosRoleManagerError
from clusterman.util import get_clusterman_logger


logger = get_clusterman_logger(__name__)
MESOS_CACHE_TTL = 10


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
    def __init__(self, name, min_capacity, max_capacity, services_file, master_service_label):
        self.name = name
        self.resource_groups = []
        with open(services_file) as f:
            services = yaml.load(f)
        self.api_endpoint = 'http://{host}:{port}/api/v1'.format(
            host=services[master_service_label]['host'],
            port=services[master_service_label]['port'],
        )
        self.min_capacity = min_capacity
        self.max_capacity = max_capacity

    def modify_target_capacity(self, new_target_capacity):
        curr_target_capacity = self.target_capacity
        if new_target_capacity > curr_target_capacity:
            self._add_capacity(new_target_capacity - curr_target_capacity)
        elif new_target_capacity < curr_target_capacity:
            self._remove_capacity(curr_target_capacity - new_target_capacity)

    def _add_capacity(self, delta):
        if self.target_capacity + delta > self.max_capacity:
            delta = self.max_capacity - self.target_capacity
        delta_per_resource = delta / len(self.resource_groups)
        for resource_group in self.resource_groups:
            resource_group.modify_target_capacity(delta_per_resource)

    def _remove_capacity(self, delta):
        idle_agents = self._idle_agents_by_market()
        total_market_capacities = defaultdict(float)
        for group in self.resource_groups:
            for market, capacity in group.market_capacities:
                if market in idle_agents:
                    total_market_capacities[market] += capacity

        remaining_delta = delta
        instances_to_remove = defaultdict(list)
        while remaining_delta > 0:
            market_to_reduce, available_capacity = max(total_market_capacities.items(), key=itemgetter(1))
            if available_capacity == 0:
                logger.warn("No idle instances left to remove; aborting")
                break

            instance = idle_agents[market_to_reduce].pop()
            instance_group = self._find_resource_group(instance)
            instance_weight = instance_group.market_weight(market_to_reduce)

            if self.total_capacity - instance_weight < self.min_capacity:
                continue

            instances_to_remove[instance_group].append(instance)
            remaining_delta -= instance_weight

        for group, instances in instances_to_remove.items():
            group.terminate_instances_by_id(instances)

    def _find_resource_group(self, instance):
        for group in self.resource_groups:
            if instance in group.instances:
                return group

    def _idle_agents_by_market(self):
        idle_agents = [
            agent['agent_info']['hostname']
            for agent in self._agents
            if allocated_cpu_resources(agent) == 0
        ]

        return {
            get_instance_market(instance): instance['InstanceId']
            for instance in ec2_describe_instances(filters={'private-dns-name': idle_agents})
        }

    @property
    def target_capacity(self):
        return sum(group.target_capacity for group in self.resource_groups)

    @property
    @ttl_cache(ttl=MESOS_CACHE_TTL)
    def _agents(self):
        response = requests.post(self.api_endpoint, data={'type': 'GET_AGENTS'})
        if not response.ok:
            raise MesosRoleManagerError(f'Could not get instances from Mesos master:\n{response.text}')

        for agent in response.json()['get_agents']['agents']:
            for attr in agent['agent_info']['attributes']:
                if attr['name'] == 'role' and self.name == attr['text']['value']:
                    yield agent
                    break  # once we've generated a valid agent, don't need to loop through the rest of its attrs
