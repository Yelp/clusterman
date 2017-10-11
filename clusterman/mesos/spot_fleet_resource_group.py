import json
from collections import defaultdict

from cachetools.func import ttl_cache

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import ResourceGroupError
from clusterman.mesos.mesos_role_manager import MESOS_CACHE_TTL
from clusterman.mesos.mesos_role_resource_group import MesosRoleResourceGroup
from clusterman.mesos.mesos_role_resource_group import protect_unowned_instances
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


class SpotFleetResourceGroup(MesosRoleResourceGroup):

    def __init__(self, sfr_id):
        self.sfr_id = sfr_id
        self.market_weights = {  # Can't change WeightedCapacity of SFRs, so cache them here for frequent access
            get_instance_market(spec): spec['WeightedCapacity']
            for spec in self._configuration['SpotFleetRequestConfig']['LaunchSpecifications']
        }

    def market_weight(self, market):
        return self.market_weights[market]

    def modify_target_capacity(self, new_capacity, should_terminate=False):
        termination_policy = 'Default' if should_terminate else 'NoTermination'
        response = ec2.modify_spot_fleet_request(
            SpotFleetRequestId=self.sfr_id,
            TargetCapacity=int(new_capacity),
            ExcessCapacityTerminationPolicy=termination_policy,
        )
        if not response['Return']:
            raise ResourceGroupError("Could not change size of spot fleet: {resp}".format(
                resp=json.dumps(response),
            ))

    @protect_unowned_instances
    def terminate_instances_by_id(self, instance_ids, batch_size=500):
        if not instance_ids:
            logger.warn('No instances to terminate')
            return [], 0

        # Store the state of the spot fleet before we do anything
        original_fulfilled_capacity = self.fulfilled_capacity
        instance_weights = {
            instance['InstanceId']: self.market_weights[get_instance_market(instance)]
            for instance in ec2_describe_instances(instance_ids)
        }

        # AWS API recommends not terminating more than 1000 instances at a time, and to
        # terminate larger numbers in batches
        terminated_instance_ids = []
        for batch in range(0, len(instance_ids), batch_size):
            response = ec2.terminate_instances(InstanceIds=instance_ids[batch:batch + batch_size])
            terminated_instance_ids.extend([instance['InstanceId'] for instance in response['TerminatingInstances']])

        # It's possible that not every instance is terminated.  The most likely cause for this
        # is that AWS terminated the instance inbetween getting its status and the terminate_instances
        # request.  This is probably fine but let's log a warning just in case.
        missing_instances = set(instance_ids) - set(terminated_instance_ids)
        if missing_instances:
            logger.warn('Some instances could not be terminated; they were probably killed previously')
            logger.warn(f'Missing instances: {list(missing_instances)}')

        terminated_weight = sum(instance_weights[i] for i in terminated_instance_ids)
        self.modify_target_capacity(original_fulfilled_capacity - terminated_weight)

        logger.info(f'Terminated weight: {terminated_weight}; instances: {terminated_instance_ids}')
        return terminated_instance_ids, terminated_weight

    @property
    def id(self):
        return self.sfr_id

    @property
    @ttl_cache(ttl=MESOS_CACHE_TTL)
    def instances(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        return [
            instance['InstanceId']
            for page in ec2.get_paginator('describe_spot_fleet_instances').paginate(SpotFleetRequestId=self.sfr_id)
            for instance in page['ActiveInstances']
        ]

    @property
    def market_capacities(self):
        return {
            market: len(instances) * self.market_weights[market]
            for market, instances in self._instances_by_market.items()
        }

    @property
    def target_capacity(self):
        return self._configuration['SpotFleetRequestConfig']['TargetCapacity']

    @property
    def fulfilled_capacity(self):
        return self._configuration['SpotFleetRequestConfig']['FulfilledCapacity']

    @property
    def status(self):
        return self._configuration['SpotFleetRequestState']

    @property
    @ttl_cache(ttl=MESOS_CACHE_TTL)
    def _configuration(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        fleet_configuration = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[self.sfr_id])
        return fleet_configuration['SpotFleetRequestConfigs'][0]

    @property
    @ttl_cache(ttl=MESOS_CACHE_TTL)
    def _instances_by_market(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        instance_dict = defaultdict(list)
        for instance in ec2_describe_instances(self.instances):
            instance_dict[get_instance_market(instance)].append(instance)
        return instance_dict
