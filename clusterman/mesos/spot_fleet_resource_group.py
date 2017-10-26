import json
from collections import defaultdict

from cached_property import timed_cached_property

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import s3
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import ResourceGroupError
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_role_resource_group import MesosRoleResourceGroup
from clusterman.mesos.mesos_role_resource_group import protect_unowned_instances
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


def load_spot_fleets_from_s3(bucket, prefix, role=None):
    object_list = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    spot_fleets = []
    for obj_metadata in object_list['Contents']:
        obj = s3.get_object(Bucket=bucket, Key=obj_metadata['Key'])
        sfr_metadata = json.load(obj['Body'])
        for resource_key, resource in sfr_metadata['cluster_autoscaling_resources'].items():
            if not resource_key.startswith('aws_spot_fleet_request'):
                continue
            if role and resource['pool'] != role:  # NOTE the SFR metadata uploaded to S3 uses pool where we mean role
                continue

            spot_fleets.append(SpotFleetResourceGroup(resource['id']))

    return spot_fleets


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

        instance_weights = {
            instance['InstanceId']: self.market_weights[get_instance_market(instance)]
            for instance in ec2_describe_instances(instance_ids)
        }

        # There is an unavoidable race condition here; if the fulfilled capacity changes
        # between now and the modify_target_capacity below, the final numbers for the
        # cluster will be incorrect.  There are two cases:
        #  1) The fulfilled capacity goes up between these two calls: in this case, there
        #     will just be some extra instances hanging around (above our target capacity)
        #     these extra instances will (in theory) get terminated the next time we scale
        #     down, or when we get outbid
        #  2) The fulfilled capacity goes down between these two calls: in this case, nothing
        #     bad will happen since the SFR will just try to re-fill the extra needed capacity
        original_fulfilled_capacity = self.fulfilled_capacity

        # AWS API recommends not terminating more than 1000 instances at a time, and to
        # terminate larger numbers in batches
        terminated_instance_ids = []
        for batch in range(0, len(instance_ids), batch_size):
            response = ec2.terminate_instances(InstanceIds=instance_ids[batch:batch + batch_size])
            terminated_instance_ids.extend([instance['InstanceId'] for instance in response['TerminatingInstances']])

        # It's possible that not every instance is terminated.  The most likely cause for this
        # is that AWS terminated the instance in between getting its status and the terminate_instances
        # request.  This is probably fine but let's log a warning just in case.
        missing_instances = set(instance_ids) - set(terminated_instance_ids)
        if missing_instances:
            logger.warn('Some instances could not be terminated; they were probably killed previously')
            logger.warn(f'Missing instances: {list(missing_instances)}')
        capacity_to_terminate = sum(instance_weights[i] for i in instance_ids)

        # We use the _requested_ terminated capacity instead of the _actual_ terminated capacity here;
        # if AWS took some instances away before we were able to terminate them we still want to account
        # for them in the target_capacity change.  Note that if there is some other reason why the instances
        # weren't terminated, they could still be floating around, as in case (1) in the race condition
        # discussion above
        self.modify_target_capacity(original_fulfilled_capacity - capacity_to_terminate)

        logger.info(f'{self.id} terminated weight: {capacity_to_terminate}; instances: {terminated_instance_ids}')
        return terminated_instance_ids, capacity_to_terminate

    @property
    def id(self):
        return self.sfr_id

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
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
            if market.az
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

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _configuration(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        fleet_configuration = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[self.sfr_id])
        return fleet_configuration['SpotFleetRequestConfigs'][0]

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _instances_by_market(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        instance_dict = defaultdict(list)
        for instance in ec2_describe_instances(self.instances):
            instance_dict[get_instance_market(instance)].append(instance)
        return instance_dict
