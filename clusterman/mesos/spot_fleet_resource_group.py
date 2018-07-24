from collections import defaultdict
from typing import Dict
from typing import List
from typing import Sequence

import botocore
import simplejson as json
from cached_property import timed_cached_property
from mypy_extensions import TypedDict

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import s3
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.exceptions import ResourceGroupError
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.mesos_pool_resource_group import protect_unowned_instances
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
CANCELLED_STATES = ('cancelled', 'cancelled_terminating')

_S3Config = TypedDict(
    '_S3Config',
    {
        'bucket': str,
        'prefix': str,
    }
)

SpotFleetResourceGroupConfig = TypedDict(
    'SpotFleetResourceGroupConfig',
    {
        's3': _S3Config,
        'tag': str,
    }
)


class SpotFleetResourceGroup(MesosPoolResourceGroup):

    def __init__(self, sfr_id: str) -> None:
        self.sfr_id = sfr_id

        # Can't change WeightedCapacity of SFRs, so cache them here for frequent access
        self._market_weights = self._generate_market_weights()

    def _generate_market_weights(self):
        return {
            get_instance_market(spec): spec['WeightedCapacity']
            for spec in self._configuration['SpotFleetRequestConfig']['LaunchSpecifications']
        }

    def market_weight(self, market: InstanceMarket) -> float:
        return self._market_weights[market]

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool = False,
        dry_run: bool = False,
    ) -> None:
        if self.is_stale:
            logger.info(f"Not modifying spot fleet request since it is in state {self.status}")
            return
        kwargs = {
            'SpotFleetRequestId': self.sfr_id,
            'TargetCapacity': int(target_capacity),
            'ExcessCapacityTerminationPolicy': 'Default' if terminate_excess_capacity else 'NoTermination'
        }
        logger.info(f'Modifying spot fleet request with arguments: {kwargs}')
        if dry_run:
            return

        response = ec2.modify_spot_fleet_request(**kwargs)
        if not response['Return']:
            logger.critical('Could not change size of spot fleet:\n{resp}'.format(resp=json.dumps(response)))
            raise ResourceGroupError('Could not change size of spot fleet: check logs for details')

    @protect_unowned_instances
    def terminate_instances_by_id(
        self,
        instance_ids: List[str],
        batch_size: int = 500,
    ) -> Sequence[str]:
        if not instance_ids:
            logger.warn(f'No instances to terminate in {self.sfr_id}')
            return []

        instance_weights = {}
        for instance in ec2_describe_instances(instance_ids):
            instance_market = get_instance_market(instance)
            if not instance_market.az:
                logger.warn(f"Instance {instance['InstanceId']} missing AZ info, likely already terminated so skipping")
                instance_ids.remove(instance['InstanceId'])
                continue
            instance_weights[instance['InstanceId']] = self.market_weight(get_instance_market(instance))

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
        terminated_capacity = sum(instance_weights[i] for i in instance_ids)

        logger.info(f'{self.id} terminated weight: {terminated_capacity}; instances: {terminated_instance_ids}')
        return terminated_instance_ids

    @property
    def id(self) -> str:
        return self.sfr_id

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def instance_ids(self) -> Sequence[str]:
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        return [
            instance['InstanceId']
            for page in ec2.get_paginator('describe_spot_fleet_instances').paginate(SpotFleetRequestId=self.sfr_id)
            for instance in page['ActiveInstances']
            if instance is not None
        ]

    @property
    def market_capacities(self) -> Dict[InstanceMarket, float]:
        return {
            market: len(instances) * self.market_weight(market)
            for market, instances in self._instances_by_market.items()
            if market.az
        }

    @property
    def target_capacity(self) -> float:
        if self.is_stale:
            # If we're in cancelled, cancelled_running, or cancelled_terminated, then no more instances will be
            # launched. This is effectively a target_capacity of 0, so let's just pretend like it is.
            return 0
        return self._configuration['SpotFleetRequestConfig']['TargetCapacity']

    @property
    def fulfilled_capacity(self) -> float:
        return self._configuration['SpotFleetRequestConfig']['FulfilledCapacity']

    @property
    def status(self) -> str:
        return self._configuration['SpotFleetRequestState']

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _configuration(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        fleet_configuration = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[self.sfr_id])
        return fleet_configuration['SpotFleetRequestConfigs'][0]

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _instances_by_market(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        instance_dict: Dict[InstanceMarket, List[Dict]] = defaultdict(list)
        for instance in ec2_describe_instances(self.instance_ids):
            instance_dict[get_instance_market(instance)].append(instance)
        return instance_dict

    @property
    def is_stale(self) -> bool:
        try:
            return self.status.startswith('cancelled')
        except botocore.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code', 'Unknown') == 'InvalidSpotFleetRequestId.NotFound':
                return True
            raise e

    @staticmethod
    def load(
        cluster: str,
        pool: str,
        config: SpotFleetResourceGroupConfig,
    ) -> Sequence['SpotFleetResourceGroup']:
        return load(cluster, pool, config)


def load(
    cluster: str,
    pool: str,
    config: SpotFleetResourceGroupConfig,
) -> Sequence[SpotFleetResourceGroup]:
    if 'tag' in config:
        ec2_resource_groups = load_spot_fleets_from_ec2(
            cluster=cluster,
            pool=pool,
            sfr_tag=config['tag'],
        )
        logger.info(f'SFRs loaded from ec2: {list(ec2_resource_groups)}')
    else:
        ec2_resource_groups = {}

    if 's3' in config:
        s3_resource_groups = load_spot_fleets_from_s3(
            config['s3']['bucket'],
            config['s3']['prefix'],
            pool=pool,
        )
        logger.info(f'SFRs loaded from s3: {list(s3_resource_groups)}')
    else:
        s3_resource_groups = {}

    # Nifty new syntax to merge dicts
    resource_groups = {
        sfr_id: sfrg for sfr_id, sfrg in {**ec2_resource_groups, **s3_resource_groups}.items()
        if sfrg.status not in CANCELLED_STATES
    }
    logger.info(f'Merged ec2 & s3 SFRs: {list(resource_groups)}')

    return list(resource_groups.values())


def load_spot_fleets_from_s3(bucket: str, prefix: str, pool: str = None) -> Dict[str, SpotFleetResourceGroup]:
    prefix = prefix.rstrip('/') + '/'
    object_list = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    spot_fleets = {}
    for obj_metadata in object_list['Contents']:
        obj = s3.get_object(Bucket=bucket, Key=obj_metadata['Key'])
        sfr_metadata = json.load(obj['Body'])
        for resource_key, resource in sfr_metadata['cluster_autoscaling_resources'].items():
            if not resource_key.startswith('aws_spot_fleet_request'):
                continue
            if pool and resource['pool'] != pool:
                continue

            spot_fleets[resource['id']] = SpotFleetResourceGroup(resource['id'])

    return spot_fleets


def load_spot_fleets_from_ec2(cluster: str, pool: str, sfr_tag: str) -> Dict[str, SpotFleetResourceGroup]:
    """ Loads SpotFleetResourceGroups by filtering SFRs in the AWS account by tags
    for pool, cluster and a tag that identifies paasta SFRs
    """
    spot_fleet_requests_tags = get_spot_fleet_request_tags()
    spot_fleets = {}
    for sfr_id, tags in spot_fleet_requests_tags.items():
        try:
            puppet_role_tags = json.loads(tags[sfr_tag])
            if puppet_role_tags['pool'] == pool and puppet_role_tags['paasta_cluster'] == cluster:
                sfrg = SpotFleetResourceGroup(sfr_id)
                spot_fleets[sfr_id] = sfrg
        except KeyError:
            continue
    return spot_fleets


def get_spot_fleet_request_tags() -> Dict[str, Dict[str, str]]:
    """ Gets a dictionary of SFR id -> a dictionary of tags. The tags are taken
    from the TagSpecifications for the first LaunchSpecification
    """
    spot_fleet_requests = ec2.describe_spot_fleet_requests()
    sfr_id_to_tags = {}
    for sfr_config in spot_fleet_requests["SpotFleetRequestConfigs"]:
        launch_specs = sfr_config["SpotFleetRequestConfig"]["LaunchSpecifications"]
        try:
            # we take the tags from the 0th launch spec for now
            # they should always be identical in every launch spec
            tags = launch_specs[0]["TagSpecifications"][0]["Tags"]
        except (IndexError, KeyError):
            # if this SFR is misssing the TagSpecifications
            tags = []
        tags_dict = {tag['Key']: tag['Value'] for tag in tags}
        sfr_id_to_tags[sfr_config["SpotFleetRequestId"]] = tags_dict
    return sfr_id_to_tags
