import json
from collections import defaultdict
from typing import List
from typing import Mapping
from typing import Sequence
from typing import TypeVar

import colorlog
from cached_property import timed_cached_property
from mypy_extensions import TypedDict
from spotinst_sdk import SpotinstClient
from spotinst_sdk.aws_elastigroup import Capacity
from spotinst_sdk.aws_elastigroup import DetachConfiguration
from spotinst_sdk.aws_elastigroup import Elastigroup

from clusterman.aws.markets import InstanceMarket
from clusterman.exceptions import ResourceGroupError
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.mesos_pool_resource_group import protect_unowned_instances
from clusterman.spotinst.client import get_spotinst_client

logger = colorlog.getLogger(__name__)

CREDENTIALS_NAMESPACE = 'spotinst_cfg'


SpotinstClientType = TypeVar('SpotinstClientType', bound=SpotinstClient)

Instance = TypedDict(
    'Instance',
    {
        'availability_zone': str,
        'instance_id': str,
        'instance_type': str,
    }
)

ElasticGroupWeight = TypedDict(
    'ElasticGroupWeight',
    {
        'instance_type': str,
        'weighted_capacity': float,
    }
)

ElasticGroupInstanceTypes = TypedDict(
    'ElasticGroupInstanceTypes',
    {
        'weights': Sequence[ElasticGroupWeight],
    }
)

ElasticGroupTag = TypedDict(
    'ElasticGroupTag',
    {
        'tag_key': str,
        'tag_value': str,
    }
)

ElasticGroupLaunchSpecification = TypedDict(
    'ElasticGroupLaunchSpecification',
    {
        'tags': Sequence[ElasticGroupTag],
    }
)

ElasticGroupCompute = TypedDict(
    'ElasticGroupCompute',
    {
        'instance_types': ElasticGroupInstanceTypes,
        'launch_specification': ElasticGroupLaunchSpecification,
    }
)

ElasticGroupCapacity = TypedDict(
    'ElasticGroupCapacity',
    {
        'target': float,
    }
)

ElasticGroup = TypedDict(
    'ElasticGroup',
    {
        'compute': ElasticGroupCompute,
        'capacity': ElasticGroupCapacity,
        'id': str,
    }
)

SpotInstResourceGroupConfig = TypedDict(
    'SpotInstResourceGroupConfig',
    {}
)


class SpotInstResourceGroup(MesosPoolResourceGroup):

    def __init__(self, group_id: str) -> None:
        self._group_id = group_id
        self._client = get_spotinst_client()
        self._target_capacity = self.fulfilled_capacity

    def market_weight(self, market: InstanceMarket) -> float:
        try:
            weights = self._group['compute']['instance_types']['weights']
        except KeyError:
            weights = []
        for weight in weights:
            # SpotInst doesn't allow to set different weights per AZ.
            if weight['instance_type'] == market.instance:
                return float(weight['weighted_capacity'])
        raise ResourceGroupError(f'Cannot find weight for market {market}')

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool = False,
        dry_run: bool = False,
    ) -> None:
        fulfilled_capacity = self.fulfilled_capacity
        if target_capacity > fulfilled_capacity:
            self.modify_fulfilled_capacity(target_capacity, dry_run)
        elif target_capacity < fulfilled_capacity:
            if terminate_excess_capacity:
                self.modify_fulfilled_capacity(target_capacity, dry_run)
            else:
                # Since SpotInst assumes that the target capacity is identical to the
                # fulfilled capacity, and terminates an exceed capacity immediatelly,
                # just remember the new target capacity here and decrement it in
                # `terminate_instances_by_id()`
                pass

        if dry_run:
            return
        self._target_capacity = target_capacity

    def modify_fulfilled_capacity(
        self,
        target_capacity: float,
        dry_run: bool,
    ) -> None:
        capacity_update = Capacity(target=target_capacity)
        group_update = Elastigroup(capacity=capacity_update)
        logger.debug(f'Modifying spotinst group {self._group_id} with update: {group_update}')
        if dry_run:
            return
        self._client.update_elastigroup(
            group_update=group_update,
            group_id=self._group_id,
        )

    @protect_unowned_instances
    def terminate_instances_by_id(self, instance_ids: Sequence[str]) -> Sequence[str]:
        if not instance_ids:
            logger.warn(f'No instances to terminate in {self._group_id}')
            return []

        group_detach_request = DetachConfiguration(
            instances_to_detach=instance_ids,
            should_terminate_instances=True,
            should_decrement_target_capacity=True,
        )
        self._client.detach_elastigroup_instances(
            group_id=self._group_id,
            detach_configuration=group_detach_request,
        )
        return instance_ids

    @property
    def id(self) -> str:
        return self._group_id

    @property
    def instance_ids(self) -> Sequence[str]:
        return [instance['instance_id'] for instance in self._instances if instance['instance_id'] is not None]

    @property
    def market_capacities(self) -> Mapping[InstanceMarket, float]:
        instances_by_market: Mapping[InstanceMarket, List[Instance]] = defaultdict(list)
        for instance in self._instances:
            instances_by_market[get_spotinst_instance_market(instance)].append(instance)
        return {
            market: len(instances) * self.market_weight(market)
            for market, instances in instances_by_market.items()
        }

    @property
    def target_capacity(self) -> float:
        return self._target_capacity

    @property
    def fulfilled_capacity(self) -> float:
        return self._group['capacity']['target']

    @property
    def status(self) -> str:
        return 'active'

    @property
    def is_stale(self) -> bool:
        return False

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _instances(self) -> Sequence[Instance]:
        return self._client.get_elastigroup_active_instances(self._group_id)

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _group(self) -> ElasticGroup:
        return self._client.get_elastigroup(self._group_id)

    @staticmethod
    def load(
        cluster: str,
        pool: str,
        config: SpotInstResourceGroupConfig,
    ) -> Mapping[str, MesosPoolResourceGroup]:
        return load_elastigroups(cluster, pool)


def load_elastigroups(
    cluster: str,
    pool: str,
) -> Mapping[str, MesosPoolResourceGroup]:
    """ Loads SpotInst elasticgroups by filtering all elasticgroups from
    SpotInst account by tags for pool, cluster and a tag that identifies paasta
    SpotInst elasticgroups.
    """
    client = get_spotinst_client()

    spotinst_groups_tags = get_spotinst_tags(client)
    spotinst_groups = {}
    for group_id, tags in spotinst_groups_tags.items():
        try:
            puppet_role_tags = json.loads(tags['puppet:role::paasta'])
            if puppet_role_tags['pool'] == pool and puppet_role_tags['paasta_cluster'] == cluster:
                spotinst_groups[group_id] = SpotInstResourceGroup(group_id)
        except KeyError:
            continue
    return spotinst_groups


def get_spotinst_tags(client: SpotinstClientType) -> Mapping[str, Mapping[str, str]]:
    """ Gets a dictionary of SpotInst group id -> a dictionary of tags.
    """
    groups: Sequence[ElasticGroup] = client.get_elastigroups()
    spotinst_id_to_tags = {}
    for group in groups:
        try:
            tags = group['compute']['launch_specification']['tags']
        except (IndexError, KeyError):
            tags = []
        tags_dict = {tag['tag_key']: tag['tag_value'] for tag in tags}
        spotinst_id_to_tags[group['id']] = tags_dict
    return spotinst_id_to_tags


def get_spotinst_instance_market(instance: Instance) -> InstanceMarket:
    return InstanceMarket(
        instance=instance['instance_type'],
        az=instance['availability_zone'],
    )
