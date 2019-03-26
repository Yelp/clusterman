from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections import defaultdict
from typing import Any
from typing import List
from typing import Mapping
from typing import Sequence

import colorlog
import simplejson as json
from cached_property import timed_cached_property

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.constants import CACHE_TTL_SECONDS


logger = colorlog.getLogger(__name__)


def protect_unowned_instances(func):
    """ A decorator that protects instances that are not owned by a particular AWSResourceGroup from being modified

    It is assumed that the decorated function takes a list of instance IDs as its first argument; this list
    is modified before the decorated function is called to strip out any unowned instances.  In this case a warning
    is logged.
    """

    def wrapper(self, instance_ids, *args, **kwargs):
        resource_group_instances = list(set(instance_ids) & set(self.instance_ids))
        invalid_instances = set(instance_ids) - set(self.instance_ids)
        if invalid_instances:
            logger.warn(f'Some instances are not part of this resource group ({self.id}):\n{invalid_instances}')
        return func(self, resource_group_instances, *args, **kwargs)
    return wrapper


class AWSResourceGroup(metaclass=ABCMeta):
    """
    The AWSResourceGroup is an abstract object codifying the interface that objects belonging to a Mesos
    cluster are expected to adhere to.  In general, a "AWSResourceGroup" object should represent a collection of
    machines that are a part of a Mesos cluster, and should have an API for adding and removing hosts from the
    AWSResourceGroup, as well as querying the state of the resource group.
    """

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id

    def market_weight(self, market: InstanceMarket) -> float:  # pragma: no cover
        """ Return the weighted capacity assigned to a particular EC2 market by this resource group

        The weighted capacity is a SpotFleet concept but for consistency we assume other resource group types will also
        have weights assigned to them; this will allow the MesosPool to operate on a variety of different resource types

        Note that market_weight is compared to fulfilled_capacity when scaling down a pool, so it must return the same
        units.

        :param market: the :py:class:`.InstanceMarket` to get the weighted capacity for
        :returns: the weighted capacity of the market (defaults to 1 unless overridden)
        """
        return 1

    @abstractmethod
    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool,
        dry_run: bool,
    ) -> None:  # pragma: no cover
        """ Modify the target capacity for the resource group

        :param target_capacity: the (weighted) new target capacity for the resource group
        :param terminate_excess_capacity: boolean indicating whether to terminate instances if the
            new target capacity is less than the current capacity
        :param dry_run: boolean indicating whether to take action or just write to stdout
        """
        pass

    @protect_unowned_instances
    def terminate_instances_by_id(self, instance_ids: List[str], batch_size: int = 500) -> Sequence[str]:
        """ Terminate instances in this resource group

        :param instance_ids: a list of instance IDs to terminate
        :param batch_size: number of instances to terminate at one time
        :returns: a list of terminated instance IDs
        """
        if not instance_ids:
            logger.warn(f'No instances to terminate in {self.group_id}')
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
        """ A unique identifier for this AWSResourceGroup """
        return self.group_id

    @abstractproperty
    def instance_ids(self) -> Sequence[str]:  # pragma: no cover
        """ The list of instance IDs belonging to this AWSResourceGroup """
        pass

    @property
    def market_capacities(self) -> Mapping[InstanceMarket, float]:
        return {
            market: len(instances) * self.market_weight(market)
            for market, instances in self._instances_by_market.items()
            if market.az
        }

    @property
    def target_capacity(self) -> float:
        """ The target (or desired) weighted capacity for this AWSResourceGroup

        Note that the actual weighted capacity in the AWSResourceGroup may be smaller or larger than the
        target capacity, depending on the state of the AWSResourceGroup, available instance types, and
        previous operations; use self.fulfilled_capacity to get the actual capacity
        """
        if self.is_stale:
            # If we're in cancelled, cancelled_running, or cancelled_terminated, then no more instances will be
            # launched. This is effectively a target_capacity of 0, so let's just pretend like it is.
            return 0
        return self._target_capacity

    @abstractproperty
    def fulfilled_capacity(self) -> float:  # pragma: no cover
        """ The actual weighted capacity for this AWSResourceGroup """
        pass

    @abstractproperty
    def status(self) -> str:  # pragma: no cover
        """ The status of the AWSResourceGroup (e.g., running, modifying, terminated, etc.) """
        pass

    @abstractproperty
    def is_stale(self) -> bool:  # pragma: no cover
        """Whether this AWSResourceGroup is stale."""
        pass

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _instances_by_market(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        instance_dict: Mapping[InstanceMarket, List[Mapping]] = defaultdict(list)
        for instance in ec2_describe_instances(self.instance_ids):
            instance_dict[get_instance_market(instance)].append(instance)
        return instance_dict

    @abstractproperty
    def _target_capacity(self):  # pragma: no cover
        pass

    @classmethod
    def load(cls, cluster: str, pool: str, config: Any) -> Mapping[str, 'AWSResourceGroup']:
        """ Load a list of corresponding resource groups

        :param cluster: a cluster name
        :param pool: a pool name
        :param config: a config specific to a resource group type
        :returns: a dictionary of resource groups, indexed by id
        """
        resource_group_tags = cls._get_resource_group_tags()
        matching_resource_groups = {}

        try:
            identifier_tag_label = config['tag']
        except KeyError:
            return {}

        for rg_id, tags in resource_group_tags.items():
            try:
                identifier_tags = json.loads(tags[identifier_tag_label])
                if identifier_tags['pool'] == pool and identifier_tags['paasta_cluster'] == cluster:
                    rg = cls(rg_id)
                    matching_resource_groups[rg_id] = rg
            except KeyError:
                continue
        return matching_resource_groups

    @classmethod
    def _get_resource_group_tags(cls) -> Mapping[str, Mapping[str, str]]:  # pragma: no cover
        return {}
