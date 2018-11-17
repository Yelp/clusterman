import pprint
import threading
import time
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import Mapping
from typing import Sequence
from typing import Set

import colorlog
import simplejson as json
from cached_property import timed_cached_property
from mypy_extensions import TypedDict

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.mesos_pool_resource_group import protect_unowned_instances

_BATCH_DETACH_SIZE = 20
_BATCH_TERM_SIZE = 200

logger = colorlog.getLogger(__name__)


AutoScalingResourceGroupConfig = TypedDict(
    'AutoScalingResourceGroupConfig',
    {
        'tag': str,
    }
)


class AutoScalingResourceGroup(MesosPoolResourceGroup):
    """
    Auto Scaling Groups (ASGs)
    """

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id  # ASG id
        self._scale_down_to: Any[float] = None

        self._marked_for_death: Set[str] = set()
        threading.Thread(
            target=self._terminate_detached_instances,
            daemon=True,
        ).start()

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _group_config(self):
        """ Retrieve our ASG's configuration from AWS.

        Note: Response from this API call are cached to prevent hitting any AWS
        request limits.
        """
        response = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.group_id],
        )
        return response['AutoScalingGroups'][0]

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _launch_config(self):
        """ Retrieve our ASG's launch configuration from AWS

        Note: Response from this API call are cached to prevent hitting any AWS
        request limits.
        """
        response = autoscaling.describe_launch_configurations(
            LaunchConfigurationNames=[
                self._group_config['LaunchConfigurationName'],
            ],
        )
        return response['LaunchConfigurations'][0]

    def market_weight(self, market: InstanceMarket) -> float:
        """ Returns the weight of a given market

        ASGs have no concept of weight, so if ASG is available in the market's
        AZ, we return a weight of 1

        :param market: The market for which we want to count instances for
        :returns: The number of instances in the given market
        """
        return 1 if market in self.market_capacities else 0

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool = False,
        dry_run: bool = False,
        honor_cooldown: bool = False,
    ) -> None:
        """ Modify the desired capacity for the ASG.

        :param target_capacity: The new desired number of instances in th ASG.
            Must be such that the desired capacity is between the minimum and
            maximum capacities of the ASGs. The desired capacity will be rounded
            to the minimum or maximum otherwise, whichever is closer.
        :param terminate_excess_capacity: Boolean indicating whether or not to
            terminate excess instances in the event of a scale down
        :param dry_run: Boolean indicating whether or not to take action or just
            log
        :param honor_cooldown: Boolean for whether or not to wait for a period
            of time (cooldown, set in ASG config) after the previous scaling
            activity has completed before initiating this one. Defaults to False,
            which is the AWS default for manual scaling activities.
        """
        # Round target_cpacity to min or max if necessary
        max_size = self._group_config['MaxSize']
        min_size = self._group_config['MinSize']
        if target_capacity > max_size:
            logger.warn(
                f'New target_capacity={target_capacity} exceeds ASG MaxSize={max_size}, '
                'setting to max instead'
            )
            target_capacity = max_size
        elif target_capacity < min_size:
            logger.warn(
                f'New target_capacity={target_capacity} falls below ASG MinSize={min_size}, '
                'setting to min instead'
            )
            target_capacity = min_size

        kwargs = dict(
            AutoScalingGroupName=self.group_id,
            DesiredCapacity=int(target_capacity),
            HonorCooldown=honor_cooldown,
        )
        if dry_run:
            logger.info(
                'Would have set target capacity for ASG with arguments:\n'
                f'{pprint.pformat(kwargs)}'
            )
            return  # for safety, in case anyone adds code after branch
        # In the event of scale down, we may not want to terminate excess
        # capacity because MesosPoolManager will do it instead in a special way.
        elif ((target_capacity < self.target_capacity and terminate_excess_capacity) or
                target_capacity > self.target_capacity):
            logger.info(
                'Setting target capacity for ASG with arguments:\n'
                f'{pprint.pformat(kwargs)}'
            )
            autoscaling.set_desired_capacity(**kwargs)
        else:
            self._scale_down_to = target_capacity

    @protect_unowned_instances
    def terminate_instances_by_id(
        self,
        instance_ids: Sequence[str],
    ) -> Sequence[str]:
        """ Terminate instances in the ASG

        The autoscaling client does not support batch termination, only
        instance-by-instance termination. To avoid hitting AWS request limits,
        we simply tag the instances we want to terminate and detach them all
        from the ASG at once.

        We have a running thread that periodically terminates instances if they
        have finished detaching.

        :param instance_ids: A list of instance IDs to terminate
        :returns: A list of terminated instance IDs
        """
        if not instance_ids:
            logger.warn(f'No instances to terminate in {self.group_id}')
            return []

        def detach(ids, decrement_desired_capacity):
            # The autoscaling API specifies that we can only specify up to 20
            # ids at once to detach:
            for i in range(0, len(ids), _BATCH_DETACH_SIZE):
                to_detach = ids[i:i + _BATCH_DETACH_SIZE]
                autoscaling.detach_instances(
                    InstanceIds=to_detach,
                    AutoScalingGroupName=self.group_id,
                    ShouldDecrementDesiredCapacity=decrement_desired_capacity,
                )
                self._marked_for_death.update(to_detach)

        if self._scale_down_to:
            # if we have a pending scale down from modify_target_capacity,
            # we detach while lowering the ASGs desired capacity as much as
            # we can until we hit the new target capacity (self._scale_down_to).
            detach(instance_ids[:int(self._scale_down_to)], True)
            # After that, if we still have ids to detach, we detach WITHOUT
            # reducing the desired capacity.
            detach(instance_ids[int(self._scale_down_to):], False)

            if len(instance_ids) >= self._scale_down_to:
                self._scale_down_to = None
            else:
                self._scale_down_to -= len(instance_ids)
        else:
            detach(instance_ids, False)

        # Although these instances have only been detached, we consider them
        # terminated for the purposes of the ASG
        logger.info(f'ASG {self.id}: detached instances: {instance_ids}')
        return instance_ids

    def _terminate_detached_instances(self):
        """ Periodically terminate detached instances using the EC2 client.
        Intended to be run in a separate thread.
        """
        while True:
            detached_ids = []
            if self._marked_for_death:
                # We first need to get the ids for instances that have finished
                # detachings)
                detached_ids = list(self._marked_for_death - set(self.instance_ids))

            if detached_ids:  # if theres something that has finished detaching
                # AWS API recommends terminating not more than 1000 instances
                # at a time.
                terminated_ids = []
                for i in range(0, len(detached_ids), _BATCH_TERM_SIZE):
                    response = ec2.terminate_instances(InstanceIds=detached_ids)
                    terminated_ids.extend([
                        inst['InstanceId']
                        for inst in response['TerminatingInstances']
                    ])

                # It's possible that not every instance appears terminated,
                # probably because AWS already terminated them. Log just in case.
                missing_ids = set(detached_ids) - set(terminated_ids)
                if missing_ids:
                    logger.warn(
                        'Some instances could not be terminated; '
                        'they were probably killed previously'
                    )
                    logger.warn(f'Missing instances: {list(missing_ids)}')

                logger.info(f'ASG {self.id}: terminated: {terminated_ids}')
                self._marked_for_death -= set(detached_ids)

            time.sleep(CACHE_TTL_SECONDS)  # to avoid hitting AWS request limit

    @property
    def id(self) -> str:
        """ Returns the ASG's id """
        return self.group_id

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def instance_ids(self) -> Sequence[str]:
        """ Returns a list of instance IDs belonging to this ASG.

        Note: Response from this API call are cached to prevent hitting any AWS
        request limits.
        """
        return [
            inst['InstanceId']
            for inst in self._group_config['Instances']
            if inst is not None
        ]

    @property
    def market_capacities(self) -> Mapping[InstanceMarket, float]:
        """ Returns the total capacity (number of instances) per market that the
        ASG is in.
        """
        instances_by_market: Dict[InstanceMarket, float] = defaultdict(float)
        for inst in self._group_config['Instances']:
            inst_market = InstanceMarket(
                self._launch_config['InstanceType'],  # type uniform in asg
                inst['AvailabilityZone'],
            )
            instances_by_market[inst_market] += 1
        return instances_by_market

    @property
    def target_capacity(self) -> float:
        if self._scale_down_to:
            return self._scale_down_to
        else:
            return self._group_config['DesiredCapacity']

    @property
    def fulfilled_capacity(self) -> float:
        return len(self._group_config['Instances'])

    @property
    def status(self) -> str:
        """ The status of the ASG

        An ASG either exists or it doesn't. Thus, if we can query its status,
        it is active.
        """
        return 'active'

    @property
    def is_stale(self) -> bool:
        """ Whether or not the ASG is stale

        An ASG either exists or it doesn't. Thus, the concept of staleness
        doesn't exist.
        """
        return False

    @staticmethod
    def load(
        cluster: str,
        pool: str,
        config: AutoScalingResourceGroupConfig,
    ) -> Mapping[str, 'MesosPoolResourceGroup']:
        """
        Loads a list of ASGs in the given cluster and pool

        :param cluster: A cluster name
        :param pool: A pool name
        :param config: An ASG config
        :returns: A dictionary of autoscaling resource groups, indexed by the id
        """
        asg_tags = _get_asg_tags()
        asgs = {}
        for asg_id, tags in asg_tags.items():
            try:
                puppet_role_tags = json.loads(tags[config['tag']])
                if (puppet_role_tags['pool'] == pool and
                        puppet_role_tags['paasta_cluster'] == cluster):
                    asgs[asg_id] = AutoScalingResourceGroup(asg_id)
            except KeyError:
                continue
        return asgs


def _get_asg_tags() -> Mapping[str, Mapping[str, str]]:
    """ Retrieves the tags for each ASG """
    asg_id_to_tags = {}
    for page in autoscaling.get_paginator('describe_auto_scaling_groups').paginate():
        for asg in page['AutoScalingGroups']:
            tags_dict = {tag['Key']: tag['Value'] for tag in asg['Tags']}
            asg_id_to_tags[asg['AutoScalingGroupName']] = tags_dict
    return asg_id_to_tags
