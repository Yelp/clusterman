import pprint
import threading
import time
from typing import Any
from typing import Mapping
from typing import Sequence

import colorlog
from cached_property import timed_cached_property

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket
from clusterman.mesos.constants import CACHE_TTL_SECONDS
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.mesos_pool_resource_group import protect_unowned_instances

_BATCH_DETACH_SIZE = 20
_BATCH_TERM_SIZE = 200

logger = colorlog.getLogger(__name__)


class AutoScalingResourceGroup(MesosPoolResourceGroup):
    """
    Auto Scaling Groups (ASGs)
    """

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id  # ASG id

        self._marked_for_death = set()
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
        """ Return the weighted capacity assigned to a particular EC2 market by
        this resource group.

        An ASG may only have instances of a single type. Therefore, for that
        type, the weighted capacity is the size of the group, and None for all
        other types.
        """
        # We first check that our ASG has instances of the same type in the
        # same availability zone(s) as the market we are looking at
        if (market.instance is self._launch_config['InstanceType'] and
                market.az in set(self._group_config['AvailabilityZones'])):
            # We count and return the number instances in the market's AZ
            return sum(
                market.az is None or market.az is inst['AvailabilityZone']
                for inst in self._group_config['Instances']
                if inst is not None
            )
        return None

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        terminate_excess_capacity: bool,
        dry_run: bool,
        honor_cooldown: bool = False,
    ) -> None:
        """ Modify the desired capacity for the ASG.

        :param target_capacity: The new desired number of instances in th ASG.
            Must be such that the desired capacity is between the minimum and
            maximum capacities of the ASGs. The desired capacity will be rounded
            to the minimum or maximum otherwise, whichever is closer.
        :param terminate_excess_capacity: This is a spot fleet concept. For us,
            this is merely an interface argument, and does not affect this
            function. The ASG's scaling policy will create or terminate
            instances to maintain that capacity. In other words, we cannot
            choose to NOT terminate.
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
                f'New target_capacty={target_capacity} exceeds ASG MaxSize={max_size}, '
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
            return  # for safety, in case anyone adds code after branches
        else:
            logger.info(
                'Setting target capacity for ASG with arguments:\n'
                f'{pprint.pformat(kwargs)}'
            )
            autoscaling.set_desired_capacity(**kwargs)

    @protect_unowned_instances
    def terminate_instances_by_id(
        self,
        instance_ids: Sequence[str],
        decrement_desired_capacity: bool = False,
    ) -> Sequence[str]:
        """ Terminate instances in the ASG

        The autoscaling client does not support batch termination, only
        instance-by-instance termination. To avoid hitting AWS request limits,
        we simply tag the instances we want to terminate and detach them all
        from the ASG at once.

        We have a running thread that periodically terminates instances if they
        have finished detaching.

        :param instance_ids: A list of instance IDs to terminate
        :param decrement_desired_capacity: A boolean for whether or not
            terminating instances also means decrementing the desired capacity
            for the ASG. Defaults to False.
        :returns: A list of terminated instance IDs
        """
        if not instance_ids:
            logger.warn(f'No instances to terminate in {self.group_id}')
            return []

        # The autoscaling API specifies that we can only specify up to 20 ids at
        # once to detach:
        for i in range(0, len(instance_ids), _BATCH_DETACH_SIZE):
            autoscaling.detach_instances(
                InstanceIds=instance_ids[i:i + _BATCH_DETACH_SIZE],
                AutoScalingGroupName=self.group_id,
                ShouldDecrementDesiredCapacity=decrement_desired_capacity,
            )

        # Although these instances have only been detached, we consider them
        # terminated for the purposes of the ASG
        logger.info(f'ASG {self.id}: detached instances: {instance_ids}')
        return instance_ids

    def _terminate_detached_instances(self):
        """ Periodically terminate detached instances using the EC2 client. """
        while True:
            if self._marked_for_death:
                # We first need to get the ids for instances that have finished
                # detaching
                instance_ids = set(self.instance_ids)
                detached_ids = [
                    inst_id
                    for inst_id in self._marked_for_death
                    if inst_id not in instance_ids
                ]

                # AWS API recommends terminating not more than 1000 instances
                # at a time.
                terminated_ids = []
                for i in range(0, len(instance_ids), _BATCH_TERM_SIZE):
                    response = ec2.terminate_instances(InstanceIds=detached_ids)
                    terminated_ids.extend([
                        inst['InstanceId']
                        for inst in response['TerminatingInstances']
                    ])

                # It's possible that not every instance appears terminated,
                # probably because AWS already terminated them. Log just in case.
                missing_instances = set(detached_ids) - set(terminated_ids)
                if missing_instances:
                    logger.warn(
                        'Some instances could not be terminated; '
                        'they were probably killed previously'
                    )
                    logger.warn(f'Missing instances: {list(missing_instances)}')

                logger.info(f'ASG {self.id}: terminated instances: {terminated_ids}')
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
    def target_capacity(self) -> float:
        return self._group_config['DesiredCapacity']

    @property
    def fulfilled_capacity(self) -> float:
        return len(self._group_config['Instances'])

    @property
    def status(self) -> str:
        """ The status of the ASG

        ASG configs only include the 'Status' key when it is being terminated,
        according to the API. Thus, we assume that if there is no status, the
        ASG is fine.

        [WIP] I'm not sure if this is the best way to an ASG's status as it
        really applies to the instances. But I can't think of a way to do it
        that is similar to how we get it for SFRs.
        """
        return self._group_config.get('Status', 'active')

    @property
    def is_stale(self) -> bool:
        """ [WIP] Not sure an ASG can become stale? What does staleness for an
        ASG even mean?
        """
        return False

    def load(cluster: str, pool: str, config: Any) -> Mapping[str, 'MesosPoolResourceGroup']:
        """ How do I use cluster pool info to select ASGs? Tags like SFRs? """
        return {}
