import itertools
from collections import defaultdict

from clusterman.common.aws import get_instance_resources
from clusterman.exceptions import SimulationError


class Instance:
    id = itertools.count()

    def __init__(self, market, launch_time, bid_price=None):
        self.id = next(Instance.id)
        self.market = market
        self.launch_time = launch_time
        self.bill_time = launch_time
        self.resources = get_instance_resources(self.market)
        self.last_price = None
        self.bid_price = bid_price
        self.active = True

    @property
    def spot(self):
        return self.bid_price is not None


class Cluster:
    def __init__(self):
        self._instances = {}
        self._active_instance_ids_by_market = defaultdict(list)
        self.ebs_storage = 0

    def __len__(self):
        return len(self.active_instances)

    def modify_capacity(self, instances_by_market, modify_time):
        """ Modify the capacity of the cluster to match a specified state

        :param instances_by_market: a dict from InstanceMarket -> num, representing the desired number of
            instances in each specified market; unspecified markets are left unchanged
        :param modify_time: arrow object corresponding to the instance launch or termination time
        :returns: a tuple (added_instance_ids, removed_instance_ids) of lists of instance ids that were added/removed
        """
        added_instances, removed_instance_ids = [], []
        for market, num in instances_by_market.items():
            market_size = len(self._active_instance_ids_by_market[market])
            delta = int(num - market_size)

            if delta > 0:
                instances = [Instance(market, modify_time) for i in range(delta)]
                self._active_instance_ids_by_market[market].extend([instance.id for instance in instances])
                added_instances.extend(instances)

            if delta < 0:
                to_del = abs(delta)
                for id in self._active_instance_ids_by_market[market][:to_del]:
                    self._instances[id].active = False
                    removed_instance_ids.append(id)
                del self._active_instance_ids_by_market[market][:to_del]

        self._instances.update({instance.id: instance for instance in added_instances})
        return [instance.id for instance in added_instances], removed_instance_ids

    def prune_instances(self, instance_ids):
        """ To terminate an instance, we just mark it inactive so that its pricing data persists;
        once the cluster cost has been modified to take into account the terminated instance we can clean it up

        :param instance_ids: a list of instance ids that no longer need to be tracked
        :raises SimulationError: if an active instance is pruned
        """
        for id in instance_ids:
            if self._instances[id].active:
                raise SimulationError(f'Tried to prune active instance {id}')
            del self._instances[id]

    @property
    def active_instances(self):
        return {id: instance for id, instance in self._instances.items() if instance.active}

    @property
    def all_instances(self):
        return self._instances

    @property
    def cpu(self):
        return sum(instance.resources.cpu for instance in self.active_instances.values())

    @property
    def mem(self):
        return sum(instance.resources.mem for instance in self.active_instances.values())

    @property
    def disk(self):
        # Not all instance types have storage and require a mounted EBS volume
        return self.ebs_storage + sum(
            instance.resources.disk
            for instance in self.active_instances.values()
            if instance.resources.disk is not None
        )
