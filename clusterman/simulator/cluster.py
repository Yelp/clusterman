import itertools
from collections import defaultdict

from clusterman.aws.markets import get_instance_resources


class Instance:
    id = itertools.count()

    def __init__(self, market, start_time, bid_price=None):
        self.id = next(Instance.id)
        self.market = market
        self.start_time = start_time
        self.end_time = None
        self.resources = get_instance_resources(self.market)
        self.bid_price = bid_price

    @property
    def spot(self):
        return self.bid_price is not None


class Cluster:
    def __init__(self):
        self._instances = {}
        self._instance_ids_by_market = defaultdict(list)
        self.ebs_storage = 0

    def __len__(self):
        return len(self._instances)

    def modify_size(self, instances_by_market, modify_time):
        """ Modify the capacity of the cluster to match a specified state

        :param instances_by_market: a dict from InstanceMarket -> num, representing the desired number of
            instances in each specified market; unspecified markets are left unchanged
        :param modify_time: arrow object corresponding to the instance launch or termination time
        :returns: a tuple (added_instances, removed_instances)
        """
        added_instances, removed_instances = [], []
        for market, num in instances_by_market.items():
            delta = int(num - self.market_size(market))

            if delta > 0:
                instances = [Instance(market, modify_time) for i in range(delta)]
                self._instance_ids_by_market[market].extend([instance.id for instance in instances])
                added_instances.extend(instances)

            if delta < 0:
                to_del = abs(delta)
                for id in self._instance_ids_by_market[market][:to_del]:
                    self._instances[id].end_time = modify_time
                    removed_instances.append(self._instances[id])
                    del self._instances[id]
                del self._instance_ids_by_market[market][:to_del]

        self._instances.update({instance.id: instance for instance in added_instances})
        return added_instances, removed_instances

    def market_size(self, market):
        return len(self._instance_ids_by_market[market])

    @property
    def instances(self):
        return {id: instance for id, instance in self._instances.items()}

    @property
    def cpu(self):
        return sum(instance.resources.cpu for instance in self._instances.values())

    @property
    def mem(self):
        return sum(instance.resources.mem for instance in self._instances.values())

    @property
    def disk(self):
        # Not all instance types have storage and require a mounted EBS volume
        return self.ebs_storage + sum(
            instance.resources.disk
            for instance in self._instances.values()
            if instance.resources.disk is not None
        )
