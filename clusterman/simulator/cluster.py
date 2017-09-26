import itertools
from collections import defaultdict

from clusterman.common.aws import get_instance_resources


class Instance:
    id = itertools.count()

    def __init__(self, market, launch_time):
        self.id = next(Instance.id)
        self.market = market
        self.launch_time = launch_time
        self.resources = get_instance_resources(self.market)


class Cluster:
    def __init__(self):
        self.instances = {}
        self._instance_ids_by_market = defaultdict(list)
        self.ebs_storage = 0

    def modify_capacity(self, instances_by_market, modify_time):
        """ Modify the capacity of the cluster to match a specified state

        :param instances_by_market: a dict from InstanceMarket -> num, representing the desired number of
            instances in each specified market; unspecified markets are left unchanged
        :param modify_time: arrow object corresponding to the instance launch or termination time
        :returns: list of ids (integers) of the added instances
        """
        added_instances = []
        for market, num in instances_by_market.items():
            market_size = len(self._instance_ids_by_market[market])
            delta = int(num - market_size)

            if delta > 0:
                instances = [Instance(market, modify_time) for i in range(delta)]
                self._instance_ids_by_market[market].extend([instance.id for instance in instances])
                added_instances.extend(instances)

            if delta < 0:
                to_del = abs(delta)
                for id in self._instance_ids_by_market[market][:to_del]:
                    del self.instances[id]
                del self._instance_ids_by_market[market][:to_del]

        self.instances.update({instance.id: instance for instance in added_instances})
        return [instance.id for instance in added_instances]

    @property
    def ids(self):
        return self.instances.keys()

    @property
    def cpu(self):
        return sum(instance.resources.cpu for instance in self.instances.values())

    @property
    def mem(self):
        return sum(instance.resources.mem for instance in self.instances.values())

    @property
    def disk(self):
        # Not all instance types have storage and require a mounted EBS volume
        return self.ebs_storage + sum(
            instance.resources.disk
            for instance in self.instances.values()
            if instance.resources.disk is not None
        )
