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

    def add_instances(self, instances_by_market, launch_time):
        """ Add instances from the specified market(s) to the cluster

        :param instances_by_market: a dict from InstanceMarket -> num; instances to add grouped by market
        :param launch_time: arrow object corresponding to the instance launch time
        :returns: list of ids (integers) of the added instances
        """
        added_instances = []
        for market, num in instances_by_market.items():
            instances = [Instance(market, launch_time) for i in range(num)]
            self._instance_ids_by_market[market].extend([instance.id for instance in instances])
            added_instances.extend(instances)

        self.instances.update({instance.id: instance for instance in added_instances})
        return [instance.id for instance in added_instances]

    def terminate_instances_by_market(self, instances_by_market):
        """ Remove instances from the specified market(s) from the cluster

        :param instances_by_market: a dict from InstanceMarket -> num; instances to remove grouped by market
        :raises ValueError: if more instances are requested than exist in a market
        """
        for market, num in instances_by_market.items():
            market_size = len(self._instance_ids_by_market[market])
            if num > market_size:
                raise ValueError(f'Tried to remove {num} instances but {market} only has {market_size}')

            for id in self._instance_ids_by_market[market][:num]:
                del self.instances[id]
            del self._instance_ids_by_market[market][:num]

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
