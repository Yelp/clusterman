import itertools


class Event(object):
    """ Base event class; does nothing """
    id = itertools.count()

    def __init__(self, time, msg=None):
        """ Every subclass should call super().__init__(time, msg) to ensure needed setup is done

        :param time: an arrow object indicating the time the event should fire
        :param msg: a message to display; if this is None, just print the class name
        """
        self.id = next(Event.id)
        self.time = time
        self.msg = msg or type(self).__name__

    def __lt__(self, other):
        """ Sort order is based on time """
        return self.time < other.time

    def __str__(self):
        return f'=== Event {self.id} -- {self.time}\t[{self.msg}]'

    def handle(self, simulator):
        """ Subclasses can override this for more complex behaviour

        :param simulator: a clusterman Simulator instance which the event can access to change state
        """
        pass


class AddClusterCapacityEvent(Event):
    def __init__(self, time, instance_types, msg=None):
        """ Trigger this event when adding more instances to the cluster

        :param instance_types: a dict of InstanceMarket -> integer indicating how many instances to add
        """
        super().__init__(time, msg)
        self.instance_types = dict(instance_types)

    def handle(self, simulator):
        # add the instances to the cluster and compute costs for their first hour
        instance_ids = simulator.cluster.add_instances(self.instance_types, launch_time=self.time)
        simulator.add_event(ComputeClusterCostEvent(self.time, instance_ids))


class ComputeClusterCostEvent(Event):
    def __init__(self, time, instance_ids, msg=None):
        """ Trigger this event whenever costs need to be recomputed
        AWS charges new costs when an instance is launched and every hour thereafter

        :param instance_ids: a list of ids to compute costs for
        """
        super().__init__(time, msg)
        self.instance_ids = list(instance_ids)

    def handle(self, simulator):
        # any instances that are still active at this point need to be billed
        active_instances = set(self.instance_ids) & set(simulator.cluster.ids)
        for id in active_instances:
            simulator.total_cost += simulator.spot_prices[simulator.cluster.instances[id].market]

        # if there are still active instances, add a new event for 1 hour from now to compute their costs
        if active_instances:
            simulator.add_event(ComputeClusterCostEvent(self.time.shift(hours=1), active_instances))


class SpotPriceChangeEvent(Event):
    def __init__(self, time, spot_prices, msg=None):
        """ Trigger this event whenever spot prices change

        :param spot_prices: a dict of InstanceMarket -> float indicating the new spot prices
        """
        super().__init__(time, msg)
        self.spot_prices = dict(spot_prices)

    def handle(self, simulator):
        for market, price in self.spot_prices.items():
            simulator.spot_prices[market] = price


class TerminateInstancesEvent(Event):
    def __init__(self, time, instance_types, msg=None):
        """ Trigger this event whenever instances are terminated

        :param instance_types: a dict of InstanceMarket -> num, how many instances to remove from each market
        """
        super().__init__(time, msg)
        self.instance_types = dict(instance_types)

    def handle(self, simulator):
        # TODO (CLUSTERMAN-13) handle outbidding events by reverting the price for the last hour
        simulator.cluster.terminate_instances_by_market(self.instance_types)
