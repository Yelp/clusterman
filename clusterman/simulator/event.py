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
        """ Sort order is based on time, then priority """
        return (self.time, EVENT_PRIORITIES[self.__class__]) < (other.time, EVENT_PRIORITIES[other.__class__])

    def __str__(self):
        return f'=== Event {self.id} -- {self.time}\t[{self.msg}]'

    def handle(self, simulator):
        """ Subclasses can override this for more complex behaviour

        :param simulator: a clusterman Simulator instance which the event can access to change state
        """
        pass


class ModifyClusterCapacityEvent(Event):
    def __init__(self, time, instance_types, msg=None):
        """ Trigger this event when the cluster capacity changes

        :param instance_types: a dict of InstanceMarket -> integer indicating the new (desired) capacity for the market
        """
        super().__init__(time, msg=msg)
        self.instance_types = dict(instance_types)

    def handle(self, simulator):
        # add the instances to the cluster and compute costs for their first hour
        added_instance_ids = simulator.cluster.modify_capacity(self.instance_types, modify_time=self.time)
        # TODO (CLUSTERMAN-13) handle outbidding events by reverting the price for the last hour for lost instances
        added_instance_prices = {instance_id: 0 for instance_id in added_instance_ids}
        simulator.add_event(ComputeClusterCostEvent(self.time, added_instance_prices))


class ComputeClusterCostEvent(Event):
    def __init__(self, time, instance_ids_with_prices, msg=None):
        """ Trigger this event whenever costs need to be recomputed
        AWS charges new costs when an instance is launched and every hour thereafter

        :param instance_ids: a list of ids to compute costs for
        """
        super().__init__(time, msg=msg)
        self.instance_ids_with_prices = dict(instance_ids_with_prices)

    def handle(self, simulator):
        price_delta = 0
        active_instances = {}
        for instance_id, prev_price in self.instance_ids_with_prices.items():
            if instance_id in simulator.cluster.ids:
                new_price = simulator.spot_prices[simulator.cluster.instances[instance_id].market]
                price_delta += new_price - prev_price
                active_instances[instance_id] = new_price
            else:
                price_delta -= prev_price
        simulator.cost_per_hour.modify_last_value(self.time, price_delta)

        # if there are still active instances, add a new event for 1 hour from now to compute their costs
        if active_instances:
            simulator.add_event(ComputeClusterCostEvent(self.time.shift(hours=1), active_instances))


class SpotPriceChangeEvent(Event):
    def __init__(self, time, spot_prices, msg=None):
        """ Trigger this event whenever spot prices change

        :param spot_prices: a dict of InstanceMarket -> float indicating the new spot prices
        """
        super().__init__(time, msg=msg)
        self.spot_prices = dict(spot_prices)

    def handle(self, simulator):
        for market, price in self.spot_prices.items():
            simulator.spot_prices[market] = price


# Event priorities are used for secondary sorting of events; if event A and B are scheduled at the same
# time and priority(A) < priority(B), A will be processed before B.
EVENT_PRIORITIES = {
    Event: 0,
    ModifyClusterCapacityEvent: 1,
    SpotPriceChangeEvent: 2,

    # compute cluster cost after all other events for deterministic (?) pricing results
    ComputeClusterCostEvent: 3,
}
