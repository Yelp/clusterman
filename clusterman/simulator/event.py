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
        __, removed_instances = simulator.cluster.modify_size(
            self.instance_types,
            modify_time=self.time,
        )
        simulator.capacity.add_breakpoint(self.time, simulator.cluster.cpu)

        for instance in removed_instances:
            simulator.compute_instance_cost(instance)


class InstancePriceChangeEvent(Event):
    def __init__(self, time, prices, msg=None):
        """ Trigger this event whenever instance prices change

        :param prices: a dict of InstanceMarket -> float indicating the new instance prices
        """
        super().__init__(time, msg=msg)
        self.prices = dict(prices)

    def handle(self, simulator):
        for market, price in self.prices.items():
            simulator.instance_prices[market].add_breakpoint(self.time, price)


# Event priorities are used for secondary sorting of events; if event A and B are scheduled at the same
# time and priority(A) < priority(B), A will be processed before B.
EVENT_PRIORITIES = {
    Event: 0,
    ModifyClusterCapacityEvent: 1,
    InstancePriceChangeEvent: 2,
}
