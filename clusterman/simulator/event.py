import itertools

from clusterman.exceptions import SimulationError


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
        added_instance_ids, removed_instance_ids = simulator.cluster.modify_capacity(
            self.instance_types,
            modify_time=self.time,
        )

        for id in removed_instance_ids:
            instance = simulator.cluster.all_instances[id]

            # TODO (CLUSTERMAN-22) right now there's no way to add spot instances so this check never gets invoked
            # TODO (CLUSTERMAN-22) add some itests to make sure this is working correctly
            if instance.spot and simulator.spot_prices[instance.market] > instance.bid_price:
                simulator.cost_per_hour.modify_value(instance.bill_time, -instance.last_price)
                instance.last_price = 0

        simulator.add_event(ComputeClusterCostEvent(self.time, added_instance_ids))


class ComputeClusterCostEvent(Event):
    def __init__(self, time, instance_ids, msg=None):
        """ Trigger this event whenever costs need to be recomputed
        AWS charges new costs when an instance is launched and every hour thereafter

        :param instance_ids: a list of ids to compute costs for
        """
        super().__init__(time, msg=msg)
        self.instance_ids = list(instance_ids)

    def handle(self, simulator):
        price_delta = 0
        active_instance_ids, inactive_instance_ids = [], []
        for instance_id in self.instance_ids:
            instance = simulator.cluster.all_instances[instance_id]

            if instance.last_price is not None and self.time != instance.bill_time.shift(hours=1):
                raise SimulationError(f'Instance charged at {self.time} which != {instance.bill_time.shift(hours=1)}')

            if instance.active:
                curr_price = simulator.spot_prices[instance.market]
                price_delta += curr_price
                if instance.last_price:
                    price_delta -= instance.last_price
                instance.last_price = curr_price
                instance.bill_time = self.time
                active_instance_ids.append(instance_id)
            else:
                price_delta -= instance.last_price
                inactive_instance_ids.append(instance_id)
        simulator.cost_per_hour.modify_value(self.time, price_delta)

        # if there are still active instances, add a new event for 1 hour from now to compute their costs
        if active_instance_ids:
            simulator.add_event(ComputeClusterCostEvent(self.time.shift(hours=1), active_instance_ids))
        # TODO (CLUSTERMAN-42) we should call prune_instances automatically
        simulator.cluster.prune_instances(inactive_instance_ids)


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
