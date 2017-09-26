from collections import defaultdict
from heapq import heappop
from heapq import heappush

import arrow
from sortedcontainers import SortedDict

from clusterman.math.piecewise import hour_transform
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.cluster import Cluster
from clusterman.simulator.event import Event
from clusterman.util import get_clusterman_logger


logger = get_clusterman_logger(__name__)


class SimulationMetadata:
    def __init__(self, cluster_name, tag):
        self.cluster = cluster_name
        self.tag = tag
        self.sim_start = None
        self.sim_end = None

    def __enter__(self):
        self.sim_start = arrow.now()

    def __exit__(self, type, value, traceback):
        self.sim_end = arrow.now()

    def __str__(self):
        return f'({self.cluster}, {self.tag}, {self.sim_start}, {self.sim_end})'


class Simulator:
    def __init__(self, metadata, start_time, end_time):
        """ Maintains all of the state for a clusterman simulation

        :param metadata: a SimulationMetadata object
        :param start_time: an arrow object indicating the start of the simulation
        :param end_time: an arrow object indicating the end of the simulation
        """
        self.metadata = metadata
        self.cluster = Cluster()
        self.start_time = start_time
        self.current_time = start_time
        self.end_time = end_time
        self.spot_prices = defaultdict(float)  # TODO not sure this is the best idea...
        self.cost_per_hour = PiecewiseConstantFunction()
        self.capacity = PiecewiseConstantFunction()

        # The event queue holds all of the simulation events, ordered by time
        self.event_queue = []

        # Don't use add_event here or the end_time event will get discarded
        heappush(self.event_queue, Event(self.start_time, msg='Simulation begins'))
        heappush(self.event_queue, Event(self.end_time, msg='Simulation ends'))

    def add_event(self, evt):
        """ Add a new event to the queue; events outside the simulation time bounds will be ignored

        :param evt: an Event object or subclass
        """
        if evt.time >= self.end_time:
            logger.info(f'Adding event after simulation end time ({evt.time}); event ignored')
            return
        elif evt.time < self.current_time:
            logger.info(f'Adding event before self.current_time ({evt.time}); event ignored')
            return

        heappush(self.event_queue, evt)

    def run(self):
        """ Run the simulation until the end, processing each event in the queue one-at-a-time in priority order """
        print(f'Starting simulation from {self.start_time} to {self.end_time}')
        with self.metadata:
            while self.event_queue:
                evt = heappop(self.event_queue)
                self.current_time = evt.time
                logger.event(evt)
                evt.handle(self)
        print('Simulation complete ({time}s)'.format(
            time=(self.metadata.sim_end - self.metadata.sim_start).total_seconds()
        ))

    def cost_data(self, start_time=None, end_time=None, step=None):
        """ Compute the cost for the cluster in the specified time range, grouped into chunks

        :param start_time: the lower bound of the range (if None, use simulation start time)
        :param end_time: the upper bound of the range (if None, use simulation end time)
        :param step: the width of time for each chunk
        :returns: a list of costs for the cluster from start_time to end_time
        """
        start_time = start_time or self.start_time
        end_time = end_time or self.end_time
        return self.cost_per_hour.integrals(start_time, end_time, step, transform=hour_transform)

    def capacity_data(self, start_time=None, end_time=None, step=None):
        """ Compute the capacity for the cluster in the specified time range, grouped into chunks

        :param start_time: the lower bound of the range (if None, use simulation start time)
        :param end_time: the upper bound of the range (if None, use simulation end time)
        :param step: the width of time for each chunk
        :returns: a list of capacities for the cluster from start_time to end_time
        """
        start_time = start_time or self.start_time
        end_time = end_time or self.end_time
        return self.capacity.values(start_time, end_time, step)

    def cost_per_cpu_data(self, start_time=None, end_time=None, step=None):
        """ Compute the cost per CPU for the cluster in the specified time range, grouped into chunks

        :param start_time: the lower bound of the range (if None, use simulation start time)
        :param end_time: the upper bound of the range (if None, use simulation end time)
        :param step: the width of time for each chunk
        :returns: a list of costs per CPU for the cluster from start_time to end_time
        """
        start_time = start_time or self.start_time
        end_time = end_time or self.end_time
        cost_data = self.cost_data(start_time, end_time, step)
        capacity_data = self.capacity_data(start_time, end_time, step)
        return SortedDict([
            (timestamp, cost / capacity)
            for ((timestamp, cost), capacity) in zip(cost_data.items(), capacity_data.values())
            if capacity != 0
        ])
