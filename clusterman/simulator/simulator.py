from collections import defaultdict
from datetime import timedelta
from heapq import heappop
from heapq import heappush

import arrow

from clusterman.math.piecewise import hour_transform
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.reports.cost import make_cost_report
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
        with self.metadata:
            while self.event_queue:
                evt = heappop(self.event_queue)
                self.current_time = evt.time
                logger.event(evt)
                evt.handle(self)
        print(self.metadata)

    def cost(self, start_time=None, end_time=None):
        """ Compute the cost for the in the specified time range

        :param start_time: the lower bound of the range (if None, use simulation start time)
        :param end_time: the upper bound of the range (if None, use simulation end time)
        :returns: the total cost for the cluster in the specified time range
        """
        start_time = start_time or self.start_time
        end_time = end_time or self.end_time
        return self.cost_per_hour.integral(start_time, end_time, transform=hour_transform)

    def make_report(self):
        """ Generate a report about the cluster usage and cost """
        print('Analyzing simulation data')
        cost_data = self.cost_per_hour.integrals(
            self.start_time,
            self.end_time,
            timedelta(seconds=60),
            transform=hour_transform,
        )
        print('Generating cost report')
        fig = make_cost_report(self.metadata, cost_data, self.start_time, self.end_time)
        fig.savefig('test.pdf')
