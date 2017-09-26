from heapq import heappop
from heapq import heappush

from clusterman.simulator.cluster import Cluster
from clusterman.simulator.event import Event
from clusterman.util import get_clusterman_logger


logger = get_clusterman_logger(__name__)


class Simulator:
    def __init__(self, start_time, end_time):
        """ Maintains all of the state for a clusterman simulation

        :param start_time: an arrow object indicating the start of the simulation
        :param end_time: an arrow object indicating the end of the simulation
        """
        self.cluster = Cluster()
        self.start_time = start_time
        self.current_time = start_time
        self.end_time = end_time
        self.spot_prices = {}
        self.total_cost = 0.0

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
        while self.event_queue:
            evt = heappop(self.event_queue)
            self.current_time = evt.time
            print(evt)
            evt.handle(self)

    def report_results(self):
        print(f'Total cost: {self.total_cost}')
