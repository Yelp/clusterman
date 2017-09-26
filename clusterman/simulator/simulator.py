from queue import PriorityQueue

from clusterman.simulator.cluster import Cluster
from clusterman.simulator.event import Event


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
        self.event_queue = PriorityQueue()

        # Don't use add_event here or the end_time event will get discarded
        self.event_queue.put(Event(self.start_time, msg='Simulation begins'))
        self.event_queue.put(Event(self.end_time, msg='Simulation ends'))

    def add_event(self, evt):
        """ Add a new event to the queue; events after the simulation end will be discarded

        :param evt: an Event object or subclass
        """
        if evt.time >= self.end_time:
            print('WARNING: adding event after simulation end time; event ignored')
            return
        elif evt.time < self.current_time:
            print('WARNING: adding event before self.current_time; event ignored')
            return

        self.event_queue.put(evt)

    def run(self):
        """ Run the simulation until the end, processing each event in the queue one-at-a-time in priority order """
        while not self.event_queue.empty():
            evt = self.event_queue.get()
            self.current_time = evt.time
            print(evt)
            evt.handle(self)

    def report_results(self):
        print(f'Total cost: {self.total_cost}')
