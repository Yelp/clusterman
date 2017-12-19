import json
from collections import defaultdict
from datetime import timedelta
from heapq import heappop
from heapq import heappush

import arrow
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import METADATA
from sortedcontainers import SortedDict

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.aws.markets import get_instance_market
from clusterman.math.piecewise import hour_transform
from clusterman.math.piecewise import piecewise_breakpoint_generator
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.event import Event
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster
from clusterman.simulator.simulated_mesos_role_manager import SimulatedMesosRoleManager
from clusterman.util import get_clusterman_logger


logger = get_clusterman_logger(__name__)


class SimulationMetadata:
    def __init__(self, cluster_name, role):
        self.cluster = cluster_name
        self.role = role
        self.sim_start = None
        self.sim_end = None

    def __enter__(self):
        self.sim_start = arrow.now()

    def __exit__(self, type, value, traceback):
        self.sim_end = arrow.now()

    def __str__(self):
        return f'({self.cluster}, {self.role}, {self.sim_start}, {self.sim_end})'


class Simulator:
    def __init__(self, metadata, start_time, end_time, autoscaler_config, metrics_client,
                 billing_frequency=timedelta(seconds=1), refund_outbid=True):
        """ Maintains all of the state for a clusterman simulation

        :param metadata: a SimulationMetadata object
        :param start_time: an arrow object indicating the start of the simulation
        :param end_time: an arrow object indicating the end of the simulation
        :param billing_frequency: a timedelta object indicating how often to charge for an instance
        :param refund_outbid: if True, do not incur any cost for an instance lost to an outbid event
        """
        self.metadata = metadata
        self.metrics_client = metrics_client
        if autoscaler_config:
            print(f'Autoscaler configured; will run every frequency {self.autoscaler.signal.period_minutes} minutes')
            self._make_autoscaler(autoscaler_config)
            self.aws_clusters = self.autoscaler.mesos_role_manager.resource_groups
        else:
            print('No autoscaler configured; using metrics for cluster size')
            self.autoscaler = None
            self.aws_clusters = [SimulatedAWSCluster(self)]

        self.autoscaler = None
        self.start_time = start_time
        self.current_time = start_time
        self.end_time = end_time

        self.billing_frequency = billing_frequency
        self.refund_outbid = refund_outbid

        self.instance_prices = defaultdict(lambda: PiecewiseConstantFunction())
        self.cost_per_hour = PiecewiseConstantFunction()
        self.capacity = PiecewiseConstantFunction()
        self.markets = set()

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

        # charge any instances that haven't been terminated yet
        for cluster in self.aws_clusters:
            for instance in cluster.instances.values():
                instance.end_time = self.current_time
                self.compute_instance_cost(instance)

        print('Simulation complete ({time}s)'.format(
            time=(self.metadata.sim_end - self.metadata.sim_start).total_seconds()
        ))

    def compute_instance_cost(self, instance):
        """ Adjust the cost-per-hour function to account for the specified instance

        :param instance: an Instance object to compute costs for; the instance must have a start_time and end_time
        """

        # Charge for the price of the instance when it is launched
        prices = self.instance_prices[instance.market]
        curr_timestamp = instance.start_time
        delta, last_billed_price = 0, prices.call(curr_timestamp)
        self.cost_per_hour.add_delta(curr_timestamp, last_billed_price)

        # Loop through all the breakpoints in the instance_prices function (in general this should be more efficient
        # than looping through the billing times, as long as billing happens more frequently than price change
        # events; this is expected to be the case for billing frequencies of ~1s)
        for bp_timestamp in piecewise_breakpoint_generator(prices.breakpoints, instance.start_time, instance.end_time):

            # if the breakpoint exceeds the next billing point, we need to charge for that billing point
            # based on whatever the most recent breakpoint value before the billing point was (this is tracked
            # in the delta variable).  Then, we need to advance the current time to the billing point immediately
            # preceding (and not equal to) the breakpoint
            if bp_timestamp >= curr_timestamp + self.billing_frequency:
                self.cost_per_hour.add_delta(curr_timestamp + self.billing_frequency, delta)

                # we assume that if the price change and the billing point occur simultaneously,
                # that the instance is charged with the new price; so, we step the curr_timestep back
                # so that this block will get triggered on the next time through the loop
                jumps, remainder = divmod(bp_timestamp - curr_timestamp, self.billing_frequency)
                if not remainder:
                    jumps -= 1
                curr_timestamp += jumps * self.billing_frequency
                last_billed_price += delta

            # piecewise_breakpoint_generator includes instance.end_time in the list of results, so that we can do
            # one last price check (the above if block) before the instance gets terminated.  However, that means
            # here we only want to update delta if the timestamp is a real breakpoint
            if bp_timestamp in prices.breakpoints:
                delta = prices.breakpoints[bp_timestamp] - last_billed_price

        # TODO (CLUSTERMAN-54) add some itests to make sure this is working correctly
        # Determine whether or not to bill for the last billing period of the instance.  We charge for the last billing period if
        # any of the following conditions are met:
        #   a) the instance is not a spot instance
        #   b) self.refund_outbid is false, e.g. we have "new-style" AWS pricing
        #   c) the instance bid price (when it was terminated) is greater than the current spot price
        if not instance.spot or not self.refund_outbid or instance.bid_price > prices.call(instance.end_time):
            curr_timestamp += self.billing_frequency
        self.cost_per_hour.add_delta(curr_timestamp, -last_billed_price)

    @property
    def total_cost(self):
        return self.cost_data().values()[0]

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

    def _make_autoscaler(self, autoscaler_config):
        with open(autoscaler_config) as f:
            config = json.load(f)
        # TODO (CLUSTERMAN-154) we need to load multiple SFRs here
        role_manager = SimulatedMesosRoleManager(self.metadata.cluster, self.metadata.role, [config], self)
        actual_target_capacities = self.metrics_client.get_metric_values(
            generate_key_with_dimensions(
                'target_capacity',
                {'cluster': self.metadata.cluster, 'role': self.metadata.role},
            ),
            METADATA,
            self.start_time,
            self.start_time.shift(seconds=30),  # metrics collector runs 1x/min, so this should just get one data point
        )
        role_manager.modify_target_capacity(actual_target_capacities[0])
        for spec in config['LaunchSpecifications']:
            self.markets |= {get_instance_market(spec)}
        self.autoscaler = Autoscaler(
            self.metadata.cluster,
            self.metadata.role,
            role_manager,
            self.metrics_client,
        )
