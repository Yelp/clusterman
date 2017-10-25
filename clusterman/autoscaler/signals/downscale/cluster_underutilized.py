from clusterman.autoscaler.signals.base_signal import BaseSignal
from clusterman.autoscaler.util import get_average_cpu_util


class ClusterUnderutilizedSignal(BaseSignal):
    """If the cluster usage is too low, scale down"""

    def __init__(self, cluster, role, signal_config):
        BaseSignal.__init__(self, cluster, role, signal_config)

        self.query_period = signal_config['query_period_minutes'] * 60.0
        self.scale_down_threshold = signal_config['scale_down_threshold']
        self.units_to_remove = signal_config['units_to_remove']

    def delta(self):
        avg_cpu_util = get_average_cpu_util(self.cluster, self.role, self.query_period)
        avg_cpu_util_half = get_average_cpu_util(self.cluster, self.role, self.query_period / 2.0)

        if max(avg_cpu_util, avg_cpu_util_half) <= self.scale_down_threshold:
            self._active = True
            return -self.units_to_remove

        return 0
