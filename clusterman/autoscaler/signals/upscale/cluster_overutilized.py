from clusterman.autoscaler.signals.base_signal import BaseSignal
from clusterman.autoscaler.util import get_average_cpu_util


class ClusterOverutilizedSignal(BaseSignal):
    """If the cluster is close to full capacity, scale up"""

    def __init__(self, cluster, role, signal_config):
        BaseSignal.__init__(self, cluster, role, signal_config)

        self.query_period = signal_config['query_period_minutes'] * 60
        self.units_to_add = signal_config['units_to_add']
        self.scale_up_threshold = signal_config['scale_up_threshold']

    def delta(self):
        if get_average_cpu_util(self.cluster, self.role, self.query_period) >= self.scale_up_threshold:
            self._active = True
            return self.units_to_add

        return 0
