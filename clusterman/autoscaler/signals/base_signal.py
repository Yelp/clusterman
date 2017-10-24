from abc import ABCMeta
from abc import abstractmethod

from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


class BaseSignal(metaclass=ABCMeta):
    """All autoscaling signals MUST inherit from this class to ensure priorities are set correctly

    In order to be correctly processed by the scaling engine, an auto-scaling signal MUST do two things:
        1. Implement a function self.delta(), which returns the number of units the signal is requesting to scale up or
           down by; this value can be 0 if the signal wishes to maintain the current cluster capacity
        2. In self.delta(), set self.active = True if the signal should be processed by the signal manager

    Additionally, each signal MUST have an entry corresponding to its class name in the config file if it will be
    checked by the signal manager.  This entry SHOULD have a priority setting to determine how the signal will be
    processed relative to other signals that are enabled.  Other signal parameters specified in the config file will be
    passed in to the signal constructor where they can be handled appropriately.

    """

    def __init__(self, cluster, role, signal_config):
        self.active = False
        self.cluster = cluster
        self.role = role
        try:
            self.priority = signal_config['priority']
        except KeyError:
            logger.warn('No signal priority specified for {signal_name}; defaulting to 0'.format(
                signal_name=signal_config['name'],
            ))
            self.priority = 0

    @abstractmethod
    def delta(self, target_capacity):
        pass
