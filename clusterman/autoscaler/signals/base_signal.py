from abc import ABCMeta
from abc import abstractmethod
from collections import namedtuple

from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
SignalResult = namedtuple('SignalResult', ['active', 'delta'])
SignalResult.__new__.__defaults__ = (False, 0)


class BaseSignal(metaclass=ABCMeta):
    """ All autoscaling signals MUST inherit from this class to ensure priorities are set correctly.

    Additionally, each signal MUST have an entry corresponding to its class name in the config file if it will be
    checked by the signal manager.  This entry SHOULD have a priority setting to determine how the signal will be
    processed relative to other signals that are enabled.  Other signal parameters specified in the config file will be
    passed in to the signal constructor where they can be handled appropriately.
    """

    def __init__(self, cluster, role, signal_config):
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
    def __call__(self):  # pragma: no cover
        """ Check to see if the signal has fired or not

        :returns: a SignalResult
        """
        pass
