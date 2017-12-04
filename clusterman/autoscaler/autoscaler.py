import inspect
from collections import defaultdict

import staticconf
import yelp_meteorite

import clusterman.autoscaler.signals.downscale as DownscaleSignals
import clusterman.autoscaler.signals.upscale as UpscaleSignals
from clusterman.mesos.constants import ROLE_NAMESPACE
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import get_clusterman_logger


DELTA_GAUGE_NAME = 'clusterman.autoscaler.delta'
logger = get_clusterman_logger(__name__)


# TODO (CLUSTERMAN-105) Figure out a better way to read input signals
def _read_signals():
    """ Create a dictionary mapping of name->class for all signals that we know about

    This feels a little hack-y to me, but it works for now
    """
    sdict = {}
    for name, cls in inspect.getmembers(UpscaleSignals, inspect.isclass):
        sdict[name] = cls
    for name, cls in inspect.getmembers(DownscaleSignals, inspect.isclass):
        sdict[name] = cls
    return sdict


class Autoscaler:
    def __init__(self, cluster, role):
        self.cluster = cluster
        self.role = role
        logger.info(f'Initializing autoscaler engine for {self.role} in {self.cluster}...')
        self.config = staticconf.NamespaceReaders(ROLE_NAMESPACE.format(role=self.role))
        self.delta_gauge = yelp_meteorite.create_gauge(DELTA_GAUGE_NAME, {'cluster': cluster, 'role': role})
        logger.info('Connecting to Mesos')
        self.mesos_role_manager = MesosRoleManager(self.cluster, self.role)
        logger.info('Loading autoscaling signals')
        self.signals = self._load_signals()
        logger.info('Initialization complete')

    def run(self, dry_run=False):
        """ Do a single check to scale the fleet up or down if necessary.

        :param dry_run: Don't actually modify the fleet size, just print what would happen
        """
        delta = self._compute_cluster_delta()
        self.delta_gauge.set(delta, {'dry_run': dry_run})
        new_target_capacity = self.mesos_role_manager.target_capacity + delta
        if dry_run:
            logger.warn('This is a dry run: cluster size will not change.')
        else:
            self.mesos_role_manager.modify_target_capacity(new_target_capacity)

    def load_signal(self):
        self.signals = self._load_signals()

    def _load_signals(self):
        """ Load the signals used by the autoscaler, based on the values in srv-configs and the signals directory """
        active_signals = defaultdict(list)
        signals = _read_signals()
        for signal_config in self.config.read_list('autoscale_signals'):
            signal_name = signal_config['name']
            signal_init = None
            try:
                signal_init = signals[signal_name]
            except KeyError:
                logger.warn(f'Unknown signal {signal_name}; ignoring')
                continue

            signal = None
            try:
                signal = signal_init(self.cluster, self.role, signal_config)
            except KeyError as e:
                logger.warn(f'Could not load signal {signal_name}, missing config value; ignoring ({e})')
                continue

            logger.info(f'Registering signal {signal_name} (priority {signal.priority})')
            active_signals[signal.priority].append(signal)

        return active_signals

    def _compute_cluster_delta(self):
        """ Iterate through all signals and evaluate them

        :returns: the requested capacity change of the first activated signal, or 0 if no signals fire
        """
        # Sort the signals in priority order and evaluate them; if two signals have the same
        # priority, they are executed in an arbitrary order.  TODO (CLUSTERMAN-104)
        for __, signals in sorted(self.signals.items()):
            for signal in signals:
                signal_name = signal.__class__.__name__
                logger.info(f'Evaluating signal {signal_name}')
                try:
                    signal_result = signal()
                except Exception as err:
                    logger.error(f'Error in signal handling, skipping {signal_name}: {err}')
                    continue
                logger.info(f'Done with signal {signal_name}')

                if signal_result.active:
                    delta = self._constrain_cluster_delta(signal_result.delta)
                    logger.info(f'Signal {signal_name} activated; cluster capacity changing by {delta} weight.')
                    return delta

        logger.info('No signals were activated; cluster size will not change')
        return 0

    def _constrain_cluster_delta(self, delta):
        """ Signals can return arbitrary values, so make sure we don't add or remove too much capacity """
        if delta > 0:
            return min(
                self.config.read_int('defaults.max_capacity') - self.mesos_role_manager.target_capacity,
                self.config.read_int('defaults.max_weight_to_add'),
                delta,
            )
        elif delta < 0:
            return max(
                self.config.read_int('defaults.min_capacity') - self.mesos_role_manager.target_capacity,
                -self.config.read_int('defaults.max_weight_to_remove'),
                delta,
            )
        else:
            return 0
