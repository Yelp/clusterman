from collections import namedtuple

import staticconf

from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
AutoscalingConfig = namedtuple(
    'AutoscalingConfig',
    ['setpoint', 'setpoint_margin'],
)
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'  # TODO (CLUSTERMAN-254) make this a config param


def get_autoscaling_config(config_namespace):
    """ Load autoscaling configuration values from the provided config_namespace, falling back to the
    values stored in the default namespace if none are specified.

    :param config_namespace: namespace to read from before falling back to the default namespace
    :returns: AutoscalingConfig object with loaded config values
    """
    default_setpoint = staticconf.read_float('autoscaling.setpoint')
    default_setpoint_margin = staticconf.read_float('autoscaling.setpoint_margin')

    reader = staticconf.NamespaceReaders(config_namespace)
    return AutoscalingConfig(
        setpoint=reader.read_float('autoscaling.setpoint', default=default_setpoint),
        setpoint_margin=reader.read_float('autoscaling.setpoint_margin', default=default_setpoint_margin),
    )
