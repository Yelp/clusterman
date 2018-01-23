import os
import socket
import subprocess
import time
from collections import namedtuple

import staticconf

from clusterman.exceptions import SignalConfigurationError
from clusterman.exceptions import SignalConnectionError
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'
SignalConfig = namedtuple(
    'SignalConfig',
    ['name', 'branch_or_tag', 'period_minutes', 'required_metrics', 'parameters'],
)
MetricConfig = namedtuple('MetricConfig', ['name', 'type', 'minute_range'])  # duplicated from clusterman_signals repo


def _get_cache_location():
    """ Store clusterman-specific cached data in ~/.cache/clusterman """
    return os.path.join(os.path.expanduser("~"), '.cache', 'clusterman')


def _sha_from_branch_or_tag(branch_or_tag):
    """ Convert a branch or tag for clusterman_signals into a git SHA """
    result = subprocess.run(
        ['git', 'ls-remote', '--exit-code', SIGNALS_REPO, branch_or_tag],
        stdout=subprocess.PIPE,
        check=True,
    )
    output = result.stdout.decode()
    sha = output.split('\t')[0]
    return sha


def _get_local_signal_directory(branch_or_tag):
    """ Get the directory path for the local version of clusterman_signals corresponding to a
    particular branch or tag.  Stores the signal in ~/.cache/clusterman/clusterman_signals_{git_sha}
    """
    local_repo_cache = _get_cache_location()
    sha = _sha_from_branch_or_tag(branch_or_tag)
    local_path = os.path.join(local_repo_cache, f'clusterman_signals_{sha}')

    # If we don't have a local copy of the signal, clone it
    if not os.path.exists(local_path):
        # clone the clusterman_signals repo with a specific version into the path at local_path
        # --depth 1 says to squash all the commits to minimize data transfer/disk space
        subprocess.run(
            ['git', 'clone', '--depth', '1', '--branch', branch_or_tag, SIGNALS_REPO, local_path],
            check=True,
        )

        # Build the signal's virtualenv
        subprocess.run(['make', 'venv'], cwd=local_path, check=True)
    else:
        logger.debug(f'signal version {sha} exists in cache, not re-cloning')

    return local_path


def read_signal_config(config_namespace):
    """Validate and return autoscaling signal config from the given namespace.

    If namespace does not contain a signal config, returns None.

    :param config_namespace: namespace to read values from
    :returns SignalConfig
    """
    reader = staticconf.NamespaceReaders(config_namespace)
    name = reader.read_string('autoscale_signal.name', default=None)
    if not name:
        return None
    period_minutes = reader.read_int('autoscale_signal.period_minutes')
    if period_minutes <= 0:
        raise SignalConfigurationError(f'Length of signal period must be positive, got {period_minutes}')
    metrics_dict_list = reader.read_list('autoscale_signal.required_metrics', default=[])
    parameter_dict_list = reader.read_list('autoscale_signal.custom_parameters', default=[])

    parameter_dict = {key: value for param_dict in parameter_dict_list for (key, value) in param_dict.items()}

    branch_or_tag = reader.read_string('autoscale_signal.branch_or_tag')
    required_metric_keys = set(MetricConfig._fields)
    metric_configs = []
    for metrics_dict in metrics_dict_list:
        missing = required_metric_keys - set(metrics_dict.keys())
        if missing:
            raise SignalConfigurationError(f'Missing required metric keys {missing} in {metrics_dict}')
        metric_config = {key: metrics_dict[key] for key in metrics_dict if key in required_metric_keys}
        metric_configs.append(MetricConfig(**metric_config))
    return SignalConfig(name, branch_or_tag, period_minutes, metric_configs, parameter_dict)


def load_signal_connection(branch_or_tag, role, signal_name):
    """ Create a connection to the specified signal over a unix socket

    :param branch_or_tag: the git branch or tag for the version of the signal to use
    :param role: the role we are loading the signal for
    :param signal_name: the name of the signal we want to load
    :returns: a socket connection which can read/write data to the specified signal
    """
    signal_dir = _get_local_signal_directory(branch_or_tag)

    # this creates an abstract namespace socket which is auto-cleaned on program exit
    s = socket.socket(socket.AF_UNIX)
    s.bind(f'\0{role}-{signal_name}-socket')
    s.listen(1)

    # We have to *create* the socket before starting the subprocess so that the subprocess
    # will be able to connect to it, but we have to start the subprocess before trying to
    # accept connections, because accept blocks
    signal_process = subprocess.Popen(
        [
            os.path.join(signal_dir, 'venv', 'bin', 'python'),
            '-m',
            'clusterman_signals.run',
            role,
            signal_name,
        ],
    )
    time.sleep(2)  # Give the signal subprocess time to start, then check to see if it's running
    return_code = signal_process.poll()
    if return_code:
        raise SignalConnectionError(f'Could not load signal {signal_name}; aborting')
    conn, __ = s.accept()
    return conn
