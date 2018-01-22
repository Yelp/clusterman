import os
import socket
import subprocess
from collections import namedtuple

import staticconf

from clusterman.exceptions import SignalConfigurationError
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'
SIGNAL_SOCK_NAME = 'clusterman-signal-socket'
SignalConfig = namedtuple(
    'SignalConfig',
    ['name', 'branch_or_tag', 'period_minutes', 'required_metrics', 'parameters'],
)
MetricConfig = namedtuple('MetricConfig', ['name', 'type', 'minute_range'])  # duplicated from clusterman_signals repo


def _get_cache_location():
    return os.path.join(os.path.expanduser("~"), '.cache', 'clusterman')


def _sha_from_branch_or_tag(branch_or_tag):
    result = subprocess.run(
        ['git', 'ls-remote', '--exit-code', SIGNALS_REPO, branch_or_tag],
        stdout=subprocess.PIPE,
        check=True,
    )
    output = result.stdout.decode()
    sha = output.split('\t')[0]
    return sha


def _get_local_signal_directory(branch_or_tag):
    local_repo_cache = _get_cache_location()
    sha = _sha_from_branch_or_tag(branch_or_tag)
    local_path = os.path.join(local_repo_cache, f'clusterman_signals_{sha}')

    if not os.path.exists(local_path):
        subprocess.run(
            ['git', 'clone', '--depth', '1', '--branch', branch_or_tag, SIGNALS_REPO, local_path],
            check=True,
        )
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
    signal_dir = _get_local_signal_directory(branch_or_tag)
    s = socket.socket(socket.AF_UNIX)
    s.bind(f'\0{SIGNAL_SOCK_NAME}')  # this creates an abstract namespace socket which is auto-cleaned on program exit
    s.listen(1)
    subprocess.Popen([
        os.path.join(signal_dir, 'venv', 'bin', 'python'),
        '-m',
        'clusterman_signals.run',
        role,
        signal_name,
        'clusterman-signal-socket',
    ])
    conn, __ = s.accept()
    return conn
