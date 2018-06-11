import os
import socket
import struct
import subprocess
import time
from collections import namedtuple
from threading import Thread

import simplejson as json
import staticconf
from clusterman_metrics import APP_METRICS
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS
from simplejson.errors import JSONDecodeError
from staticconf.errors import ConfigurationError

from clusterman.autoscaler.config import get_required_metric_configs
from clusterman.autoscaler.config import MetricConfig
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import MetricsError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.exceptions import SignalValidationError
from clusterman.util import get_clusterman_logger
from clusterman.util import run_subprocess_and_log
from clusterman.util import sha_from_branch_or_tag

logger = get_clusterman_logger(__name__)
ACK = bytes([1])
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'  # TODO (CLUSTERMAN-254) make this a config param
SOCKET_MESG_SIZE = 4096
SOCKET_TIMEOUT_SECONDS = 300
SignalConfig = namedtuple(
    'SignalConfig',
    ['name', 'repo', 'branch_or_tag', 'period_minutes', 'required_metrics', 'parameters'],
)
SIGNAL_LOGGERS = {}


def _generate_metric_key(metric, cluster, pool, app):
    # Create the key to query the datastore with
    if metric.type == SYSTEM_METRICS:
        metric_key = generate_key_with_dimensions(metric.name, {'cluster': cluster, 'pool': pool})
    elif metric.type == APP_METRICS:
        metric_key = app + ',' + metric.name
    else:
        raise MetricsError('Signal cannot read {metric.type} metrics')
    return metric_key


def _init_signal_io_threads(signal_name, signal_process):
    """ Capture stdout/stderr from the signal """
    def log_signal_output(fd, log_fn):
        while True:
            line = fd.readline().decode().strip()
            if not line:
                break
            log_fn(line)
        logger.info('Stopping signal IO threads')

    if signal_name not in SIGNAL_LOGGERS:
        SIGNAL_LOGGERS[signal_name] = (
            get_clusterman_logger(f'{signal_name}.stdout').info,
            get_clusterman_logger(f'{signal_name}.stderr').warning,
        )
    stdout_thread = Thread(
        target=log_signal_output,
        kwargs={'fd': signal_process.stdout, 'log_fn': SIGNAL_LOGGERS[signal_name][0]},
        daemon=True,
    )
    stderr_thread = Thread(
        target=log_signal_output,
        kwargs={'fd': signal_process.stderr, 'log_fn': SIGNAL_LOGGERS[signal_name][1]},
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()


def _get_cache_location():  # pragma: no cover
    """ Store clusterman-specific cached data in ~/.cache/clusterman """
    return os.path.join(os.path.expanduser("~"), '.cache', 'clusterman')


def _get_local_signal_directory(repo, branch_or_tag):
    """ Get the directory path for the local version of clusterman_signals corresponding to a
    particular branch or tag.  Stores the signal in ~/.cache/clusterman/clusterman_signals_{git_sha}
    """
    local_repo_cache = _get_cache_location()
    sha = sha_from_branch_or_tag(repo, branch_or_tag)
    local_path = os.path.join(local_repo_cache, f'clusterman_signals_{sha}')
    subprocess_kwargs = {'cwd': local_path, 'stdout': subprocess.PIPE, 'stderr': subprocess.STDOUT}

    # If we don't have a local copy of the signal, clone it
    if not os.path.exists(local_path):
        # clone the clusterman_signals repo with a specific version into the path at local_path
        # --depth 1 says to squash all the commits to minimize data transfer/disk space
        os.makedirs(local_path)
        run_subprocess_and_log(
            logger,
            ['git', 'clone', '--depth', '1', '--branch', branch_or_tag, repo, local_path],
            **subprocess_kwargs,
        )
    else:
        logger.debug(f'signal version {sha} exists in cache, not re-cloning')

    # Alwasy re-build the signal's virtualenv
    run_subprocess_and_log(logger, ['make', 'clean'], **subprocess_kwargs)
    run_subprocess_and_log(logger, ['make', 'prod'], **subprocess_kwargs)

    return local_path


class Signal:
    def __init__(self, cluster, pool, app, config_namespace, metrics_client, signal_namespace=None):
        """ Create an encapsulation of the Unix sockets via which we communicate with signals

        :param cluster: the name of the cluster this signal is for
        :param pool: the name of the pool this signal is for
        :param app: the name of the application this signal is for
        :param config_namespace: the staticconf namespace we can find the signal config in
        :param metrics_client: the metrics client to use to populate signal metrics
        :param signal_namespace: the namespace in the signals repo to find the signal class
            (if this is None, we default to the app name)
        """
        self.cluster = cluster
        self.pool = pool
        self.app = app
        self.config = self._get_signal_config(config_namespace)
        self.metrics_client = metrics_client
        self.signal_namespace = signal_namespace or self.app
        self._signal_conn = self._start_signal_process()

    def evaluate(self, timestamp, retry_on_broken_pipe=True):
        """ Communicate over a Unix socket with the signal to evaluate its result

        :param timestamp: a Unix timestamp to pass to the signal as the "current time"
        :param retry_on_broken_pipe: if the signal socket pipe is broken, restart the signal process and try again
        :returns: a dict of resource_name -> requested resources from the signal
        :raises SignalConnectionError: if the signal connection fails for some reason
        """
        # Get the required metrics for the signal
        metrics = self._get_metrics(timestamp)

        try:
            # First send the length of the metrics data
            metric_bytes = json.dumps({'metrics': metrics, 'timestamp': timestamp.timestamp}).encode()
            len_metrics = struct.pack('>I', len(metric_bytes))  # bytes representation of the length, packed big-endian
            self._signal_conn.send(len_metrics)
            response = self._signal_conn.recv(SOCKET_MESG_SIZE)
            if response != ACK:
                raise SignalConnectionError(f'Error occurred sending metric length to signal (response={response})')

            # Then send the actual metrics data, broken up into chunks
            for i in range(0, len(metric_bytes), SOCKET_MESG_SIZE):
                self._signal_conn.send(metric_bytes[i:i + SOCKET_MESG_SIZE])
            response = self._signal_conn.recv(SOCKET_MESG_SIZE)
            ack_bit = response[:1]
            if ack_bit != ACK:
                raise SignalConnectionError(f'Error occurred sending metric data to signal (response={response})')

            # Sometimes the signal sends the ack and the reponse "too quickly" so when we call
            # recv above it gets both values.  This should handle that case, or call recv again
            # if there's no more data in the previous message
            response = response[1:] or self._signal_conn.recv(SOCKET_MESG_SIZE)
            logger.info(response)
            return json.loads(response)['Resources']

        except JSONDecodeError as e:
            raise ClustermanSignalError('Signal evaluation failed') from e
        except BrokenPipeError as e:
            if retry_on_broken_pipe:
                logger.error('Signal connection failed; reloading the signal and trying again')
                self._signal_conn = self._start_signal_process()
                return self.evaluate(timestamp, retry_on_broken_pipe=False)
            else:
                raise ClustermanSignalError('Signal evaluation failed') from e

    def _get_signal_config(self, config_namespace):
        """Validate and return autoscaling signal config from the given namespace.

        :param config_namespace: namespace to read values from
        :returns: SignalConfig object with the values filled in
        :raises staticconf.errors.ConfigurationError: if the config namespace is missing a required value
        :raises NoSignalConfiguredException: if the config namespace doesn't define a custom signal
        :raises SignalValidationError: if some signal parameter is incorrectly set
        """
        reader = staticconf.NamespaceReaders(config_namespace)
        try:
            name = reader.read_string('autoscale_signal.name')
        except ConfigurationError:
            raise NoSignalConfiguredException(f'No signal was configured in {config_namespace}')

        period_minutes = reader.read_int('autoscale_signal.period_minutes')
        if period_minutes <= 0:
            raise SignalValidationError(f'Length of signal period must be positive, got {period_minutes}')

        parameter_dict_list = reader.read_list('autoscale_signal.parameters', default=[])
        parameter_dict = {key: value for param_dict in parameter_dict_list for (key, value) in param_dict.items()}

        required_metrics = reader.read_list('autoscale_signal.required_metrics', default=[])
        required_metric_keys = set(MetricConfig._fields)
        required_metric_configs = []
        for metrics_dict in required_metrics:
            missing = required_metric_keys - set(metrics_dict.keys())
            if missing:
                raise SignalValidationError(f'Missing required metric keys {missing} in {metrics_dict}')
            metric_config = {key: metrics_dict[key] for key in metrics_dict if key in required_metric_keys}
            required_metric_configs.append(MetricConfig(**metric_config))
        branch_or_tag = reader.read_string('autoscale_signal.branch_or_tag')
        return SignalConfig(name, SIGNALS_REPO, branch_or_tag, period_minutes, required_metric_configs, parameter_dict)

    def _start_signal_process(self):
        """ Create a connection to the specified signal over a unix socket

        :returns: a socket connection which can read/write data to the specified signal
        """
        signal_dir = _get_local_signal_directory(self.config.repo, self.config.branch_or_tag)

        # this creates an abstract namespace socket which is auto-cleaned on program exit
        s = socket.socket(socket.AF_UNIX)
        s.bind(f'\0{self.signal_namespace}-{self.config.name}-socket')
        s.listen(1)  # only allow one connection at a time
        s.settimeout(SOCKET_TIMEOUT_SECONDS)

        # We have to *create* the socket before starting the subprocess so that the subprocess
        # will be able to connect to it, but we have to start the subprocess before trying to
        # accept connections, because accept blocks
        signal_process = subprocess.Popen(
            [
                os.path.join(signal_dir, 'prodenv', 'bin', 'python'),
                '-m',
                'clusterman_signals.run',
                self.signal_namespace,
                self.config.name,
            ],
            cwd=signal_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _init_signal_io_threads(self.config.name, signal_process)
        time.sleep(2)  # Give the signal subprocess time to start, then check to see if it's running
        return_code = signal_process.poll()
        if return_code:
            raise SignalConnectionError(f'Could not load signal {self.config.name}; aborting')
        signal_conn, __ = s.accept()
        signal_conn.settimeout(SOCKET_TIMEOUT_SECONDS)

        signal_kwargs = json.dumps({'parameters': self.config.parameters})
        signal_conn.send(signal_kwargs.encode())
        logger.info(f'Loaded signal {self.config.name} from {self.signal_namespace}')

        return signal_conn

    def _get_metrics(self, end_time):
        """ Get the metrics required for a signal """

        # We re-query the metrics index every time we evaluate the signal, in case we've started logging
        # new (matching) metrics since the batch has started
        all_required_metrics = get_required_metric_configs(self.app, self.config.required_metrics)
        metrics = {}
        for metric in all_required_metrics:
            start_time = end_time.shift(minutes=-metric.minute_range)

            metric_key = _generate_metric_key(metric, self.cluster, self.pool, self.app)
            __, metrics[metric.name] = self.metrics_client.get_metric_values(
                metric_key,
                metric.type,
                start_time.timestamp,
                end_time.timestamp
            )
        return metrics
