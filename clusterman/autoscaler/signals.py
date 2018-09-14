import os
import socket
import struct
import subprocess
import time
from collections import defaultdict
from threading import Thread
from typing import Callable
from typing import Dict
from typing import Mapping
from typing import Optional
from typing import Tuple

import arrow
import colorlog
import simplejson as json
import staticconf
from clusterman_metrics import APP_METRICS
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import MetricsValuesDict
from clusterman_metrics import SYSTEM_METRICS
from mypy_extensions import TypedDict
from simplejson.errors import JSONDecodeError
from staticconf.errors import ConfigurationError

from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import MetricsError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.exceptions import SignalValidationError
from clusterman.util import run_subprocess_and_log
from clusterman.util import sha_from_branch_or_tag

logger = colorlog.getLogger(__name__)
ACK = bytes([1])
DEFAULT_SIGNALS_REPO = 'git@sysgit.yelpcorp.com:clusterman_signals'
SOCKET_MESG_SIZE = 4096
SOCKET_TIMEOUT_SECONDS = 300
SIGNAL_LOGGERS: Mapping[
    str,
    Tuple[
        Callable[[str], None],
        Callable[[str], None],
    ]
] = {}
MetricsConfigDict = TypedDict(
    'MetricsConfigDict',
    {
        'name': str,
        'type': str,
        'minute_range': int,
        'regex': bool,
    }
)


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
            colorlog.getLogger(f'{signal_name}.stdout').info,
            colorlog.getLogger(f'{signal_name}.stderr').warning,
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
    return os.path.join(os.path.expanduser('~'), '.cache', 'clusterman')


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
    def __init__(
        self,
        cluster: str,
        pool: str,
        app: str,
        config_namespace: str,
        metrics_client: ClustermanMetricsBotoClient,
        signal_namespace: Optional[str] = None
    ) -> None:
        """ Create an encapsulation of the Unix sockets via which we communicate with signals

        :param cluster: the name of the cluster this signal is for
        :param pool: the name of the pool this signal is for
        :param app: the name of the application this signal is for
        :param config_namespace: the staticconf namespace we can find the signal config in
        :param metrics_client: the metrics client to use to populate signal metrics
        :param signal_namespace: the namespace in the signals repo to find the signal class
            (if this is None, we default to the app name)
        """
        reader = staticconf.NamespaceReaders(config_namespace)

        try:
            self.name: str = reader.read_string('autoscale_signal.name')
        except ConfigurationError:
            raise NoSignalConfiguredException(f'No signal was configured in {config_namespace}')

        self.cluster: str = cluster
        self.pool: str = pool
        self.app: str = app

        self.period_minutes: int = reader.read_int('autoscale_signal.period_minutes')
        if self.period_minutes <= 0:
            raise SignalValidationError(f'Length of signal period must be positive, got {self.period_minutes}')

        self.parameters: Dict = {
            key: value
            for param_dict in reader.read_list('autoscale_signal.parameters', default=[])
            for (key, value) in param_dict.items()
        }

        self.required_metrics: list = reader.read_list('autoscale_signal.required_metrics', default=[])

        self.repo: str = reader.read_string('autoscale_signal.repository', default=DEFAULT_SIGNALS_REPO)
        self.branch_or_tag: str = reader.read_string('autoscale_signal.branch_or_tag')

        self.metrics_client: ClustermanMetricsBotoClient = metrics_client
        self.signal_namespace: str = signal_namespace or self.app
        self._signal_conn: socket.socket = self._start_signal_process()

    def evaluate(self, timestamp: arrow.Arrow, retry_on_broken_pipe: bool = True) -> Dict:
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

            # simplejson auto-decodes bytes data but mypy doesn't like it
            return json.loads(response)['Resources']  # type: ignore

        except JSONDecodeError as e:
            raise ClustermanSignalError('Signal evaluation failed') from e
        except BrokenPipeError as e:
            if retry_on_broken_pipe:
                logger.error('Signal connection failed; reloading the signal and trying again')
                self._signal_conn = self._start_signal_process()
                return self.evaluate(timestamp, retry_on_broken_pipe=False)
            else:
                raise ClustermanSignalError('Signal evaluation failed') from e

    def _start_signal_process(self) -> socket.socket:
        """ Create a connection to the specified signal over a unix socket

        :returns: a socket connection which can read/write data to the specified signal
        """
        signal_dir = _get_local_signal_directory(self.repo, self.branch_or_tag)

        # this creates an abstract namespace socket which is auto-cleaned on program exit
        s = socket.socket(socket.AF_UNIX)
        s.bind(f'\0{self.signal_namespace}-{self.name}-socket')
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
                self.name,
            ],
            cwd=signal_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _init_signal_io_threads(self.name, signal_process)
        time.sleep(2)  # Give the signal subprocess time to start, then check to see if it's running
        return_code = signal_process.poll()
        if return_code:
            raise SignalConnectionError(f'Could not load signal {self.name}; aborting')
        signal_conn, __ = s.accept()
        signal_conn.settimeout(SOCKET_TIMEOUT_SECONDS)

        signal_kwargs = json.dumps({'parameters': self.parameters})
        signal_conn.send(signal_kwargs.encode())
        logger.info(f'Loaded signal {self.name} from {self.signal_namespace}')

        return signal_conn

    def _get_metrics(self, end_time: arrow.Arrow) -> MetricsValuesDict:
        """ Get the metrics required for a signal """

        metrics: MetricsValuesDict = defaultdict(list)
        metric_dict: MetricsConfigDict
        for metric_dict in self.required_metrics:
            if metric_dict['type'] not in (SYSTEM_METRICS, APP_METRICS):
                raise MetricsError(f"Metrics of type {metric_dict['type']} cannot be queried by signals.")

            # Need to add the cluster/pool to get the right system metrics
            # TODO (CLUSTERMAN-126) this should probably be cluster/pool/app eventually
            dims = {'cluster': self.cluster, 'pool': self.pool} if metric_dict['type'] == SYSTEM_METRICS else {}

            # We only support regex expressions for APP_METRICS
            if 'regex' not in metric_dict:
                metric_dict['regex'] = False

            start_time = end_time.shift(minutes=-metric_dict['minute_range'])
            query_results = self.metrics_client.get_metric_values(
                metric_dict['name'],
                metric_dict['type'],
                start_time.timestamp,
                end_time.timestamp,
                is_regex=metric_dict['regex'],
                extra_dimensions=dims,
                app_identifier=self.app,
            )
            metrics.update(query_results)
        return metrics
