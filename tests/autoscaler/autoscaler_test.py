import arrow
import mock
import pytest
import simplejson as json
import staticconf
from clusterman_metrics import APP_METRICS
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import SYSTEM_METRICS
from simplejson.errors import JSONDecodeError
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.autoscaler import CAPACITY_GAUGE_NAME
from clusterman.autoscaler.util import MetricConfig
from clusterman.autoscaler.util import SignalConfig
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.exceptions import SignalValidationError


@pytest.fixture
def run_timestamp():
    return arrow.get(300)


@pytest.fixture(autouse=True)
def mock_logger():
    with mock.patch('clusterman.autoscaler.autoscaler.logger') as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def pool_configs():
    with staticconf.testing.PatchConfiguration(
        {
            'scaling_limits': {
                'min_capacity': 24,
                'max_capacity': 5000,
                'max_weight_to_add': 200,
                'max_weight_to_remove': 10,
            },
        },
        namespace=POOL_NAMESPACE.format(pool='bar'),
    ):
        yield


@pytest.fixture(autouse=True)
def scaling_configs():
    with staticconf.testing.PatchConfiguration(
        {
            'autoscaling': {
                'default_signal_role': 'clusterman',
                'setpoint': 0.7,
                'setpoint_margin': 0.1,
                'cpus_per_weight': 8,
            },
        },
        namespace=DEFAULT_NAMESPACE,
    ):
        yield


@pytest.fixture
def mock_gauge():
    with mock.patch('yelp_meteorite.create_gauge', autospec=True) as mock_gauge:
        yield mock_gauge


@pytest.fixture
def mock_pool_manager():
    with mock.patch('clusterman.autoscaler.autoscaler.MesosPoolManager', autospec=True) as mock_pool_manager:
        yield mock_pool_manager


@pytest.fixture
def mock_metrics_client():
    yield mock.Mock(spec=ClustermanMetricsBotoClient)


@pytest.fixture
@mock.patch('clusterman.autoscaler.autoscaler.Autoscaler.load_signal_for_app', autospec=True)
def mock_autoscaler(mock_load_signal, mock_metrics_client, mock_pool_manager, mock_gauge):
    with mock.patch('clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient', autospec=True):
        mock_autoscaler = Autoscaler('mesos-test', 'bar', ['bar'])
    mock_autoscaler.signal_config = SignalConfig(
        'CoolSignal',
        'v42',
        12,
        [MetricConfig('cpus_allocated', SYSTEM_METRICS, 10), MetricConfig('cost', APP_METRICS, 30)],
        {}
    )
    mock_autoscaler.signal_conn = mock.Mock()
    mock_autoscaler.mesos_pool_manager.target_capacity = 300
    mock_autoscaler.mesos_pool_manager.min_capacity = staticconf.read_int(
        'scaling_limits.min_capacity', namespace=POOL_NAMESPACE.format(pool='bar')
    )
    mock_autoscaler.mesos_pool_manager.max_capacity = staticconf.read_int(
        'scaling_limits.max_capacity', namespace=POOL_NAMESPACE.format(pool='bar')
    )
    return mock_autoscaler


def signal_config():
    return SignalConfig(
        'CoolSignal',
        'v42',
        3,
        [MetricConfig('metricA', 'system_metrics', 8)],
        {'paramA': 20, 'paramC': 'abc'},
    )


@mock.patch('clusterman.autoscaler.autoscaler.Autoscaler.load_signal_for_app', autospec=True)
def test_autoscaler_init(mock_load_signal, mock_pool_manager, mock_metrics_client, mock_gauge):
    mock_autoscaler = Autoscaler('mesos-test', 'bar', ['app'], metrics_client=mock_metrics_client())

    assert mock_autoscaler.cluster == 'mesos-test'
    assert mock_autoscaler.pool == 'bar'
    assert mock_autoscaler.apps == ['app']

    assert mock_gauge.call_args == mock.call(CAPACITY_GAUGE_NAME, {'cluster': 'mesos-test', 'pool': 'bar'})
    assert mock_autoscaler.capacity_gauge == mock_gauge.return_value

    assert mock_pool_manager.call_args == mock.call('mesos-test', 'bar')
    assert mock_autoscaler.mesos_pool_manager == mock_pool_manager.return_value
    assert mock_autoscaler.metrics_client == mock_metrics_client.return_value

    assert mock_load_signal.call_count == 1


def test_autoscaler_init_too_many_apps():
    with pytest.raises(NotImplementedError):
        Autoscaler('mesos-test', 'bar', ['app1', 'app2'])


@pytest.mark.parametrize('read_config,expected_default', [
    (NoSignalConfiguredException, True),  # no app signal
    (mock.Mock(), False),  # Custom app signal successful
])
@mock.patch('clusterman.autoscaler.autoscaler.read_signal_config', autospec=True)
@mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_connection', autospec=True)
def test_load_signal(mock_init_signal, mock_read_config, mock_autoscaler, read_config, expected_default):
    default_config = signal_config()
    mock_read_config.side_effect = [default_config, read_config]
    mock_autoscaler.load_signal_for_app('bar')
    assert mock_read_config.call_args_list == [
        mock.call(DEFAULT_NAMESPACE),
        mock.call(POOL_NAMESPACE.format(pool='bar')),
    ]

    assert mock_autoscaler.signal_config == (default_config if expected_default else read_config)


@mock.patch('clusterman.autoscaler.autoscaler.sensu_checkin', autospec=True)
@mock.patch('clusterman.autoscaler.autoscaler.read_signal_config', autospec=True)
def test_load_signal_warning(mock_read_config, mock_sensu, mock_autoscaler):
    default_config = mock.Mock()
    mock_read_config.side_effect = [default_config, SignalValidationError]
    mock_autoscaler._init_signal_connection = mock.Mock()
    mock_autoscaler.load_signal_for_app('bar')
    assert mock_sensu.call_count == 1
    assert mock_autoscaler._init_signal_connection.call_count == 1
    assert mock_autoscaler.signal_config == default_config


@mock.patch('clusterman.autoscaler.autoscaler.logger')
@mock.patch('clusterman.autoscaler.autoscaler.load_signal_connection')
class TestInitSignalConnection:
    @pytest.mark.parametrize('use_default', [True, False])
    def test_init_signal_for_app(self, mock_load_signal, mock_logger, use_default, mock_autoscaler):
        app = staticconf.read_string('autoscaling.default_signal_role') if use_default else mock_autoscaler.apps[0]
        mock_autoscaler._init_signal_connection(app, use_default)
        assert mock_load_signal.call_args == mock.call('v42', app, 'CoolSignal')
        assert json.loads(mock_load_signal.return_value.send.call_args[0][0]) == {
            'parameters': mock_autoscaler.signal_config.parameters,
        }
        assert mock_logger.info.call_count == 1

    def test_init_signal_fallback_to_default(self, mock_load_signal, mock_logger, mock_autoscaler):
        default_role = staticconf.read_string('autoscaling.default_signal_role')
        mock_signal_conn = mock.Mock()
        mock_load_signal.side_effect = [SignalConnectionError(), mock_signal_conn]
        mock_autoscaler._init_signal_connection(mock_autoscaler.apps[0], use_default=False)
        assert mock_load_signal.call_args_list == [
            mock.call('v42', mock_autoscaler.apps[0], 'CoolSignal'),
            mock.call('v42', default_role, 'CoolSignal'),
        ]
        assert json.loads(mock_signal_conn.send.call_args[0][0]) == {
            'parameters': mock_autoscaler.signal_config.parameters,
        }
        assert mock_logger.info.call_count == 2

    def test_init_signal_error(self, mock_load_signal, mock_logger, mock_autoscaler):
        default_role = staticconf.read_string('autoscaling.default_signal_role')
        mock_load_signal.side_effect = SignalConnectionError()
        with pytest.raises(SignalConnectionError):
            mock_autoscaler._init_signal_connection(mock_autoscaler.apps[0], use_default=False)
        assert mock_load_signal.call_args_list == [
            mock.call('v42', mock_autoscaler.apps[0], 'CoolSignal'),
            mock.call('v42', default_role, 'CoolSignal'),
        ]


@pytest.mark.parametrize('dry_run', [True, False])
def test_autoscaler_dry_run(dry_run, mock_autoscaler, run_timestamp):
    mock_autoscaler._compute_target_capacity = mock.Mock(return_value=100)
    mock_autoscaler.run(dry_run=dry_run, timestamp=run_timestamp)
    assert mock_autoscaler.capacity_gauge.set.call_args == mock.call(100, {'dry_run': dry_run})
    assert mock_autoscaler._compute_target_capacity.call_args == mock.call(run_timestamp)
    assert mock_autoscaler.mesos_pool_manager.modify_target_capacity.call_count == 1


@pytest.mark.parametrize('end_time', [arrow.get(3600), arrow.get(10000), arrow.get(35000)])
def test_get_metrics(end_time, mock_autoscaler):
    metrics = mock_autoscaler._get_metrics(end_time)
    assert mock_autoscaler.metrics_client.get_metric_values.call_args_list == [
        mock.call(
            'cpus_allocated|cluster=mesos-test,pool=bar',
            SYSTEM_METRICS,
            end_time.shift(minutes=-10).timestamp,
            end_time.timestamp,
        ),
        mock.call(
            'cost',
            APP_METRICS,
            end_time.shift(minutes=-30).timestamp,
            end_time.timestamp,
        )
    ]
    assert 'cpus_allocated' in metrics
    assert 'cost' in metrics


@pytest.mark.parametrize('signal_cpus,total_cpus,expected_capacity', [
    (None, 1000, 125),
    (799, 1000, 125),  # above setpoint, but within setpoint margin
    (980, 1000, 175),  # above setpoint margin
    (601, 1000, 125),  # below setpoint, but within setpoint margin
    (490, 1000, 87.5),  # below setpoint margin
    (1400, 1000, 250),  # above setpoint margin and total
])
@mock.patch('clusterman.autoscaler.autoscaler.evaluate_signal')
def test_compute_target_capacity(mock_evaluate_signal, mock_autoscaler, signal_cpus, total_cpus,
                                 expected_capacity, run_timestamp):
    mock_autoscaler._get_metrics = mock.Mock(return_value=[[1234, 3.5]])
    mock_autoscaler.mesos_pool_manager.target_capacity = total_cpus / staticconf.read_int('autoscaling.cpus_per_weight')
    mock_evaluate_signal.return_value = {'cpus': signal_cpus}
    new_target_capacity = mock_autoscaler._compute_target_capacity(run_timestamp)
    assert new_target_capacity == pytest.approx(expected_capacity)
    assert mock_evaluate_signal.call_args == mock.call([[1234, 3.5]], mock_autoscaler.signal_conn)


@mock.patch('clusterman.autoscaler.autoscaler.evaluate_signal')
def test_evaluate_failed(mock_evaluate_signal, mock_autoscaler, run_timestamp):
    mock_autoscaler._get_metrics = mock.Mock(return_value=[[1234, 3.5]])
    mock_evaluate_signal.side_effect = JSONDecodeError('foo', 'bar', 3)

    with pytest.raises(ClustermanSignalError):
        mock_autoscaler._compute_target_capacity(run_timestamp)
    assert mock_autoscaler._last_signal_traceback == 'bar'


@mock.patch('clusterman.autoscaler.autoscaler.evaluate_signal')
@pytest.mark.parametrize('last_traceback', ['foo', None])
def test_evaluate_failed_again(mock_evaluate_signal, mock_autoscaler, run_timestamp, mock_logger, last_traceback):
    mock_autoscaler._get_metrics = mock.Mock(return_value=[[1234, 3.5]])
    mock_evaluate_signal.side_effect = BrokenPipeError
    mock_autoscaler._last_signal_traceback = last_traceback

    with pytest.raises(ClustermanSignalError):
        mock_autoscaler._compute_target_capacity(run_timestamp)
    if last_traceback:
        assert 'foo' in mock_logger.error.call_args[0][0]
