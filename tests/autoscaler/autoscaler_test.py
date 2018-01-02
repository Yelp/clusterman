import arrow
import mock
import pytest
import staticconf
from clusterman_metrics import ClustermanMetricsSimulationClient
from clusterman_signals.base_signal import BaseSignal
from clusterman_signals.base_signal import MetricConfig
from clusterman_signals.base_signal import SignalResources
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.autoscaler import DELTA_GAUGE_NAME
from clusterman.autoscaler.util import SignalConfig
from clusterman.mesos.constants import ROLE_NAMESPACE


@pytest.fixture(autouse=True)
def mock_logger():
    with mock.patch('clusterman.autoscaler.autoscaler.logger') as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def role_configs():
    with staticconf.testing.PatchConfiguration(
        {
            'scaling_limits': {
                'min_capacity': 24,
                'max_capacity': 5000,
                'max_weight_to_add': 200,
                'max_weight_to_remove': 10,
            },
        },
        namespace=ROLE_NAMESPACE.format(role='bar'),
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
def mock_import_signals():
    with mock.patch('clusterman.autoscaler.autoscaler.import_module') as mock_import:
        yield mock_import


@pytest.fixture
def mock_role_manager():
    with mock.patch('clusterman.autoscaler.autoscaler.MesosRoleManager', autospec=True) as mock_role_manager:
        yield mock_role_manager


@pytest.fixture
def mock_metrics_client():
    yield mock.Mock(spec=ClustermanMetricsSimulationClient)


@pytest.fixture
@mock.patch('clusterman.autoscaler.autoscaler.Autoscaler.load_signal', autospec=True)
def mock_autoscaler(mock_load_signal, mock_role_manager, mock_gauge):
    mock_autoscaler = Autoscaler('foo', 'bar')
    mock_autoscaler.mesos_role_manager.target_capacity = 300
    mock_autoscaler.mesos_role_manager.min_capacity = staticconf.read_int(
        'scaling_limits.min_capacity', namespace=ROLE_NAMESPACE.format(role='bar')
    )
    mock_autoscaler.mesos_role_manager.max_capacity = staticconf.read_int(
        'scaling_limits.max_capacity', namespace=ROLE_NAMESPACE.format(role='bar')
    )
    return mock_autoscaler


@pytest.fixture
def mock_constrain_delta():
    with mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._constrain_cluster_delta', autospec=True) as mock_constrain:
        mock_constrain.side_effect = lambda cls, x: x
        yield mock_constrain


@pytest.fixture
def mock_signal():
    return mock.Mock(spec=BaseSignal)


def signal_config():
    return SignalConfig(
        'CoolSignal',
        3,
        [MetricConfig('metricA', 'system_metrics', 8)],
        {'paramA': 20, 'paramC': 'abc'},
    )


@mock.patch('clusterman.autoscaler.autoscaler.Autoscaler.load_signal', autospec=True)
def test_autoscaler_init(mock_load_signal, mock_role_manager, mock_metrics_client, mock_gauge):
    mock_autoscaler = Autoscaler('foo', 'bar', None, mock_metrics_client())

    assert mock_autoscaler.cluster == 'foo'
    assert mock_autoscaler.role == 'bar'

    assert mock_gauge.call_args_list == [mock.call(DELTA_GAUGE_NAME, {'cluster': 'foo', 'role': 'bar'})]
    assert mock_autoscaler.delta_gauge == mock_gauge.return_value

    assert mock_role_manager.call_args_list == [mock.call('foo', 'bar')]
    assert mock_autoscaler.mesos_role_manager == mock_role_manager.return_value
    assert mock_autoscaler.metrics_client == mock_metrics_client.return_value

    assert mock_load_signal.call_count == 1


@pytest.mark.parametrize('role_config,role_signal,expected_default', [
    (None, Exception, True),  # no role signal
    (Exception, Exception, True),  # invalid role signal config
    (signal_config(), Exception, True),  # loading role signal fails
    (signal_config(), mock_signal(), False),  # Custom role signal successful
])
@mock.patch('clusterman.autoscaler.autoscaler.read_signal_config', autospec=True)
@mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._init_signal_from_config', autospec=True)
def test_load_signal(mock_init_signal, mock_read_config, mock_autoscaler, role_config, role_signal, expected_default):
    # Set up mocks according to test parameters
    default_signal = mock_signal()
    default_config = signal_config()
    if isinstance(role_config, SignalConfig):  # mock a return value for the role signal, since there's a config for it
        mock_init_signal.side_effect = [role_signal, default_signal]
    else:
        mock_init_signal.side_effect = [default_signal]
    mock_read_config.side_effect = [role_config, default_config]

    mock_autoscaler.load_signal()
    mock_read_config.assert_any_call(ROLE_NAMESPACE.format(role='bar'))
    if expected_default:
        # call args is most recent call
        assert mock_init_signal.call_args == mock.call(mock_autoscaler, 'clusterman', default_config)
        assert mock_read_config.call_args == mock.call(DEFAULT_NAMESPACE)
        assert mock_autoscaler.signal == default_signal
    else:
        assert mock_init_signal.call_args == mock.call(mock_autoscaler, 'bar', role_config)
        assert mock_autoscaler.signal == role_signal


def test_init_signal_from_config(mock_import_signals, mock_autoscaler):
    config = signal_config()
    config_role = 'anything'
    val = mock_autoscaler._init_signal_from_config(config_role, config)
    assert mock_import_signals.call_args_list == [mock.call(f'clusterman_signals.{config_role}')]
    assert mock_import_signals.return_value.CoolSignal.call_args_list == [mock.call(
        'foo',
        'bar',
        period_minutes=config.period_minutes,
        required_metrics=config.required_metrics,
        custom_parameters=config.custom_parameters,
        metrics_client=mock_autoscaler.metrics_client,
    )]
    assert val == mock_import_signals.return_value.CoolSignal.return_value


@pytest.mark.parametrize('dry_run', [True, False])
@pytest.mark.parametrize('run_timestamp', [None, arrow.get(300)])
def test_autoscaler_dry_run(dry_run, run_timestamp, mock_autoscaler):
    mock_autoscaler._compute_cluster_delta = mock.Mock(return_value=100)
    mock_autoscaler.run(dry_run=dry_run, timestamp=run_timestamp)
    assert mock_autoscaler.delta_gauge.set.call_args_list == [mock.call(100, {'dry_run': dry_run})]
    assert mock_autoscaler._compute_cluster_delta.call_args_list == [mock.call(run_timestamp)]
    assert mock_autoscaler.mesos_role_manager.modify_target_capacity.call_count == 1


@pytest.mark.parametrize('signal_cpus,total_cpus,expected_delta', [
    (None, 500, 0),
    (799, 1000, 0),  # above setpoint, but within setpoint delta
    (980, 1000, 50),  # above setpoint delta
    (601, 1000, 0),  # below setpoint, but within setpoint delta
    (490, 1000, -37.5),  # below setpoint delta
    (1400, 1000, 125),  # above setpoint delta and total
])
@pytest.mark.parametrize('run_timestamp', [None, arrow.get(300)])
def test_compute_cluster_delta(run_timestamp, mock_autoscaler, mock_signal,
                               mock_constrain_delta, signal_cpus, total_cpus, expected_delta):
    mock_signal.get_signal.return_value = SignalResources(signal_cpus, None, None)
    mock_autoscaler.signal = mock_signal
    mock_autoscaler.mesos_role_manager.get_resource_total.return_value = total_cpus
    delta = mock_autoscaler._compute_cluster_delta(run_timestamp)
    assert delta == pytest.approx(expected_delta)
    if delta != 0:
        assert mock_constrain_delta.call_count == 1
    assert mock_autoscaler.signal.get_signal.call_args_list == [mock.call(run_timestamp)]


def test_constrain_cluster_delta_normal_scale_up(mock_autoscaler):
    delta = mock_autoscaler._constrain_cluster_delta(100)
    assert delta == 100


def test_constrain_cluster_delta_normal_scale_down(mock_autoscaler):
    delta = mock_autoscaler._constrain_cluster_delta(-5)
    assert delta == -5


def test_constrain_cluster_delta_zero(mock_autoscaler):
    delta = mock_autoscaler._constrain_cluster_delta(0)
    assert delta == 0


def test_constrain_cluster_delta_normal_scale_down_when_signal_delta_is_too_high(mock_autoscaler):
    delta = mock_autoscaler._constrain_cluster_delta(-4000)
    assert delta == -10


def test_constrain_cluster_delta_normal_scale_up_when_signal_delta_is_too_high(mock_autoscaler):
    delta = mock_autoscaler._constrain_cluster_delta(4000)
    assert delta == 200


def test_constrain_cluster_delta_restrict_scale_up_above_maximum(mock_autoscaler):
    mock_autoscaler.mesos_role_manager.target_capacity = 4900
    delta = mock_autoscaler._constrain_cluster_delta(150)
    assert delta == 100


def test_constrain_cluster_delta_restrict_scale_down_below_minimum(mock_autoscaler):
    mock_autoscaler.mesos_role_manager.target_capacity = 30
    delta = mock_autoscaler._constrain_cluster_delta(-40)
    assert delta == -6
