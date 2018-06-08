import arrow
import mock
import pytest
import staticconf

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import NoSignalConfiguredException


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


@pytest.fixture
def mock_autoscaler():
    autoscaling_config_dict = {
        'default_signal_role': 'clusterman',
        'setpoint': 0.7,
        'setpoint_margin': 0.1,
        'cpus_per_weight': 8,
    }

    with mock.patch('clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient', autospec=True), \
            mock.patch('clusterman.autoscaler.autoscaler.MesosPoolManager', autospec=True), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._get_signal_for_app', autospec=True), \
            mock.patch('clusterman.autoscaler.autoscaler.yelp_meteorite'), \
            mock.patch('clusterman.autoscaler.autoscaler.Signal'), \
            staticconf.testing.PatchConfiguration({'autoscaling': autoscaling_config_dict}):
        mock_autoscaler = Autoscaler('mesos-test', 'bar', ['bar'])
    mock_autoscaler.mesos_pool_manager.target_capacity = 300
    mock_autoscaler.mesos_pool_manager.min_capacity = staticconf.read_int(
        'scaling_limits.min_capacity', namespace=POOL_NAMESPACE.format(pool='bar')
    )
    mock_autoscaler.mesos_pool_manager.max_capacity = staticconf.read_int(
        'scaling_limits.max_capacity', namespace=POOL_NAMESPACE.format(pool='bar')
    )
    return mock_autoscaler


def test_autoscaler_init_too_many_apps():
    with pytest.raises(NotImplementedError):
        Autoscaler('mesos-test', 'bar', ['app1', 'app2'])


@pytest.mark.parametrize('signal_response', [
    NoSignalConfiguredException,  # no app signal
    ValueError,  # app signal failed to load
    mock.Mock()  # Custom app signal successful
])
def test_get_signal_for_app(mock_autoscaler, signal_response):
    with mock.patch('clusterman.autoscaler.autoscaler.Signal') as mock_signal, \
            mock.patch('clusterman.autoscaler.autoscaler.sensu_checkin') as mock_sensu:
        mock_signal.side_effect = signal_response
        signal = mock_autoscaler._get_signal_for_app('bar')
        assert mock_sensu.call_count == (signal_response == ValueError)

    assert signal == (mock_autoscaler.default_signal if isinstance(signal_response, Exception) else signal)


@pytest.mark.parametrize('dry_run', [True, False])
def test_autoscaler_run(dry_run, mock_autoscaler, run_timestamp):
    mock_autoscaler._compute_target_capacity = mock.Mock(return_value=100)
    mock_autoscaler.signal.evaluate.side_effect = ValueError
    mock_autoscaler.default_signal.evaluate.return_value = {'cpus': 100000}
    with pytest.raises(ValueError):
        mock_autoscaler.run(dry_run=dry_run, timestamp=run_timestamp)
    assert mock_autoscaler.capacity_gauge.set.call_args == mock.call(100, {'dry_run': dry_run})
    assert mock_autoscaler._compute_target_capacity.call_args == mock.call({'cpus': 100000}, run_timestamp)
    assert mock_autoscaler.mesos_pool_manager.modify_target_capacity.call_count == 1


@pytest.mark.parametrize('signal_cpus,total_cpus,expected_capacity', [
    (None, 1000, 125),
    (799, 1000, 125),  # above setpoint, but within setpoint margin
    (980, 1000, 175),  # above setpoint margin
    (601, 1000, 125),  # below setpoint, but within setpoint margin
    (490, 1000, 87.5),  # below setpoint margin
    (1400, 1000, 250),  # above setpoint margin and total
])
def test_compute_target_capacity(mock_autoscaler, signal_cpus, total_cpus,
                                 expected_capacity, run_timestamp):
    mock_autoscaler.mesos_pool_manager.target_capacity = \
        total_cpus / mock_autoscaler.autoscaling_config.cpus_per_weight
    new_target_capacity = mock_autoscaler._compute_target_capacity({'cpus': signal_cpus}, run_timestamp)
    assert new_target_capacity == pytest.approx(expected_capacity)
