import arrow
import mock
import pysensu_yelp
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
    }

    with mock.patch('clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient', autospec=True), \
            mock.patch('clusterman.autoscaler.autoscaler.MesosPoolManager', autospec=True), \
            mock.patch('clusterman.autoscaler.autoscaler.Autoscaler._get_signal_for_app', autospec=True), \
            mock.patch('clusterman.autoscaler.autoscaler.yelp_meteorite'), \
            mock.patch('clusterman.autoscaler.autoscaler.Signal'), \
            staticconf.testing.PatchConfiguration({'autoscaling': autoscaling_config_dict}):
        mock_autoscaler = Autoscaler('mesos-test', 'bar', ['bar'], monitoring_enabled=False)
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
        Autoscaler('mesos-test', 'bar', ['app1', 'app2'], monitoring_enabled=False)


@mock.patch(
    'clusterman.autoscaler.signals.Signal',
    mock.Mock(side_effect=Exception),
    autospec=None
)
@pytest.mark.parametrize('monitoring_enabled', [True, False])
def test_monitoring_enabled(mock_autoscaler, monitoring_enabled):
    mock_autoscaler.monitoring_enabled = monitoring_enabled

    with mock.patch('pysensu_yelp.send_event'):
        mock_autoscaler._get_signal_for_app('bar')

        assert pysensu_yelp.send_event.call_count == (1 if monitoring_enabled else 0)


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
    assert mock_autoscaler._compute_target_capacity.call_args == mock.call({'cpus': 100000})
    assert mock_autoscaler.mesos_pool_manager.modify_target_capacity.call_count == 1


class TestComputeTargetCapacity:

    @pytest.mark.parametrize('resource', ['cpus', 'mem', 'disk'])
    @pytest.mark.parametrize('signal_resource,total_resource,expected_capacity', [
        (None, 1000, 125),
        (767, 1000, 125),  # above setpoint, but within setpoint margin
        (980, 1000, 175),  # above setpoint margin
        (633, 1000, 125),  # below setpoint, but within setpoint margin
        (490, 1000, 87.5),  # below setpoint margin
        (1400, 1000, 250),  # above setpoint margin and total
    ])
    def test_single_resource(self, mock_autoscaler, resource, signal_resource, total_resource, expected_capacity):
        mock_autoscaler.mesos_pool_manager.target_capacity = 125
        mock_autoscaler.mesos_pool_manager.non_orphan_fulfilled_capacity = 125
        mock_autoscaler.mesos_pool_manager.get_resource_total.return_value = total_resource
        new_target_capacity = mock_autoscaler._compute_target_capacity({resource: signal_resource})
        assert new_target_capacity == pytest.approx(expected_capacity)

    def test_empty_request(self, mock_autoscaler):
        new_target_capacity = mock_autoscaler._compute_target_capacity({})
        assert new_target_capacity == mock_autoscaler.mesos_pool_manager.target_capacity

    def test_scale_most_constrained_resource(self, mock_autoscaler):
        resource_request = {'cpus': 500, 'mem': 30000, 'disk': 19000}
        resource_totals = {'cpus': 1000, 'mem': 50000, 'disk': 20000}
        mock_autoscaler.mesos_pool_manager.non_orphan_fulfilled_capacity = 100
        mock_autoscaler.mesos_pool_manager.get_resource_total.side_effect = resource_totals.__getitem__
        new_target_capacity = mock_autoscaler._compute_target_capacity(resource_request)

        # disk would be the most constrained resource, so we should scale the target_capacity (100) by an amount
        # such that requested/(total*scale_factor) = setpoint
        expected_new_target_capacity = 100 * 19000 / (20000 * 0.7)
        assert new_target_capacity == expected_new_target_capacity
