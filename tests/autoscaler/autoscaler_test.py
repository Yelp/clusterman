import mock
import pytest

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.signals.base_signal import SignalResult


@pytest.fixture
def mock_read_signals(mock_autoscaler_config_dict):
    with mock.patch('clusterman.autoscaler.autoscaler._read_signals') as mock_read_signals:
        sig1 = mock.MagicMock(priority=mock_autoscaler_config_dict['autoscale_signals'][0]['priority'])
        sig1.name = mock_autoscaler_config_dict['autoscale_signals'][0]['name']
        sig1.return_value = SignalResult()

        sig2 = mock.MagicMock(priority=0)
        sig2.name = mock_autoscaler_config_dict['autoscale_signals'][1]['name']
        sig2.return_value = SignalResult()

        sig3 = mock.MagicMock(priority=3)
        sig3.name = 'UnusedSignal'
        sig3.return_value = SignalResult()

        sig4 = mock.MagicMock(priority=mock_autoscaler_config_dict['autoscale_signals'][2]['priority'])
        sig4.name = mock_autoscaler_config_dict['autoscale_signals'][2]['name']
        sig4.return_value = SignalResult()

        mock_read_signals.return_value = {
            sig1.name: mock.Mock(return_value=sig1),
            sig2.name: mock.Mock(return_value=sig2),
            sig3.name: mock.Mock(return_value=sig3),
            sig4.name: mock.Mock(return_value=sig4),
            'MissingParamSignal': mock.Mock(side_effect=KeyError)
        }
        yield mock_read_signals


@pytest.fixture
@mock.patch('clusterman.autoscaler.autoscaler.MesosRoleManager', autospec=True)
def mock_autoscaler(mock_role_manager, mock_read_signals, mock_autoscaler_config):
    with mock.patch('clusterman.autoscaler.autoscaler.logger'):
        mock_role_manager.return_value.target_capacity = 300
        a = Autoscaler('foo', 'bar')
        return a


def test_autoscaler_init(mock_autoscaler):
    assert mock_autoscaler.cluster == 'foo'
    assert mock_autoscaler.role == 'bar'
    assert {signal.name for signal in mock_autoscaler.signals[0]} == {'FakeSignalTwo'}
    assert {signal.name for signal in mock_autoscaler.signals[1]} == {'FakeSignalOne', 'FakeSignalThree'}


@pytest.mark.parametrize('dry_run', [True, False])
def test_autoscaler_dry_run(dry_run, mock_autoscaler):
    mock_autoscaler._compute_cluster_delta = mock.Mock(return_value=100)
    mock_autoscaler.run(dry_run=dry_run)
    assert mock_autoscaler.mesos_role_manager.modify_target_capacity.call_count == int(not dry_run)


def test_compute_cluster_delta_active(mock_read_signals, mock_autoscaler):
    mock_read_signals.return_value['FakeSignalTwo'].return_value.return_value = SignalResult(True, 20)
    mock_autoscaler._constrain_cluster_delta = mock.Mock(side_effect=lambda x: x)

    delta = mock_autoscaler._compute_cluster_delta()
    assert delta == 20
    assert mock_read_signals.return_value['FakeSignalOne'].return_value.call_count == 0
    assert mock_read_signals.return_value['FakeSignalTwo'].return_value.call_count == 1
    assert mock_read_signals.return_value['FakeSignalThree'].return_value.call_count == 0


def test_signals_not_active(mock_read_signals, mock_autoscaler):
    mock_autoscaler._constrain_cluster_delta = mock.Mock(side_effect=lambda x: x)

    delta = mock_autoscaler._compute_cluster_delta()
    assert delta == 0
    assert mock_read_signals.return_value['FakeSignalOne'].return_value.call_count == 1
    assert mock_read_signals.return_value['FakeSignalTwo'].return_value.call_count == 1
    assert mock_read_signals.return_value['FakeSignalThree'].return_value.call_count == 1


def test_signals_error(mock_read_signals, mock_autoscaler):
    mock_autoscaler._constrain_cluster_delta = mock.Mock(side_effect=lambda x: x)
    mock_read_signals.return_value['FakeSignalTwo'].return_value.side_effect = Exception('something bad happened')
    mock_read_signals.return_value['FakeSignalOne'].return_value.return_value = SignalResult(True, 30)

    with mock.patch('clusterman.autoscaler.autoscaler.logger') as logger:
        delta = mock_autoscaler._compute_cluster_delta()
        assert logger.error.call_count == 1
    assert delta == 30
    assert mock_read_signals.return_value['FakeSignalOne'].return_value.call_count == 1
    assert mock_read_signals.return_value['FakeSignalTwo'].return_value.call_count == 1
    assert mock_read_signals.return_value['FakeSignalThree'].return_value.call_count == 0


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
