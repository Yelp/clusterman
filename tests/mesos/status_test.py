from datetime import datetime

import mock

from clusterman.mesos.status import _get_mesos_status_string
from clusterman.mesos.status import MesosAgentState


@mock.patch('clusterman.mesos.status.allocated_cpu_resources')
class TestGetMesosStatusString:
    def test_orphaned(self, mock_allocated):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = {}
        mock_allocated.return_value = 100
        mesos_state, __ = _get_mesos_status_string(instance, agents)
        assert mesos_state == MesosAgentState.ORPHANED

    def test_idle(self, mock_allocated):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = {'1.2.3.4': 'foo'}
        mock_allocated.return_value = 0
        mesos_state, __ = _get_mesos_status_string(instance, agents)
        assert mesos_state == MesosAgentState.IDLE

    def test_running(self, mock_allocated):
        instance = {'PrivateIpAddress': '1.2.3.4', 'LaunchTime': datetime.now()}
        agents = {'1.2.3.4': 'foo'}
        mock_allocated.return_value = 100
        mesos_state, __ = _get_mesos_status_string(instance, agents)
        assert mesos_state == MesosAgentState.RUNNING

    def test_unknown(self, mock_allocated):
        instance = {}
        agents = {'1.2.3.4': 'foo'}
        mock_allocated.return_value = 100
        mesos_state, __ = _get_mesos_status_string(instance, agents)
        assert mesos_state == MesosAgentState.UNKNOWN
