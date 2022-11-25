# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time
from datetime import timedelta
from itertools import chain
from itertools import repeat
from unittest.mock import ANY
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from clusterman.draining.queue import TerminationReason
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.interfaces.types import InstanceMetadata
from clusterman.migration.settings import MigrationPrecendence
from clusterman.migration.settings import PoolPortion
from clusterman.migration.settings import WorkerSetup
from clusterman.migration.worker import _drain_node_selection
from clusterman.migration.worker import _monitor_pool_health
from clusterman.migration.worker import event_migration_worker
from clusterman.migration.worker import RestartableDaemonProcess
from clusterman.migration.worker import uptime_migration_worker


@pytest.fixture
def event_worker_setup():
    yield WorkerSetup(
        rate=PoolPortion(2),
        prescaling=PoolPortion(1),
        precedence=MigrationPrecendence.TASK_COUNT,
        bootstrap_wait=1,
        bootstrap_timeout=2,
        disable_autoscaling=True,
        expected_duration=3,
        health_check_interval=4,
    )


@patch("clusterman.migration.worker.time")
def test_monitor_pool_health(mock_time):
    mock_manager = MagicMock()
    mock_connector = mock_manager.cluster_connector
    drained = [
        ClusterNodeMetadata(
            AgentMetadata(agent_id=i), InstanceMetadata(market=None, weight=None, ip_address=f"{i}.{i}.{i}.{i}")
        )
        for i in range(5)
    ]
    mock_manager.is_capacity_satisfied.side_effect = [False, True, True]
    mock_connector.has_enough_capacity_for_pods.side_effect = [False, False, True]
    mock_manager.is_node_still_in_pool.side_effect = chain(
        (True for i in range(3)),
        repeat(False),
    )
    mock_time.time.return_value = 0
    assert _monitor_pool_health(mock_manager, 1, drained, 120) == (True, [])
    # 1st iteration still draining some nodes
    # 2nd iteration underprovisioned capacity
    # 3rd iteration left over unscheduable pods
    assert mock_time.sleep.call_count == 3


@patch("clusterman.migration.worker.time")
@patch("clusterman.migration.worker._monitor_pool_health")
@patch("clusterman.migration.worker.get_monitoring_client")
def test_drain_node_selection(mock_sfx, mock_monitor, mock_time):
    mock_sfx = mock_sfx.return_value
    mock_drain_count_sfx = mock_sfx.create_counter.return_value
    mock_uptime_stats_sfx = mock_sfx.create_gauge.return_value
    mock_job_duration_sfx = mock_sfx.create_timer.return_value
    mock_manager = MagicMock()
    mock_monitor.return_value = (True, [])
    mock_manager.get_node_metadatas.return_value = [
        ClusterNodeMetadata(
            AgentMetadata(agent_id=i, task_count=30 - 2 * i),
            InstanceMetadata(None, None, uptime=timedelta(days=i)),
        )
        for i in range(6)
    ]
    mock_time.time.side_effect = range(5)
    worker_setup = WorkerSetup(
        rate=PoolPortion(2),
        prescaling=None,
        precedence=MigrationPrecendence.TASK_COUNT,
        bootstrap_wait=1,
        bootstrap_timeout=2,
        disable_autoscaling=False,
        expected_duration=3,
        health_check_interval=4,
    )
    assert _drain_node_selection(mock_manager, lambda n: n.agent.agent_id > 2, worker_setup) is True
    mock_manager.get_node_metadatas.assert_called_once_with(("running",))
    mock_manager.submit_for_draining.assert_has_calls(
        [
            call(
                ClusterNodeMetadata(
                    AgentMetadata(agent_id=i, task_count=30 - 2 * i),
                    InstanceMetadata(None, None, uptime=timedelta(days=i)),
                ),
                TerminationReason.NODE_MIGRATION,
            )
            for i in range(5, 2, -1)
        ]
    )
    mock_monitor.assert_has_calls(
        [
            call(
                manager=mock_manager,
                timeout=2,
                drained=[
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=5, task_count=20),
                        InstanceMetadata(None, None, uptime=timedelta(days=5)),
                    ),
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=4, task_count=22),
                        InstanceMetadata(None, None, uptime=timedelta(days=4)),
                    ),
                ],
                health_check_interval_seconds=4,
                ignore_pod_health=False,
                orphan_capacity_tollerance=0,
            ),
            call(
                manager=mock_manager,
                timeout=3,
                drained=[
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=3, task_count=24),
                        InstanceMetadata(None, None, uptime=timedelta(days=3)),
                    ),
                ],
                health_check_interval_seconds=4,
                ignore_pod_health=False,
                orphan_capacity_tollerance=0,
            ),
        ]
    )
    mock_job_duration_sfx.start.assert_called_once_with()
    mock_job_duration_sfx.stop.assert_called_once_with()
    assert mock_drain_count_sfx.count.call_count == 3
    mock_uptime_stats_sfx.set.assert_has_calls(
        [
            call(432000.0),
            call(345600.0),
            call(259200.0),
        ]
    )


@patch("clusterman.migration.worker.time")
@patch("clusterman.migration.worker._monitor_pool_health")
@patch("clusterman.migration.worker.get_monitoring_client")
def test_drain_node_selection_requeue(mock_sfx, mock_monitor, mock_time):
    mock_manager = MagicMock()
    mock_nodes = [
        ClusterNodeMetadata(
            AgentMetadata(agent_id=i, task_count=30 - 2 * i),
            InstanceMetadata(None, None, uptime=timedelta(days=i)),
        )
        for i in range(6)
    ]
    mock_monitor.side_effect = [(True, []) if i != 0 else (False, [mock_nodes[4]]) for i in range(5)]
    mock_manager.get_node_metadatas.return_value = mock_nodes
    mock_time.time.side_effect = range(5)
    worker_setup = WorkerSetup(
        rate=PoolPortion(2),
        prescaling=None,
        precedence=MigrationPrecendence.TASK_COUNT,
        bootstrap_wait=1,
        bootstrap_timeout=2,
        disable_autoscaling=False,
        expected_duration=3,
        health_check_interval=4,
        allowed_failed_drains=3,
    )
    assert _drain_node_selection(mock_manager, lambda n: n.agent.agent_id > 2, worker_setup) is True
    mock_manager.get_node_metadatas.assert_called_once_with(("running",))
    mock_manager.submit_for_draining.assert_has_calls(
        [
            call(
                ClusterNodeMetadata(
                    AgentMetadata(agent_id=i, task_count=30 - 2 * i),
                    InstanceMetadata(None, None, uptime=timedelta(days=i)),
                ),
                TerminationReason.NODE_MIGRATION,
            )
            for i in range(5, 2, -1)
        ]
        + [
            call(
                ClusterNodeMetadata(
                    AgentMetadata(agent_id=4, task_count=30 - 2 * 4),
                    InstanceMetadata(None, None, uptime=timedelta(days=4)),
                ),
                TerminationReason.NODE_MIGRATION,
            )
        ]
    )
    mock_monitor.assert_has_calls(
        [
            call(
                manager=mock_manager,
                timeout=2,
                drained=[
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=5, task_count=20),
                        InstanceMetadata(None, None, uptime=timedelta(days=5)),
                    ),
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=4, task_count=22),
                        InstanceMetadata(None, None, uptime=timedelta(days=4)),
                    ),
                ],
                health_check_interval_seconds=4,
                ignore_pod_health=False,
                orphan_capacity_tollerance=0,
            ),
            call(
                manager=mock_manager,
                timeout=3,
                drained=[
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=3, task_count=24),
                        InstanceMetadata(None, None, uptime=timedelta(days=3)),
                    ),
                    ClusterNodeMetadata(
                        AgentMetadata(agent_id=4, task_count=22),
                        InstanceMetadata(None, None, uptime=timedelta(days=4)),
                    ),
                ],
                health_check_interval_seconds=4,
                ignore_pod_health=False,
                orphan_capacity_tollerance=0,
            ),
        ]
    )


@patch("clusterman.migration.worker.time")
@patch("clusterman.migration.worker.PoolManager")
@patch("clusterman.migration.worker._drain_node_selection")
def test_uptime_migration_worker(mock_drain_selection, mock_manager_class, mock_time):
    mock_setup = MagicMock()
    mock_manager = mock_manager_class.return_value
    mock_manager.is_capacity_satisfied.side_effect = [True, False, True]
    with pytest.raises(StopIteration):  # using end of mock side-effect to get out of forever looop
        uptime_migration_worker("mesos-test", "bar", 10000, mock_setup, pool_lock=MagicMock())
    assert mock_drain_selection.call_count == 2
    selector = mock_drain_selection.call_args_list[0][0][1]
    assert selector(ClusterNodeMetadata(None, InstanceMetadata(None, None, uptime=timedelta(seconds=10001)))) is True
    assert selector(ClusterNodeMetadata(None, InstanceMetadata(None, None, uptime=timedelta(seconds=9999)))) is False


@patch("clusterman.migration.worker.time")
@patch("clusterman.migration.worker.PoolManager")
@patch("clusterman.migration.worker.enable_autoscaling")
@patch("clusterman.migration.worker.disable_autoscaling")
@patch("clusterman.migration.worker._drain_node_selection")
@patch("clusterman.migration.worker._monitor_pool_health", lambda *_, **__: (True, []))
@patch("clusterman.migration.worker.limit_function_runtime", lambda f, _: f())
def test_event_migration_worker(
    mock_drain_selection,
    mock_disable_scaling,
    mock_enable_scaling,
    mock_manager_class,
    mock_time,
    mock_migration_event,
    event_worker_setup,
):
    mock_time.time.return_value = 0
    mock_manager = mock_manager_class.return_value
    mock_manager.get_node_metadatas.return_value = [
        ClusterNodeMetadata(
            AgentMetadata(agent_id=i, task_count=30 - 2 * i, kernel=f"1.2.{i}"), InstanceMetadata(market=None, weight=4)
        )
        for i in range(1, 6)
    ]
    mock_manager.target_capacity = 19
    event_migration_worker(mock_migration_event, event_worker_setup, pool_lock=MagicMock())
    mock_manager.modify_target_capacity.assert_called_once_with(23)
    mock_disable_scaling.assert_called_once_with("mesos-test", "bar", "kubernetes", 3)
    mock_enable_scaling.assert_called_once_with("mesos-test", "bar", "kubernetes")
    mock_drain_selection.assert_called_once_with(mock_manager, ANY, event_worker_setup)
    selector = mock_drain_selection.call_args_list[0][0][1]
    assert list(filter(selector, mock_manager.get_node_metadatas.return_value)) == [
        ClusterNodeMetadata(
            AgentMetadata(agent_id=i, task_count=30 - 2 * i, kernel=f"1.2.{i}"), InstanceMetadata(market=None, weight=4)
        )
        for i in range(1, 3)
    ]


@patch("clusterman.migration.worker.time")
@patch("clusterman.migration.worker.PoolManager")
@patch("clusterman.migration.worker.enable_autoscaling")
@patch("clusterman.migration.worker.disable_autoscaling")
@patch("clusterman.migration.worker._drain_node_selection")
def test_event_migration_worker_error(
    mock_drain_selection,
    mock_disable_scaling,
    mock_enable_scaling,
    mock_manager_class,
    mock_time,
    mock_migration_event,
    event_worker_setup,
):
    mock_time.time.return_value = 0
    mock_manager = mock_manager_class.return_value
    mock_manager.get_node_metadatas.side_effect = Exception(123)
    with pytest.raises(Exception):
        event_migration_worker(mock_migration_event, event_worker_setup, pool_lock=MagicMock())
    mock_disable_scaling.assert_called_once_with("mesos-test", "bar", "kubernetes", 3)
    mock_enable_scaling.assert_called_once_with("mesos-test", "bar", "kubernetes")
    mock_manager.modify_target_capacity.assert_not_called()


def test_restartable_daemon_process():
    proc = RestartableDaemonProcess(lambda: time.sleep(10), tuple(), {})
    proc.start()
    time.sleep(0.05)
    assert proc.is_alive()
    old_handle = proc.process_handle
    proc.restart()
    time.sleep(0.05)
    assert proc.is_alive()
    assert proc.process_handle is not old_handle
    proc.kill()
