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
from argparse import Namespace
from collections import defaultdict
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from clusterman.batch.node_migration import MigrationStatus
from clusterman.batch.node_migration import NodeMigration
from clusterman.migration.event import MigrationCondition
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import ConditionOperator
from clusterman.migration.event_enums import ConditionTrait
from clusterman.migration.settings import MigrationPrecendence
from clusterman.migration.settings import PoolPortion
from clusterman.migration.settings import WorkerSetup


@pytest.fixture(scope="function")
def migration_batch():
    with patch("clusterman.batch.node_migration.KubernetesClusterConnector"), patch(
        "clusterman.batch.node_migration.setup_config"
    ), patch("clusterman.batch.node_migration.get_pool_name_list") as mock_getpool, patch(
        "clusterman.batch.node_migration.load_cluster_pool_config"
    ):
        batch = NodeMigration()
        mock_getpool.return_value = ["bar"]
        batch.options = Namespace(cluster="mesos-test", autorestart_interval_minutes=None, extra_logs=False)
        batch.configure_initial()
        assert "bar" in batch.migration_configs
        yield batch


def test_fetch_event_crd(migration_batch: NodeMigration):
    migration_batch.events_in_progress = {
        "event:mesos-test:bar": MigrationEvent(
            resource_name="mesos-test-bar-220912-1",
            cluster="mesos-test",
            pool="bar",
            label_selectors=[],
            condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, "3.2.1"),
        )
    }
    migration_batch.cluster_connector.list_node_migration_resources.return_value = [
        MigrationEvent(
            resource_name=f"mesos-test-bar-220912-{i}",
            cluster="mesos-test",
            pool="bar",
            label_selectors=[],
            condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, f"3.2.{i}"),
        )
        for i in range(3)
    ]
    assert migration_batch.fetch_event_crd() == {
        MigrationEvent(
            resource_name=f"mesos-test-bar-220912-{i}",
            cluster="mesos-test",
            pool="bar",
            label_selectors=[],
            condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, f"3.2.{i}"),
        )
        for i in range(3)
        if i != 1
    }
    migration_batch.cluster_connector.list_node_migration_resources.assert_called_once_with(
        [MigrationStatus.PENDING, MigrationStatus.INPROGRESS],
    )


@patch("clusterman.batch.node_migration.time")
def test_run(mock_time, migration_batch):
    mock_time.sleep.side_effect = StopIteration  # hacky way to stop main batch loop
    mock_event = MigrationEvent(
        resource_name="mesos-test-bar-220912-1",
        cluster="mesos-test",
        pool="bar",
        label_selectors=[],
        condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, "3.2.0"),
    )
    with patch.object(migration_batch, "spawn_uptime_worker") as mock_uptime_spawn, patch.object(
        migration_batch, "spawn_event_worker"
    ) as mock_event_spawn, patch.object(migration_batch, "fetch_event_crd") as mock_fetch_event:
        mock_fetch_event.return_value = {mock_event}
        with pytest.raises(StopIteration):
            migration_batch.run()
        mock_event_spawn.assert_called_once_with(mock_event)
        mock_uptime_spawn.assert_called_once_with("bar", "90d")
        mock_fetch_event.assert_called_once_with()


def test_get_worker_setup(migration_batch):
    assert migration_batch._get_worker_setup("does-not-exists") is None
    assert migration_batch._get_worker_setup("bar") == WorkerSetup(
        rate=PoolPortion("3%"),
        prescaling=PoolPortion(1),
        precedence=MigrationPrecendence.UPTIME,
        bootstrap_wait=180.0,
        bootstrap_timeout=180.0,
        disable_autoscaling=False,
        expected_duration=7200.0,
        health_check_interval=120,
    )


@patch("clusterman.batch.node_migration.RestartableDaemonProcess")
def test_spawn_worker(mock_process, migration_batch):
    mock_lock = MagicMock()
    mock_routine = MagicMock()
    worker_label = "foobar:123:456"
    migration_batch.worker_locks = defaultdict(mock_lock)
    assert migration_batch._spawn_worker(worker_label, mock_routine, 1, x=2) is True
    mock_process.assert_called_once_with(
        target=mock_routine, args=(1,), kwargs={"x": 2, "pool_lock": mock_lock.return_value}
    )
    assert migration_batch.migration_workers == {worker_label: mock_process.return_value}


@patch("clusterman.batch.node_migration.RestartableDaemonProcess")
def test_spawn_worker_existing(mock_process, migration_batch):
    migration_batch.migration_workers["foobar"] = MagicMock(is_alive=lambda: True)
    assert migration_batch._spawn_worker("foobar", MagicMock(), 1, x=2) is False
    mock_process.assert_not_called()


@patch("clusterman.batch.node_migration.RestartableDaemonProcess")
def test_spawn_worker_over_capacity(mock_process, migration_batch):
    migration_batch.migration_workers = {f"foobar{i}": MagicMock(is_alive=lambda: True) for i in range(6)}
    assert migration_batch._spawn_worker("event:foo:bar", MagicMock(), 1, x=2) is False
    mock_process.assert_not_called()


@pytest.mark.parametrize(
    "event,worker_setup,is_spawned",
    (
        (
            MigrationEvent(
                resource_name="mesos-test-bar-220912-1",
                cluster="mesos-test",
                pool="bar",
                label_selectors=[],
                condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, "3.2.1"),
            ),
            False,
            False,
        ),
        (
            MigrationEvent(
                resource_name="mesos-test-buzz-220912-1",
                cluster="mesos-test",
                pool="buzz",
                label_selectors=[],
                condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, "3.2.1"),
            ),
            True,
            False,
        ),
        (
            MigrationEvent(
                resource_name="mesos-test-bar-220912-1",
                cluster="mesos-test",
                pool="bar",
                label_selectors=[],
                condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, "3.2.1"),
            ),
            True,
            True,
        ),
    ),
)
@patch("clusterman.batch.node_migration.event_migration_worker")
def test_spawn_event_worker(mock_worker_routine, migration_batch, event, worker_setup, is_spawned):
    with patch.object(migration_batch, "_get_worker_setup") as mock_get_setup, patch.object(
        migration_batch, "_spawn_worker"
    ) as mock_spawn, patch.object(migration_batch, "mark_event") as mock_mark:
        mock_get_setup.return_value = worker_setup
        migration_batch.spawn_event_worker(event)
        if is_spawned:
            mock_spawn.assert_called_once_with(
                label=f"event:{event.cluster}:{event.pool}",
                routine=mock_worker_routine,
                migration_event=event,
                worker_setup=worker_setup,
            )
        else:
            mock_spawn.assert_not_called()
            mock_mark.assert_called_once_with(event, MigrationStatus.SKIPPED)


@pytest.mark.parametrize(
    "uptime,worker_setup,expected_uptime_spawn",
    (
        (1, True, None),
        ("2d", False, None),
        ("2d", True, 2 * 24 * 60 * 60),
        (100000, True, 100000),
    ),
)
@patch("clusterman.batch.node_migration.uptime_migration_worker")
def test_spawn_uptime_worker(mock_worker_routine, migration_batch, uptime, worker_setup, expected_uptime_spawn):
    with patch.object(migration_batch, "_get_worker_setup") as mock_get_setup, patch.object(
        migration_batch, "_spawn_worker"
    ) as mock_spawn:
        mock_get_setup.return_value = worker_setup
        migration_batch.spawn_uptime_worker("bar", uptime)
        if expected_uptime_spawn:
            mock_spawn.assert_called_once_with(
                label="uptime:mesos-test:bar",
                routine=mock_worker_routine,
                cluster="mesos-test",
                pool="bar",
                uptime_seconds=expected_uptime_spawn,
                worker_setup=worker_setup,
            )
        else:
            mock_spawn.assert_not_called()


def test_monitor_workers(migration_batch):
    mock_event = MigrationEvent(None, None, None, None, None)
    mock_to_restart = MagicMock(is_alive=lambda: False, exitcode=1)
    mock_ok_worker = MagicMock(is_alive=lambda: True)
    migration_batch.migration_workers = {
        "foobar": mock_ok_worker,
        "buzz": mock_to_restart,
        "some": MagicMock(is_alive=lambda: False, exitcode=0),
        "event:123:456": MagicMock(is_alive=lambda: False, exitcode=0),
    }
    migration_batch.events_in_progress = {"event:123:456": mock_event}
    with patch.object(migration_batch, "mark_event") as mock_mark:
        migration_batch.monitor_workers()
        mock_mark.assert_called_once_with(mock_event, MigrationStatus.COMPLETED)
    mock_to_restart.restart.assert_called_once_with()
    assert migration_batch.migration_workers == {"foobar": mock_ok_worker, "buzz": mock_to_restart}
    assert not migration_batch.events_in_progress
