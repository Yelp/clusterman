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
        batch.options = Namespace(cluster="mesos-test", autorestart_interval_minutes=None)
        batch.configure_initial()
        assert "bar" in batch.migration_configs
        yield batch


def test_fetch_event_crd(migration_batch: NodeMigration):
    migration_batch.events_in_progress = {
        MigrationEvent(
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
        expected_duration=86400.0,
    )
