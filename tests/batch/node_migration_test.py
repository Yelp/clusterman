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
import json
from argparse import Namespace
from unittest.mock import call
from unittest.mock import patch

import pytest

from clusterman.batch.node_migration import NodeMigration
from clusterman.migration.event import ConditionTrait
from clusterman.migration.event import MigrationCondition
from clusterman.migration.event import MigrationEvent
from clusterman.migration.settings import MigrationPrecendence
from clusterman.migration.settings import PoolPortion
from clusterman.migration.settings import WorkerSetup


@pytest.fixture(scope="function")
def migration_batch():
    with patch("clusterman.batch.node_migration.sqs"), patch("clusterman.batch.node_migration.setup_config"), patch(
        "clusterman.batch.node_migration.get_pool_name_list"
    ) as mock_getpool, patch("clusterman.batch.node_migration.load_cluster_pool_config"):
        batch = NodeMigration()
        mock_getpool.return_value = ["bar"]
        batch.options = Namespace(cluster="mesos-test", autorestart_interval_minutes=None)
        batch.configure_initial()
        assert "bar" in batch.migration_configs
        yield batch


def test_fetch_event_queue(migration_batch: NodeMigration):
    migration_batch.events_in_progress = {
        MigrationEvent(
            event_id="1",
            event_receipt="1",
            cluster="mesos-test",
            pool="bar",
            condition=MigrationCondition(ConditionTrait.KERNEL, "3.2.1"),
        )
    }
    migration_batch.EVENT_FETCH_BATCH = 3
    migration_batch.sqs_client.receive_message.side_effect = [
        {
            "Messages": [
                {
                    "MessageId": str(i),
                    "ReceiptHandle": str(i),
                    "Body": json.dumps({"cluster": "mesos-test", "pool": "bar", "condition": {"kernel": f"3.2.{i}"}}),
                }
                for i in range(3)
            ]
        },
        {
            "Messages": [
                {
                    "MessageId": str(i),
                    "ReceiptHandle": str(i),
                    "Body": json.dumps(
                        {"cluster": "mesos-test", "pool": "bar", "condition": {"lsbrelease": f"26.0{i}"}}
                    ),
                }
                for i in range(2)
            ]
        },
    ]
    assert migration_batch.fetch_event_queue() == {
        MigrationEvent(
            event_id="0",
            event_receipt="0",
            cluster="mesos-test",
            pool="bar",
            condition=MigrationCondition(ConditionTrait.KERNEL, "3.2.0"),
        ),
        MigrationEvent(
            event_id="2",
            event_receipt="2",
            cluster="mesos-test",
            pool="bar",
            condition=MigrationCondition(ConditionTrait.KERNEL, "3.2.2"),
        ),
        MigrationEvent(
            event_id="0",
            event_receipt="0",
            cluster="mesos-test",
            pool="bar",
            condition=MigrationCondition(ConditionTrait.LSBRELEASE, "26.00"),
        ),
        MigrationEvent(
            event_id="1",
            event_receipt="1",
            cluster="mesos-test",
            pool="bar",
            condition=MigrationCondition(ConditionTrait.LSBRELEASE, "26.01"),
        ),
    }
    migration_batch.sqs_client.receive_message.assert_has_calls(
        [
            call(
                MaxNumberOfMessages=3,
                QueueUrl="mesos-test-migration-event.com",
                VisibilityTimeout=900,
                WaitTimeSeconds=10,
            ),
            call(
                MaxNumberOfMessages=3,
                QueueUrl="mesos-test-migration-event.com",
                VisibilityTimeout=900,
                WaitTimeSeconds=10,
            ),
        ]
    )


@patch("clusterman.batch.node_migration.time")
def test_run(mock_time, migration_batch):
    mock_time.sleep.side_effect = StopIteration  # hacky way to stop main batch loop
    mock_event = MigrationEvent(
        event_id="0",
        event_receipt="0",
        cluster="mesos-test",
        pool="bar",
        condition=MigrationCondition(ConditionTrait.KERNEL, "3.2.0"),
    )
    with patch.object(migration_batch, "spawn_uptime_worker") as mock_uptime_spawn, patch.object(
        migration_batch, "spawn_event_worker"
    ) as mock_event_spawn, patch.object(migration_batch, "fetch_event_queue") as mock_fetch_event:
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
