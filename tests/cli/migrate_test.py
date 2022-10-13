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
import argparse
from unittest.mock import patch

import packaging.version

from clusterman.cli.migrate import main
from clusterman.migration.event import MigrationCondition
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import ConditionOperator
from clusterman.migration.event_enums import ConditionTrait
from clusterman.migration.event_enums import MigrationStatus


@patch("clusterman.cli.migrate.time")
@patch("clusterman.cli.migrate.KubernetesClusterConnector")
def test_migrate_command(mock_connector, mock_time):
    mock_args = argparse.Namespace(
        cluster="mesos-test",
        pool="bar",
        label_selector=[],
        condition_trait="lsbrelease",
        condition_operator="ge",
        condition_target="22.04",
    )
    mock_time.time.return_value = 111222333
    main(mock_args)
    mock_connector.assert_called_once_with("mesos-test", "bar", init_crd=True)
    mock_connector.return_value.create_node_migration_resource.assert_called_once_with(
        MigrationEvent(
            resource_name="mesos-test-bar-111222333",
            cluster="mesos-test",
            pool="bar",
            label_selectors=[],
            condition=MigrationCondition(
                ConditionTrait.LSBRELEASE, ConditionOperator.GE, packaging.version.parse("22.04")
            ),
        ),
        MigrationStatus.PENDING,
    )
