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
import pytest
import semver

from clusterman.migration.event import ConditionOperator
from clusterman.migration.event import ConditionTrait
from clusterman.migration.event import MigrationCondition
from clusterman.migration.event import MigrationEvent


@pytest.fixture
def mock_migration_event():
    yield MigrationEvent(
        resource_name="mesos-test-bar-111222333",
        cluster="mesos-test",
        pool="bar",
        label_selectors=[],
        condition=MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, semver.VersionInfo.parse("1.2.3")),
    )
