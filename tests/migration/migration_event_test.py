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
from datetime import timedelta
from typing import Type

import packaging.version
import pytest
import semver

from clusterman.aws.markets import InstanceMarket
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.interfaces.types import InstanceMetadata
from clusterman.migration.event import ConditionOperator
from clusterman.migration.event import ConditionTrait
from clusterman.migration.event import MigrationCondition


@pytest.mark.parametrize(
    "trait,operator,target,expected",
    (
        (
            "kernel",
            "ge",
            "1.2.3-4567-aws",
            MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, semver.VersionInfo.parse("1.2.3-4567-aws")),
        ),
        (
            "lsbrelease",
            "ge",
            "22.04",
            MigrationCondition(ConditionTrait.LSBRELEASE, ConditionOperator.GE, packaging.version.parse("22.04")),
        ),
        (
            "instance_type",
            "in",
            "m5.4xlarge,r5.2xLARGE",
            MigrationCondition(ConditionTrait.INSTANCE_TYPE, ConditionOperator.IN, ["m5.4xlarge", "r5.2xlarge"]),
        ),
        ("uptime", "lt", "30d", MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, 30 * 24 * 60 * 60)),
        ("uptime", "le", "1337", MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LE, 1337)),
    ),
)
def test_condition_from_dict(trait: str, operator: str, target: str, expected: MigrationCondition):
    assert MigrationCondition.from_dict({"trait": trait, "operator": operator, "target": target}) == expected


@pytest.mark.parametrize(
    "trait,operator,target,error",
    (
        ("kernel", "ne", "adjksfghlasdjk", ValueError),
        ("lsbrelease", "ne", "adjksfghlasdjk", ValueError),
        ("instance_type", "in", "m5.4xlarge,foobar.1xsmall", ValueError),
        ("uptime", "ge", "foobar", ValueError),
        ("instance_type", "ge", "m5.4xlarge", ValueError),
        ("uptime", "in", "1337", ValueError),
    ),
)
def test_condition_from_dict_error(trait: str, operator: str, target: str, error: Type[Exception]):
    with pytest.raises(error):
        MigrationCondition.from_dict({"trait": trait, "operator": operator, "target": target})


@pytest.mark.parametrize(
    "condition,expected",
    (
        (
            MigrationCondition(ConditionTrait.LSBRELEASE, ConditionOperator.GE, packaging.version.parse("1.2")),
            {"trait": "lsbrelease", "operator": "ge", "target": "1.2"},
        ),
        (
            MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, semver.VersionInfo.parse("1.2.3")),
            {"trait": "kernel", "operator": "ge", "target": "1.2.3"},
        ),
        (
            MigrationCondition(ConditionTrait.INSTANCE_TYPE, ConditionOperator.IN, ["m5.4xlarge", "r5.2xlarge"]),
            {"trait": "instance_type", "operator": "in", "target": "m5.4xlarge,r5.2xlarge"},
        ),
        (
            MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, 1337),
            {"trait": "uptime", "operator": "lt", "target": "1337"},
        ),
    ),
)
def test_condition_to_dict(condition, expected):
    assert condition.to_dict() == expected


def test_event_to_crd_body(mock_migration_event):
    assert mock_migration_event.to_crd_body({"foo": "bar"}) == {
        "metadata": {"labels": {"foo": "bar"}, "name": "mesos-test-bar-111222333"},
        "spec": {
            "cluster": "mesos-test",
            "condition": {"operator": "ge", "target": "1.2.3", "trait": "kernel"},
            "label_selectors": [],
            "pool": "bar",
        },
    }


@pytest.mark.parametrize(
    "condition,result",
    (
        (MigrationCondition(ConditionTrait.KERNEL, ConditionOperator.GE, semver.VersionInfo.parse("1.2.3")), True),
        (MigrationCondition(ConditionTrait.LSBRELEASE, ConditionOperator.GE, packaging.version.parse("22.04")), False),
        (MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, 1337), False),
        (MigrationCondition(ConditionTrait.INSTANCE_TYPE, ConditionOperator.IN, ["m5.4xlarge", "r5.2xlarge"]), True),
    ),
)
def test_condition_matches(condition, result):
    node_metadata = ClusterNodeMetadata(
        agent=AgentMetadata(kernel="3.2.1", lsbrelease="20.04"),
        instance=InstanceMetadata(market=InstanceMarket("m5.4xlarge", None), weight=None, uptime=timedelta(days=10)),
    )
    assert condition.matches(node_metadata) is result
