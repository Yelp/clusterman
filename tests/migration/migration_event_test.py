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

import arrow
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
from clusterman.migration.event import MigrationEvent


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


def test_event_to_string():
    event = MigrationEvent(
        resource_name="mesos-test-bar-111222333",
        cluster="mesos-test",
        pool="bar",
        label_selectors=[],
        condition=MigrationCondition(
            ConditionTrait.KERNEL,
            ConditionOperator.IN,
            [semver.VersionInfo.parse("1.2.3"), semver.VersionInfo.parse("3.4.5")],
        ),
        previous_attempts=3,
        created=arrow.get("2023-02-10T11:18:17Z"),
    )
    assert str(event) == (
        "MigrationEvent(cluster=mesos-test, pool=bar,"
        " label_selectors=[], condition=(KERNEL in 1.2.3,3.4.5),"
        " attempts=3, created=2023-02-10T11:18:17+00:00)"
    )


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


@pytest.mark.parametrize(
    "condition,current_time,result",
    (
        # instance younger than 1337s, 10d instance doesn't match
        (MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, 1337), None, False),
        # instance younger than 15d, checked with no delay, 10d instance matches
        (
            MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, timedelta(days=15).total_seconds()),
            arrow.now(),
            True,
        ),
        # instance younger than 5d, but checked with 7d delay, 10d instance matches as at check creation is what 3d old
        (
            MigrationCondition(ConditionTrait.UPTIME, ConditionOperator.LT, timedelta(days=5).total_seconds()),
            arrow.now() - timedelta(days=7),
            True,
        ),
    ),
)
def test_condition_matches_uptime_offset(condition, current_time, result):
    node_metadata = ClusterNodeMetadata(
        agent=AgentMetadata(kernel="3.2.1", lsbrelease="20.04"),
        instance=InstanceMetadata(market=InstanceMarket("m5.4xlarge", None), weight=None, uptime=timedelta(days=10)),
    )
    assert condition.matches(node_metadata, current_time) is result


def test_migration_event_equality():
    event1 = MigrationEvent(
        resource_name="mesos-test-bar-111222333",
        cluster="mesos-test",
        pool="bar",
        label_selectors=[],
        condition=MigrationCondition(
            ConditionTrait.KERNEL,
            ConditionOperator.IN,
            [semver.VersionInfo.parse("1.2.3"), semver.VersionInfo.parse("3.4.5")],
        ),
        previous_attempts=0,
        created=arrow.get("2023-02-10T11:18:17Z"),
    )
    event2 = MigrationEvent(**{**event1._asdict(), "previous_attempts": 1})
    assert event2 in {event1}
    assert event2 == event1
