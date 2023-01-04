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
from typing import Any

import packaging.version
import pytest
import semver

from clusterman.aws.markets import InstanceMarket
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.interfaces.types import InstanceMetadata
from clusterman.migration.event_enums import ConditionOperator
from clusterman.migration.event_enums import ConditionTrait


@pytest.mark.parametrize(
    "op,left,right,result",
    (
        (ConditionOperator.LT, 1, 2, True),
        (ConditionOperator.LT, 3, 2, False),
        (ConditionOperator.EQ, 2, 2, True),
        (ConditionOperator.NE, 1, 2, True),
        (ConditionOperator.IN, 1, (1, 2, 3), True),
        (ConditionOperator.IN, 1, (2, 3), False),
        (ConditionOperator.NOTIN, 1, (3, 4, 5), True),
    ),
)
def test_operator_apply(op: ConditionOperator, left: Any, right: Any, result: bool):
    assert op.apply(left, right) is result


@pytest.mark.parametrize(
    "enum_val,expected",
    (
        (ConditionTrait.INSTANCE_TYPE, "m6a.4xlarge"),
        (ConditionTrait.UPTIME, 10 * 24 * 60 * 60),
        (ConditionTrait.KERNEL, semver.VersionInfo.parse("3.2.1")),
        (ConditionTrait.LSBRELEASE, packaging.version.parse("20.04")),
    ),
)
def test_trait_get_from(enum_val, expected):
    node_metadata = ClusterNodeMetadata(
        agent=AgentMetadata(kernel="3.2.1", lsbrelease="20.04"),
        instance=InstanceMetadata(market=InstanceMarket("m6a.4xlarge", None), weight=None, uptime=timedelta(days=10)),
    )
    assert enum_val.get_from(node_metadata) == expected
