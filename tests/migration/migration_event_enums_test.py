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
from typing import Any

import pytest

from clusterman.migration.event_enums import ConditionOperator


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
