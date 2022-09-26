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
import enum
import operator
from typing import Any
from typing import Collection


class MigrationStatus(enum.Enum):
    PENDING = "pending"
    INPROGRESS = "inprogress"
    COMPLETED = "completed"
    SKIPPED = "skipped"


class ConditionTrait(enum.Enum):
    KERNEL = "kernel"
    LSBRELEASE = "lsbrelease"
    INSTANCE_TYPE = "instance_type"
    UPTIME = "uptime"


class ConditionOperator(enum.Enum):
    GT = "gt"
    GE = "ge"
    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOTIN = "notin"

    @classmethod
    def expecting_collection(cls) -> Collection["ConditionOperator"]:
        """Return operators expecting collection of object as right-operand"""
        return (cls.IN, cls.NOTIN)

    def apply(self, left: Any, right: Any) -> bool:
        """Apply operator

        :param Any left: left operand
        :param Any right: right operand
        :return: boolean result
        """
        if self == ConditionOperator.IN:
            return left in right
        elif self == ConditionOperator.NOTIN:
            return left not in right
        return getattr(operator, self.value)(left, right)


CONDITION_OPERATOR_SUPPORT_MATRIX = {
    ConditionTrait.KERNEL: set(ConditionOperator),
    ConditionTrait.LSBRELEASE: set(ConditionOperator),
    ConditionTrait.INSTANCE_TYPE: {
        ConditionOperator.EQ,
        ConditionOperator.NE,
        ConditionOperator.IN,
        ConditionOperator.NOTIN,
    },
    ConditionTrait.UPTIME: {ConditionOperator.GT, ConditionOperator.GE, ConditionOperator.LT, ConditionOperator.LE},
}
