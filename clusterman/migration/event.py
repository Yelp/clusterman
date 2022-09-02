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
import json
from typing import List
from typing import NamedTuple
from typing import Union


class ConditionTrait(enum.Enum):
    KERNEL = "kernel"
    LSBRELEASE = "lsbrelease"
    INSTANCE_TYPE = "instance_type"
    UPTIME = "uptime"


class MigrationCondition(NamedTuple):
    trait: ConditionTrait
    target: Union[str, int, List[str]]


class MigrationEvent(NamedTuple):
    event_id: str
    event_receipt: str
    cluster: str
    pool: str
    condition: MigrationCondition

    @classmethod
    def from_event(cls, event: dict) -> "MigrationEvent":
        """Parse migration trigger event into class instance

        :param str event: event data
        """
        event_data = json.loads(event["Body"])
        cond_key, cond_value = next(iter(event_data["condition"].items()))
        return cls(
            event_id=event["MessageId"],
            event_receipt=event["ReceiptHandle"],
            cluster=event_data["cluster"],
            pool=event_data["pool"],
            condition=MigrationCondition(ConditionTrait(cond_key), cond_value),
        )
