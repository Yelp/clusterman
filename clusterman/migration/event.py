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
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import Union

import arrow
import packaging.version
import semver

from clusterman.aws.markets import get_instance_type
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.migration.constants import MIGRATION_CRD_ATTEMPTS_LABEL
from clusterman.migration.event_enums import ComparableConditionTarget
from clusterman.migration.event_enums import ComparableVersion
from clusterman.migration.event_enums import CONDITION_OPERATOR_SUPPORT_MATRIX
from clusterman.migration.event_enums import ConditionOperator
from clusterman.migration.event_enums import ConditionTrait
from clusterman.util import parse_time_interval_seconds


def _load_lsbrelease_version_target(target: str) -> ComparableVersion:
    """Validate condition target as a version

    :param str target: version from user input
    :return: same value if validates as version
    """
    parsed = packaging.version.parse(target)
    if not isinstance(parsed, packaging.version.Version):
        raise ValueError(f"Invalid version string: {target}")
    return parsed


def load_timespan_target(target: str) -> int:
    """Validate and parse input timespan

    :param str target: either seconds or human readable span (e.g. 5d)
    :return: integer timespan in seconds
    """
    return int(target if target.isnumeric() else parse_time_interval_seconds(target))


def _load_instance_type_target(target: str) -> str:
    """Validate and parse instance-type migration condition target

    :param str target: target user input
    :return: list of instance types
    """
    target = target.lower()
    try:
        get_instance_type(target)
    except Exception:
        raise ValueError(f"Invalid instance type: {target}")

    return target


CONDITION_TARGET_LOADERS: Dict[ConditionTrait, Callable[[str], Union[str, int, List[str]]]] = {
    ConditionTrait.KERNEL: semver.VersionInfo.parse,
    ConditionTrait.LSBRELEASE: _load_lsbrelease_version_target,
    ConditionTrait.UPTIME: load_timespan_target,
    ConditionTrait.INSTANCE_TYPE: _load_instance_type_target,
}


class MigrationCondition(NamedTuple):
    trait: ConditionTrait
    operator: ConditionOperator
    target: Union[ComparableConditionTarget, List[ComparableConditionTarget]]

    @classmethod
    def from_dict(cls, data: dict) -> "MigrationCondition":
        """Load condition class instance from data dictionary

        :param dict data: condition data
        :return: condition instance
        """
        trait = ConditionTrait(data["trait"])
        op = ConditionOperator(data["operator"])
        if op not in CONDITION_OPERATOR_SUPPORT_MATRIX[trait]:
            raise ValueError(f"{op} is not support in conditions with {trait}")
        target_loader = CONDITION_TARGET_LOADERS[trait]
        return cls(
            trait=trait,
            operator=op,
            target=(
                list(map(target_loader, data["target"].split(",")))
                if op in ConditionOperator.expecting_collection()
                else target_loader(data["target"])
            ),
        )

    def stringify_target(self) -> str:
        """Turn condition target into a string"""
        return ",".join(map(str, self.target)) if isinstance(self.target, list) else str(self.target)

    def to_dict(self) -> dict:
        return {
            "trait": self.trait.value,
            "operator": self.operator.value,
            "target": self.stringify_target(),
        }

    def matches(self, node: ClusterNodeMetadata, present_time: Optional[arrow.Arrow] = None) -> bool:
        """Check if condition is met for a node

        :param ClusterNodeMetadata node: node metadata
        :param arrow.Arrow present_time: offset time for uptime comparisons
        :return: true if it meets the condition
        """
        target = (
            self.target + (arrow.now() - present_time).total_seconds()
            if present_time and self.trait == ConditionTrait.UPTIME
            else self.target
        )
        return self.operator.apply(self.trait.get_from(node), target)

    def __str__(self) -> str:
        return f"{self.trait.name} {self.operator.value} {self.stringify_target()}"


class MigrationEvent(NamedTuple):
    resource_name: str
    cluster: str
    pool: str
    label_selectors: List[str]
    condition: MigrationCondition
    previous_attempts: int = 0
    created: Optional[arrow.Arrow] = None

    def __hash__(self) -> int:
        """Simplified object hash since resource_name should be unique"""
        return cast(Tuple[str, str, str], self[:3]).__hash__()

    def __str__(self) -> str:
        return (
            f"MigrationEvent(cluster={self.cluster}, pool={self.pool},"
            f" label_selectors={self.label_selectors}, condition=({self.condition}),"
            f" attempts={self.previous_attempts}, created={self.created})"
        )

    def to_crd_body(self, labels: Optional[dict] = None) -> dict:
        """Pack event data into a CRD payload

        :return: payload dictionary
        """
        body = {
            "metadata": {
                "name": self.resource_name,
            },
            "spec": {
                "cluster": self.cluster,
                "pool": self.pool,
                "label_selectors": self.label_selectors,
                "condition": self.condition.to_dict(),
            },
        }
        if labels:
            body["metadata"]["labels"] = labels.copy()  # type: ignore
        return body

    def matches(self, node: ClusterNodeMetadata) -> bool:
        """Check if event condition is met for a node

        :param ClusterNodeMetadata node: node metadata
        :return: true if it meets the condition
        """
        return self.condition.matches(node, self.created)

    @classmethod
    def from_crd(cls, crd: dict) -> "MigrationEvent":
        """Load migration trigger event into class instance

        :param dict crd: event data
        :return: event instance
        """
        event_data = crd["spec"]
        return cls(
            resource_name=crd["metadata"]["name"],
            cluster=event_data["cluster"],
            pool=event_data["pool"],
            label_selectors=event_data.get("label_selectors", []),
            condition=MigrationCondition.from_dict(event_data["condition"]),
            previous_attempts=int(crd["metadata"]["labels"].get(MIGRATION_CRD_ATTEMPTS_LABEL, 0)),
            created=arrow.get(crd["metadata"].get("creationTimestamp", arrow.now())),
        )
