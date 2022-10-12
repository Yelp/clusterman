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
from typing import cast
from typing import NamedTuple
from typing import Union

from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.util import parse_time_interval_seconds


DEFAULT_POOL_PRESCALING = 0
DEFAULT_NODE_BOOT_WAIT = "3m"
DEFAULT_NODE_BOOT_TIMEOUT = "10m"
DEFAULT_WORKER_TIMEOUT = "2h"
DEFAULT_HEALTH_CHECK_INTERVAL = "2m"


class MigrationPrecendence(enum.Enum):
    UPTIME = "highest_uptime"
    TASK_COUNT = "lowest_task_count"

    @classmethod
    def default(cls) -> str:
        return cls.UPTIME.value

    def sort_key(self, node: ClusterNodeMetadata) -> int:
        """Key function to be passed to sorting routines"""
        if self == MigrationPrecendence.UPTIME:
            return -node.instance.uptime.total_seconds()
        elif self == MigrationPrecendence.TASK_COUNT:
            return node.agent.task_count
        return 0


class PoolPortion:
    def __init__(self, value: Union[str, int]) -> None:
        self.init_value = value
        if isinstance(value, str) and "%" in value:
            self.value = float(value.rstrip("%")) / 100
            self.min_portion = 1 if self.value != 0 else 0
            self.of = self._of_percent
        else:
            self.value = int(value)
            self.of = self._of_absolute
        if self.value < 0:
            raise ValueError(f"Bad value {value}, only int/percentage greater or equal to 0 allowed")

    def _of_percent(self, pool_size: int) -> int:
        return max(self.min_portion, round(self.value * pool_size))

    def _of_absolute(self, pool_size: int) -> int:
        return cast(int, self.value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PoolPortion):
            raise NotImplementedError()
        return self.init_value == other.init_value

    def __bool__(self) -> bool:
        return self.value > 0


class WorkerSetup(NamedTuple):
    rate: PoolPortion
    prescaling: PoolPortion
    precedence: MigrationPrecendence
    bootstrap_wait: float
    bootstrap_timeout: float
    disable_autoscaling: bool
    expected_duration: float
    health_check_interval: int
    ignore_pod_health: bool = False

    @classmethod
    def from_config(cls, config: dict) -> "WorkerSetup":
        strat_conf: dict = config["strategy"]
        churn_rate = PoolPortion(strat_conf["rate"])
        if not churn_rate:
            raise ValueError(f"Node migration rate must greater than 0: {strat_conf['rate']}")
        return cls(
            rate=churn_rate,
            prescaling=PoolPortion(strat_conf.get("prescaling", DEFAULT_POOL_PRESCALING)),
            precedence=MigrationPrecendence(strat_conf.get("precedence", MigrationPrecendence.default())),
            bootstrap_wait=parse_time_interval_seconds(strat_conf.get("bootstrap_wait", DEFAULT_NODE_BOOT_WAIT)),
            bootstrap_timeout=parse_time_interval_seconds(strat_conf.get("bootstrap_timeout", DEFAULT_NODE_BOOT_WAIT)),
            disable_autoscaling=config.get("disable_autoscaling", False),
            expected_duration=parse_time_interval_seconds(config.get("expected_duration", DEFAULT_WORKER_TIMEOUT)),
            ignore_pod_health=config.get("ignore_pod_health", False),
            health_check_interval=parse_time_interval_seconds(
                config.get("health_check_interval", DEFAULT_HEALTH_CHECK_INTERVAL)
            ),
        )
