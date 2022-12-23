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

import pytest

from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.interfaces.types import InstanceMetadata
from clusterman.migration.settings import MigrationPrecendence
from clusterman.migration.settings import PoolPortion
from clusterman.simulator.simulate_aws_market import simulate_InstanceMarket


@pytest.mark.parametrize(
    "initval,poolsize,result",
    (
        ("3%", 100, 3),
        (1, 100, 1),
        ("3%", 3, 1),
        (1, 3, 1),
        ("1", 3, 1),
        ("0%", 3, 0),
    ),
)
def test_pool_portion(initval, poolsize, result):
    assert PoolPortion(initval).of(poolsize) == result


@pytest.mark.parametrize(
    "initval,exctype",
    (
        ("-3%", ValueError),
        (-1, ValueError),
        ("foobar", ValueError),
    ),
)
def test_pool_portion_error(initval, exctype):
    with pytest.raises(exctype):
        PoolPortion(initval)


@pytest.mark.parametrize(
    "initval,expected",
    (
        ("5%", True),
        (1, True),
        ("0%", False),
        (0, False),
    ),
)
def test_pool_portion_truthy(initval, expected):
    assert bool(PoolPortion(initval)) is expected


@pytest.mark.parametrize(
    "precedence,expected_agent_id_order",
    (
        (MigrationPrecendence.UPTIME, ["3", "2", "1"]),
        (MigrationPrecendence.TASK_COUNT, ["1", "3", "2"]),
        (MigrationPrecendence.AZ_NAME, ["2", "1", "3"]),
    ),
)
def test_migration_precedence(precedence, expected_agent_id_order):
    nodes = [
        ClusterNodeMetadata(
            agent=AgentMetadata(agent_id="1", task_count=1),
            instance=InstanceMetadata(
                market=simulate_InstanceMarket("m6a.4xlarge", "us-west-2b"), weight=None, uptime=timedelta(days=10)
            ),
        ),
        ClusterNodeMetadata(
            agent=AgentMetadata(agent_id="2", task_count=3),
            instance=InstanceMetadata(
                market=simulate_InstanceMarket("m6a.4xlarge", "us-west-2a"), weight=None, uptime=timedelta(days=50)
            ),
        ),
        ClusterNodeMetadata(
            agent=AgentMetadata(agent_id="3", task_count=2),
            instance=InstanceMetadata(
                market=simulate_InstanceMarket("m6a.4xlarge", "us-west-2b"), weight=None, uptime=timedelta(days=90)
            ),
        ),
    ]
    assert [node.agent.agent_id for node in sorted(nodes, key=precedence.sort_key)] == expected_agent_id_order
