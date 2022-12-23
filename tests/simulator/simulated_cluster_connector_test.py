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
from unittest import mock

import arrow
import pytest

from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.types import AgentState
from clusterman.interfaces.types import ClustermanResources
from clusterman.simulator.simulate_aws_market import mock_get_market_resources
from clusterman.simulator.simulate_aws_market import simulate_InstanceMarket
from clusterman.simulator.simulated_aws_cluster import Instance
from clusterman.simulator.simulated_cluster_connector import SimulatedClusterConnector
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup

# from clusterman.aws.markets import get_market_resources
# from clusterman.aws.markets import InstanceMarket


TEST_MARKET = simulate_InstanceMarket("c3.4xlarge", "us-west-2a")


@pytest.fixture
def ssfrg_config():
    return {"LaunchSpecifications": [], "AllocationStrategy": "diversified"}


@pytest.fixture
def mock_ssfrg(ssfrg_config):
    ssfrg = SimulatedSpotFleetResourceGroup(ssfrg_config, None)
    instances = [Instance(TEST_MARKET, arrow.get(0), join_time=arrow.get(0)) for i in range(10)]
    ssfrg.instances = {instance.id: instance for instance in instances}
    return ssfrg


@pytest.fixture
def mock_cluster_connector(mock_ssfrg, simulator):
    simulator.aws_clusters = [mock_ssfrg]
    return SimulatedClusterConnector("foo", "bar", simulator)


def test_get_agent_metadata(mock_cluster_connector):
    instance = list(mock_cluster_connector.simulator.aws_clusters[0].instances.values())[0]
    mesos_resources = ClustermanResources(
        mock_get_market_resources(TEST_MARKET).cpus,
        mock_get_market_resources(TEST_MARKET).mem * 1000,
        mock_get_market_resources(TEST_MARKET).disk * 1000,
    )
    assert mock_cluster_connector.get_agent_metadata(instance.ip_address) == AgentMetadata(
        agent_id=mock.ANY,
        state=AgentState.IDLE,
        total_resources=mesos_resources,
    )


def test_get_agent_metadata_unknown(mock_cluster_connector):
    assert mock_cluster_connector.get_agent_metadata("1.2.3.4") == AgentMetadata(
        state=AgentState.ORPHANED,
    )


def test_simulated_agents(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_total("cpus") == 10 * mock_get_market_resources(TEST_MARKET).cpus
    assert mock_cluster_connector.get_resource_total("mem") == 10 * mock_get_market_resources(TEST_MARKET).mem
    assert mock_cluster_connector.get_resource_total("disk") == 10 * mock_get_market_resources(TEST_MARKET).disk
