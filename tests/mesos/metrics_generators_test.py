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
import mock
import pytest

from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.exceptions import NoResourceGroupsFoundError
from clusterman.mesos.metrics_generators import ClusterMetric
from clusterman.mesos.metrics_generators import generate_simple_metadata
from clusterman.mesos.metrics_generators import generate_system_metrics


@pytest.fixture
def mock_pool_manager():
    mock_pool_manager = mock.Mock(spec=PoolManager)
    mock_pool_manager.cluster_connector = mock.Mock(cluster="mesos-test", pool="bar", scheduler="mesos")
    mock_pool_manager.cluster = "mesos-test"
    mock_pool_manager.pool = "bar"
    mock_pool_manager.scheduler = "mesos"

    resource_totals = {"cpus": 20, "mem": 2000, "disk": 20000, "gpus": 0}
    mock_pool_manager.cluster_connector.get_resource_total.side_effect = resource_totals.get

    market_capacities = {"market1": 15, "market2": 25}
    mock_pool_manager.get_market_capacities.return_value = market_capacities

    mock_pool_manager.non_orphan_fulfilled_capacity = 12

    return mock_pool_manager


@pytest.fixture
def simple_metadata_expected_metrics(mock_pool_manager):
    return [
        ClusterMetric(metric_name="cpus_total", value=20, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},),
        ClusterMetric(metric_name="mem_total", value=2000, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},),
        ClusterMetric(
            metric_name="disk_total", value=20000, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
        ClusterMetric(metric_name="gpus_total", value=0, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},),
        ClusterMetric(
            metric_name="target_capacity",
            value=mock_pool_manager.target_capacity,
            dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
        ClusterMetric(
            metric_name="fulfilled_capacity",
            value={"market1": 15, "market2": 25},
            dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
        ClusterMetric(
            metric_name="non_orphan_fulfilled_capacity",
            value=12,
            dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
    ]


def test_generate_system_metrics(mock_pool_manager):
    resources_allocated = {"cpus": 10, "mem": 1000, "disk": 10000, "gpus": 0}
    mock_pool_manager.cluster_connector.get_resource_allocation.side_effect = resources_allocated.get

    expected_metrics = [
        ClusterMetric(
            metric_name="cpus_allocated", value=10, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
        ClusterMetric(
            metric_name="mem_allocated", value=1000, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
        ClusterMetric(
            metric_name="disk_allocated", value=10000, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
        ClusterMetric(
            metric_name="gpus_allocated", value=0, dimensions={"cluster": "mesos-test", "pool": "bar.mesos"},
        ),
    ]
    assert sorted(generate_system_metrics(mock_pool_manager)) == sorted(expected_metrics)


def test_generate_simple_metadata(mock_pool_manager, simple_metadata_expected_metrics):
    assert sorted(generate_simple_metadata(mock_pool_manager)) == sorted(simple_metadata_expected_metrics)


def test_generate_simple_metadata_no_terraform_resources(mock_pool_manager, simple_metadata_expected_metrics):
    # Break target_capacity, make sure exception is handled
    type(mock_pool_manager).target_capacity = mock.PropertyMock(side_effect=NoResourceGroupsFoundError)

    # Update expected metrics to exclude target_capacity
    expected_metrics = list(simple_metadata_expected_metrics)
    del expected_metrics[4]

    assert sorted(generate_simple_metadata(mock_pool_manager)) == sorted(expected_metrics)
