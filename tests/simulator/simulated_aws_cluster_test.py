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
import pytest

from clusterman.simulator.simulate_aws_market import simulate_InstanceMarket
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster


@pytest.fixture
def cluster(simulator):
    cluster = SimulatedAWSCluster(simulator)
    cluster.simulator.current_time.shift(seconds=+42)
    cluster.modify_size(
        {
            simulate_InstanceMarket("m4.4xlarge", "us-west-1a"): 4,
            simulate_InstanceMarket("i2.8xlarge", "us-west-1a"): 2,
            simulate_InstanceMarket("i2.8xlarge", "us-west-2a"): 1,
        }
    )
    cluster.ebs_storage += 3000
    return cluster


def test_modify_size(cluster):
    cluster.simulator.current_time.shift(seconds=+76)
    added_instances, removed_instances = cluster.modify_size(
        {
            simulate_InstanceMarket("m4.4xlarge", "us-west-1a"): 1,
            simulate_InstanceMarket("i2.8xlarge", "us-west-1a"): 4,
        }
    )
    assert len(added_instances) == 2
    assert len(removed_instances) == 4
    assert len(cluster) == 5


def test_cpu_mem_disk(cluster):
    assert len(cluster) == 7
    assert cluster.cpus == 160
    assert cluster.mem == 988
    assert cluster.disk == 22200


def test_remove_instances(cluster):
    cluster.simulator.current_time.shift(seconds=+42)
    cluster.modify_size(
        {
            simulate_InstanceMarket("m4.4xlarge", "us-west-1a"): 1,
            simulate_InstanceMarket("i2.8xlarge", "us-west-1a"): 1,
        }
    )

    assert len(cluster) == 2
    assert cluster.cpus == 48
    assert cluster.mem == 308
    assert cluster.disk == 9400


def test_terminate_instances_by_id(cluster):
    terminate_instances_ids = []
    remaining_instances_ids = []
    for i, id in enumerate(cluster.instances):
        if i % 3:
            terminate_instances_ids.append(id)
        else:
            remaining_instances_ids.append(id)
    cluster.terminate_instances_by_id(terminate_instances_ids)
    for id in terminate_instances_ids:
        assert id not in cluster.instances
    for id in remaining_instances_ids:
        assert id in cluster.instances
