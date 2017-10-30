import random

import arrow
import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.simulator.cluster import Cluster
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def simulator():
    return Simulator(SimulationMetadata('testing', 'test-tag'), arrow.get(0), arrow.get(3600))


@pytest.fixture
def cluster(simulator):
    cluster = Cluster(simulator)
    cluster.simulator.current_time.shift(seconds=+42)
    cluster.modify_size({
        InstanceMarket('m4.4xlarge', 'us-west-1a'): 4,
        InstanceMarket('i2.8xlarge', 'us-west-1a'): 2,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 1,
    })
    cluster.ebs_storage += 3000
    return cluster


@pytest.yield_fixture
def fake_markets():
    with mock.patch('clusterman.aws.markets.EC2_INSTANCE_TYPES') as mock_instance_types, \
            mock.patch('clusterman.aws.markets.EC2_AZS') as mock_azs:
        mock_instance_types.__contains__.return_value = True
        mock_azs.__contains__.return_value = True
        yield


def test_valid_market(fake_markets):
    InstanceMarket('foo', 'bar')


def test_invalid_market():
    with pytest.raises(ValueError):
        InstanceMarket('foo', 'bar')


def test_modify_cluster_capacity(cluster):
    cluster.simulator.current_time.shift(seconds=+76)
    added_instances, removed_instances = cluster.modify_size({
        InstanceMarket('m4.4xlarge', 'us-west-1a'): 1,
        InstanceMarket('i2.8xlarge', 'us-west-1a'): 4,
    })
    assert len(added_instances) == 2
    assert len(removed_instances) == 3
    assert len(cluster) == 6


def test_cpu_mem_disk(cluster):
    assert len(cluster) == 7
    assert cluster.cpu == 160
    assert cluster.mem == 988
    assert cluster.disk == 22200


def test_remove_instances(cluster):
    cluster.simulator.current_time.shift(seconds=+42)
    cluster.modify_size({
        InstanceMarket('m4.4xlarge', 'us-west-1a'): 1,
        InstanceMarket('i2.8xlarge', 'us-west-1a'): 1,
    })

    assert len(cluster) == 3
    assert cluster.cpu == 80
    assert cluster.mem == 552
    assert cluster.disk == 15800


def test_terminate_instances_by_ids(cluster):
    # Remove the random number of instances
    terminate_instances_ids = []
    remain_instances_ids = []
    for id in cluster.instances:
        if random.randint(0, 1) == 0:
            terminate_instances_ids.append(id)
        else:
            remain_instances_ids.append(id)
    cluster.terminate_instances_by_ids(terminate_instances_ids)
    for id in terminate_instances_ids:
        assert id not in cluster.instances
    for id in remain_instances_ids:
        assert id in cluster.instances
