import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.simulator.cluster import Cluster


@pytest.fixture
def cluster():
    cluster = Cluster()
    cluster.modify_capacity({
        InstanceMarket('m4.4xlarge', 'us-west-2a'): 4,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 2,
        InstanceMarket('i2.8xlarge', 'us-west-2b'): 1,
    }, modify_time=42)
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
    added_instances, removed_instances = cluster.modify_capacity({
        InstanceMarket('m4.4xlarge', 'us-west-2a'): 1,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 4,
    }, modify_time=76)
    assert len(added_instances) == 2
    assert len(removed_instances) == 3
    assert len(cluster.instances) == 6


def test_cpu_mem_disk(cluster):
    assert len(cluster) == 7
    assert cluster.cpu == 160
    assert cluster.mem == 988
    assert cluster.disk == 22200


def test_remove_instances(cluster):
    cluster.modify_capacity({
        InstanceMarket('m4.4xlarge', 'us-west-2a'): 1,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 1,
    }, modify_time=42)

    assert len(cluster) == 3
    assert cluster.cpu == 80
    assert cluster.mem == 552
    assert cluster.disk == 15800
