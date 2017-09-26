import mock
import pytest

from clusterman.common.aws import InstanceMarket
from clusterman.simulator.cluster import Cluster


@pytest.fixture
def cluster():
    cluster = Cluster()
    cluster.add_instances({
        InstanceMarket('m4.4xlarge', 'us-west-2a'): 4,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 2,
    }, launch_time=42)
    cluster.ebs_storage += 3000
    return cluster


@pytest.yield_fixture
def fake_markets():
    with mock.patch('clusterman.common.aws.EC2_INSTANCE_TYPES') as mock_instance_types, \
            mock.patch('clusterman.common.aws.EC2_AZS') as mock_azs:
        mock_instance_types.__contains__.return_value = True
        mock_azs.__contains__.return_value = True
        yield


def test_valid_market(fake_markets):
    InstanceMarket('foo', 'bar')


def test_invalid_market():
    with pytest.raises(ValueError):
        InstanceMarket('foo', 'bar')


def test_remove_too_many_hosts(fake_markets, cluster):
    with pytest.raises(ValueError):
        cluster.terminate_instances_by_market({InstanceMarket('foo', 'bar'): 2})


def test_cpu_mem_disk(cluster):
    assert len(cluster.instances) == 6
    assert cluster.cpu == 128
    assert cluster.mem == 744
    assert cluster.disk == 15800


def test_remove_instances(cluster):
    cluster.terminate_instances_by_market({
        InstanceMarket('m4.4xlarge', 'us-west-2a'): 3,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 1,
    })

    assert len(cluster.instances) == 2
    assert cluster.cpu == 48
    assert cluster.mem == 308
    assert cluster.disk == 9400
