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


@mock.patch('clusterman.simulator.cluster.is_valid_market')
class TestModifyCluster:
    def test_invalid_market(self, is_valid_market, cluster):
        is_valid_market.return_value = False
        with pytest.raises(KeyError):
            cluster.add_instances({InstanceMarket('foo', 'bar'): 4}, launch_time=42)

    def test_remove_too_many_hosts(self, is_valid_market, cluster):
        is_valid_market.return_value = True
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
