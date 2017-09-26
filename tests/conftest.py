import mock
import pytest
import staticconf

from clusterman.aws.markets import PRIVATE_AWS_CONFIG
from clusterman.math.piecewise import PiecewiseConstantFunction


_azs_patch = None


def pytest_configure(config):
    """ patch the AZs list for the entire test run (before collection) """
    global _azs_patch
    subnet_to_azs = {'subnet': {'foo': 'fake-az-1', 'bar': 'fake-az-2', 'baz': 'fake-az-3'}}
    staticconf.DictConfiguration(subnet_to_azs, namespace=PRIVATE_AWS_CONFIG)
    _azs_patch = mock.patch('clusterman.aws.markets.EC2_AZS', subnet_to_azs['subnet'].values())
    _azs_patch.__enter__()


def pytest_unconfigure(config):
    """ remove the AZs patch """
    _azs_patch.__exit__()


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
