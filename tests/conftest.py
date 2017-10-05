import mock
import pytest

from clusterman.math.piecewise import PiecewiseConstantFunction


_ttl_patch = None


def pytest_configure(config):
    """ patch the MESOS_CACHE_TTL to prevent tests from failing; needs to happen before modules loaded """
    global _ttl_patch
    _ttl_patch = mock.patch('clusterman.mesos.mesos_role_manager.MESOS_CACHE_TTL', 0)
    _ttl_patch.__enter__()


def pytest_unconfigure(config):
    """ remove the TTL patch """
    _ttl_patch.__exit__()


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
