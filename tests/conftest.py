import mock
import pytest

from clusterman.math.piecewise import PiecewiseConstantFunction


_ttl_patch = None


def pytest_configure(config):
    """ patch the CACHE_TTL to prevent tests from failing (TTL caches expire immediately);
    needs to happen before test modules are loaded """
    global _ttl_patch
    _ttl_patch = mock.patch('clusterman.mesos.constants.CACHE_TTL', -1)
    _ttl_patch.__enter__()


def pytest_unconfigure(config):
    """ remove the TTL patch """
    _ttl_patch.__exit__()


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
