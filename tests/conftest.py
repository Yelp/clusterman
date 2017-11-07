import io
from contextlib import contextmanager

import mock
import pytest
import staticconf.testing
import yelp_meteorite

from clusterman.math.piecewise import PiecewiseConstantFunction


_ttl_patch = None


def pytest_configure(config):
    """ patch the CACHE_TTL_SECONDS to prevent tests from failing (TTL caches expire immediately);
    needs to happen before test modules are loaded """
    global _ttl_patch
    _ttl_patch = mock.patch('clusterman.mesos.constants.CACHE_TTL_SECONDS', -1)
    _ttl_patch.__enter__()


def pytest_unconfigure(config):
    """ remove the TTL patch """
    _ttl_patch.__exit__()


@contextmanager
def mock_open(filename, contents=None):
    """ This function modified from 'Revolution blahg':
    https://mapleoin.github.io/perma/mocking-python-file-open

    It is licensed under a Creative Commons Attribution 3.0 license
    (http://creativecommons.org/licenses/by/3.0/)
    """
    def mock_file(*args, **kwargs):
        if args[0] == filename:
            return io.StringIO(contents)
        else:
            mocked_file.stop()
            open_file = open(*args, **kwargs)
            mocked_file.start()
            return open_file
    mocked_file = mock.patch('builtins.open', mock_file)
    mocked_file.start()
    yield
    mocked_file.stop()


@pytest.fixture(autouse=True)
def main_clusterman_config():
    config = {
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
        },
        'batches': {
            'spot_prices': {
                'run_interval_seconds': 120,
                'dedupe_interval_seconds': 60,
            },
            'cluster_metrics': {
                'run_interval_seconds': 120,
            },
            'autoscaler': {
                'run_interval_seconds': 600,
            },
        },
        'mesos_clusters': {
            'mesos-test': {
                'leader_service': 'the.mesos.leader',
                'aws_region': 'us-west-2',
            },
        },
    }

    with staticconf.testing.MockConfiguration(config):
        yield


@pytest.fixture(autouse=True)
def clusterman_role_config():
    config = {
        'resource_groups': {
            's3': {
                'bucket': 'fake-bucket',
                'prefix': 'none',
            }
        },
        'defaults': {
            'min_capacity': 3,
            'max_capacity': 345,
            'max_weight_to_add': 200,
            'max_weight_to_remove': 10,
        },
        'autoscale_signals': [
            {
                'name': 'FakeSignalOne',
                'priority': 1,
                'param1': 42,
                'param2': 'asdf',
            },
            {
                'name': 'FakeSignalTwo',
                'paramA': 24,
                'paramB': 'fdsa',
            },
            {
                'name': 'FakeSignalThree',
                'priority': 1,
            },
            {
                'name': 'FakeSignalFour',
                'priority': 7,
            },
            {
                'name': 'MissingParamSignal',
            },
        ]
    }
    with staticconf.testing.MockConfiguration(config, namespace='bar_config'):
        yield


@pytest.fixture(autouse=True)
def mock_aws_client_setup():
    with mock_open('/etc/secrets', '{"accessKeyId": "foo", "secretAccessKey": "bar"}'):
        yield


@pytest.fixture(autouse=True)
def block_meteorite_emission():
    with yelp_meteorite.testcase():
        yield


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
