import io
from contextlib import contextmanager

import mock
import pytest
import staticconf.testing
import yelp_meteorite

from clusterman.aws.client import CREDENTIALS_NAMESPACE
from clusterman.math.piecewise import PiecewiseConstantFunction


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
        },
        'mesos_clusters': {
            'mesos-test': {
                'fqdn': 'the.mesos.leader',
                'aws_region': 'us-west-2',
            },
        },
        'cluster_roles': ['bar'],
    }

    with staticconf.testing.MockConfiguration(config):
        yield


@pytest.fixture(autouse=True)
def clusterman_role_config():
    config = {
        'mesos': {
            'resource_groups': {
                's3': {
                    'bucket': 'fake-bucket',
                    'prefix': 'none',
                }
            },
        },
        'scaling_limits': {
            'min_capacity': 3,
            'max_capacity': 345,
            'max_weight_to_add': 200,
            'max_weight_to_remove': 10,
        },
    }
    with staticconf.testing.MockConfiguration(config, namespace='bar_config'):
        yield


@pytest.fixture(autouse=True)
def mock_aws_client_setup():
    config = {
        'accessKeyId': 'foo',
        'secretAccessKey': 'bar',
    }
    with staticconf.testing.MockConfiguration(config, namespace=CREDENTIALS_NAMESPACE):
        yield


@pytest.fixture(autouse=True)
def block_meteorite_emission():
    with yelp_meteorite.testcase():
        yield


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
