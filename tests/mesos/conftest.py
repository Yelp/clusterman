import io
from contextlib import contextmanager

import mock
import pytest
import staticconf.testing
from moto import mock_ec2


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
def setup_ec2():
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    yield
    mock_ec2_obj.stop()


@pytest.fixture(autouse=True)
def mock_aws_config():
    mock_config = {
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
        },
    }
    with staticconf.testing.MockConfiguration(mock_config):
        yield


@pytest.fixture(autouse=True)
def mock_aws_client_setup():
    with mock_open('/etc/secrets', '{"accessKeyId": "foo", "secretAccessKey": "bar"}'):
        yield


@pytest.fixture
def mock_agents_dict():
    return {
        'get_agents': {
            'agents': [
                {
                    'agent_info': {
                        'attributes': [
                            {'name': 'blah', 'scalar': {'value': 10}},
                            {'name': 'role', 'text': {'value': 'asdf'}},
                        ],
                        'hostname': 'not-in-the-role.yelpcorp.com',
                    }
                },
                {
                    'agent_info': {
                        'hostname': 'asdf.yelpcorp.com',
                        'allocated_resources': [{'name': 'mem', 'scalar': {'value': 10}}],
                    }
                },
                {
                    'agent_info': {
                        'attributes': [
                            {'name': 'blah', 'scalar': {'value': 10}},
                            {'name': 'role', 'text': {'value': 'baz'}},
                            {'name': 'ssss', 'text': {'value': 'hjkl'}},
                        ],
                        'hostname': 'im-in-the-role.yelpcorp.com',
                        'allocated_resources': [
                            {'name': 'mem', 'scalar': {'value': 20}},
                            {'name': 'cpus', 'scalar': {'value': 10}},
                        ],
                    }
                },
            ]
        }
    }
