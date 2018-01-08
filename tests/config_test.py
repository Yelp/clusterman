import argparse
import json

import mock
import pytest
import staticconf
import staticconf.testing
import yaml

import clusterman.config as config
from clusterman.mesos.constants import ROLE_NAMESPACE
from tests.conftest import mock_open


@pytest.fixture
def config_dir():
    return '/nail/whatever'


@pytest.fixture
def mock_config_files(config_dir):
    # Role 1 is in both cluster A and B, while Role 2 is only in A.
    with mock_open(
        config.get_role_config_path('role-1'),
        contents=yaml.dump({
            'mesos': {
                'cluster-A': {'resource_groups': 'cluster-A'},
            },
            'other_config': 18,
        }),
    ), mock_open(
        config.get_role_config_path('role-2'),
        contents=yaml.dump({
            'mesos': {
                'cluster-A': {'resource_groups': 'cluster-A'},
                'cluster-B': {'resource_groups': 'cluster-B'},
            },
            'other_config': 18,
        }),
    ), mock_open(
        '/etc/no_cfg/clusterman.json',
        contents=json.dumps({
            'accessKeyId': 'foo',
            'secretAccessKey': 'bar',
            'region': 'nowhere-useful',
        })
    ):
        yield


@pytest.fixture(autouse=True)
def mock_config_namespaces():
    # To avoid polluting staticconf for other tests, and clear out stuff from conftest that mocks configuration
    with staticconf.testing.MockConfiguration(
        {},
        namespace=ROLE_NAMESPACE.format(role='role-1'),
    ), staticconf.testing.MockConfiguration(
        {},
        namespace=ROLE_NAMESPACE.format(role='role-2'),
    ), staticconf.testing.MockConfiguration(
        {
            'mesos_clusters': {
                'cluster-A': {
                    'fqdn': 'service.leader',
                    'aws_region': 'us-test-3',
                },
            },
            'aws': {
                'access_key_file': '/etc/no_cfg/clusterman.json',
            }
        },
        namespace=staticconf.config.DEFAULT,
    ):
        yield


@pytest.mark.parametrize('cluster,include_roles', [
    (None, True),
    (None, False),
    ('cluster-A', True),
    ('cluster-A', False),
])
@mock.patch('clusterman.config.load_role_configs_for_cluster', autospec=True)
@mock.patch('clusterman.config.load_default_config')
def test_setup_config(mock_service_load, mock_role_load, cluster, include_roles, mock_config_files):
    args = argparse.Namespace(env_config_path='/nail/etc/config.yaml', cluster=cluster)
    config.setup_config(args, include_roles=include_roles)

    assert mock_service_load.call_args_list == [mock.call('/nail/etc/config.yaml', '/nail/etc/config.yaml')]
    if cluster is not None:
        assert staticconf.read_string('aws.region') == 'us-test-3'
        if include_roles:
            assert mock_role_load.call_args_list == [mock.call(cluster)]
        else:
            assert mock_role_load.call_args_list == []


@pytest.mark.parametrize('cluster,roles', [
    ('cluster-A', ['role-1', 'role-2']),
    ('cluster-B', ['role-2']),
    ('cluster-C', []),
])
@mock.patch('os.listdir')
def test_load_role_configs_for_cluster(mock_ls, cluster, roles, config_dir, mock_config_files):
    mock_ls.return_value = ['role-1', 'role-2']
    config.load_role_configs_for_cluster(cluster)

    for role in roles:
        role_namespace = ROLE_NAMESPACE.format(role=role)
        assert staticconf.read_int('other_config', namespace=role_namespace) == 18
        assert staticconf.read_string(f'mesos.resource_groups', namespace=role_namespace) == cluster

    assert sorted(staticconf.read_list('cluster_roles')) == sorted(roles)
