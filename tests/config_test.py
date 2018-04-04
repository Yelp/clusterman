import argparse
import json

import mock
import pytest
import staticconf
import staticconf.testing
import yaml

import clusterman.config as config
from clusterman.config import ROLE_NAMESPACE
from tests.conftest import mock_open


@pytest.fixture
def mock_config_files():
    # Role 1 is in both cluster A and B, while Role 2 is only in A.
    with staticconf.testing.PatchConfiguration(
        {'cluster_config_directory': '/nail/whatever'}
    ), mock_open(
        config.get_role_config_path('cluster-A', 'role-1'),
        contents=yaml.dump({
            'resource_groups': 'cluster-A',
            'other_config': 18,
        }),
    ), mock_open(
        config.get_role_config_path('cluster-A', 'role-2'),
        contents=yaml.dump({
            'resource_groups': 'cluster-A',
            'other_config': 20,
        }),
    ), mock_open(
        config.get_role_config_path('cluster-B', 'role-1'),
        contents=yaml.dump({
            'resource_groups': 'cluster-B',
            'other_config': 200,
            'autoscale_signal': {'branch_or_tag': 'v42'},
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
                    'config': [{
                        'max_weight_to_add': 10,
                        'max_weight_to_remove': 10,
                    }],
                },
            },
            'aws': {
                'access_key_file': '/etc/no_cfg/clusterman.json',
            }
        },
        namespace=staticconf.config.DEFAULT,
    ):
        yield


@pytest.mark.parametrize('cluster,include_roles,tag', [
    ('cluster-A', True, None),
    ('cluster-A', True, 'v52'),
    ('cluster-A', False, None),
])
@mock.patch('clusterman.config.load_cluster_role_configs', autospec=True)
@mock.patch('clusterman.config.load_default_config')
def test_setup_config_cluster(mock_service_load, mock_role_load, cluster, include_roles, tag, mock_config_files):
    args = argparse.Namespace(env_config_path='/nail/etc/config.yaml', cluster=cluster, signals_branch_or_tag=tag)
    config.setup_config(args, include_roles=include_roles)

    assert mock_service_load.call_args == mock.call('/nail/etc/config.yaml', '/nail/etc/config.yaml')
    assert staticconf.read_string('aws.region') == 'us-test-3'
    if include_roles:
        assert mock_role_load.call_args == mock.call(cluster, tag)
    else:
        assert mock_role_load.call_count == 0
        if tag:
            assert staticconf.read_string('autoscale_signal.branch_or_tag') == tag


def test_setup_config_region_and_cluster():
    args = argparse.Namespace(env_config_path='/nail/etc/config.yaml', cluster='foo', aws_region='bar')
    with mock.patch('clusterman.config.load_default_config'), pytest.raises(argparse.ArgumentError):
        config.setup_config(args)


@mock.patch('clusterman.config.load_default_config')
def test_setup_config_region(mock_service_load, mock_config_files):
    args = argparse.Namespace(env_config_path='/nail/etc/config.yaml', aws_region='fake-region-A')
    config.setup_config(args)
    assert staticconf.read_string('aws.region') == 'fake-region-A'
    assert mock_service_load.call_args == mock.call('/nail/etc/config.yaml', '/nail/etc/config.yaml')


@pytest.mark.parametrize('cluster,roles,role_other_config', [
    ('cluster-A', ['role-1', 'role-2'], [18, 20]),
    ('cluster-B', ['role-1'], [200]),
    ('cluster-C', [], []),
])
@mock.patch('os.listdir')
def test_load_cluster_role_configs(mock_ls, cluster, roles, role_other_config, mock_config_files):
    mock_ls.return_value = [f'{role}.yaml' for role in roles] + ['.foo.yaml']
    config.load_cluster_role_configs(cluster, None)

    for i, role in enumerate(roles):
        role_namespace = ROLE_NAMESPACE.format(role=role)
        assert staticconf.read_int('other_config', namespace=role_namespace) == role_other_config[i]
        assert staticconf.read_string(f'resource_groups', namespace=role_namespace) == cluster

    assert sorted(staticconf.read_list('cluster_roles')) == sorted(roles)
