import simplejson as json

from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup


def test_load_spot_fleets_from_tags():
    MesosPoolResourceGroup.__abstractmethods__ = set()
    MesosPoolResourceGroup._get_resource_group_tags = lambda: {
        'sfr-123': {
            'some': 'tag',
            'paasta': 'true',
            'puppet:role::paasta': json.dumps({
                'pool': 'default',
                'paasta_cluster': 'westeros-prod',
            }),
        },
        'sfr-456': {
            'some': 'tag',
            'paasta': 'true',
            'puppet:role::paasta': json.dumps({
                'pool': 'another',
                'paasta_cluster': 'westeros-prod',
            }),
        },
        'sfr-789': {
            'some': 'tag',
            'paasta': 'true',
            'puppet:role::paasta': json.dumps({
                'paasta_cluster': 'westeros-prod',
            }),
        },
        'sfr-abc': {
            'paasta': 'false',
            'puppet:role::riice': json.dumps({
                'pool': 'default',
                'paasta_cluster': 'westeros-prod',
            }),
        }
    }
    resource_groups = MesosPoolResourceGroup.load(
        cluster='westeros-prod',
        pool='default',
        config={'tag': 'puppet:role::paasta'},
    )
    assert len(resource_groups) == 1
    assert list(resource_groups) == ['sfr-123']
