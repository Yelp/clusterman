import mock
import pytest
import yaml
from moto import mock_ec2

from clusterman.aws.client import ec2
from clusterman.exceptions import MarketProtectedException
from clusterman.exceptions import MesosRoleManagerError
from clusterman.exceptions import ResourceGroupProtectedException
from clusterman.mesos.mesos_role_manager import DEFAULT_ROLE_CONFIG
from clusterman.mesos.mesos_role_manager import get_roles_in_cluster
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.mesos_role_manager import SERVICES_FILE
from tests.conftest import mock_open


@pytest.fixture
def mock_resource_groups():
    return [
        mock.Mock(
            id=f'sfr-{i}',
            instances=[f'i-{i}'],
            target_capacity=i * 2 + 1,
            fulfilled_capacity=i * 6,
            market_capacities={'market-1': i, 'market-2': i * 2, 'market-3': i * 3},
        )
        for i in range(7)
    ]


@pytest.fixture
def mock_role_manager(mock_resource_groups):
    with mock.patch('clusterman.mesos.mesos_role_manager.load_configs_for_cluster'), \
            mock_open(SERVICES_FILE, 'the.mesos.leader:\n  host: foo\n  port: 1234'), \
            mock.patch('clusterman.mesos.mesos_role_manager.load_spot_fleets_from_s3') as mock_load:
        mock_load.return_value = []
        manager = MesosRoleManager('mesos-test', 'bar')
        manager.resource_groups = mock_resource_groups

        return manager


def test_mesos_role_manager_init(mock_role_manager):
    assert mock_role_manager.role == 'bar'
    assert mock_role_manager.api_endpoint == 'http://foo:1234/api/v1'


@pytest.mark.parametrize('cluster,roles', [
    ('cluster-A', ['role-1', 'role-2']),
    ('cluster-B', ['role-2']),
    ('cluster-C', []),
])
@mock.patch('os.listdir')
def test_get_roles_in_cluster(mock_ls, cluster, roles):
    mock_ls.return_value = ['role-1', 'role-2']
    with mock_open(
        DEFAULT_ROLE_CONFIG.format(role='role-1'),
        contents=yaml.dump({
            'mesos': {
                'cluster-A': {},
            }
        }),
    ), mock_open(
        DEFAULT_ROLE_CONFIG.format(role='role-2'),
        contents=yaml.dump({
            'mesos': {
                'cluster-A': {},
                'cluster-B': {},
            }
        }),
    ):
        assert roles == get_roles_in_cluster(cluster)


def test_modify_target_capacity_no_resource_groups(mock_role_manager):
    mock_role_manager.resource_groups = []
    with pytest.raises(MesosRoleManagerError):
        mock_role_manager.modify_target_capacity(1234)


@pytest.mark.parametrize('constrain_return', [100, 50])
def test_modify_target_capacity(constrain_return, mock_role_manager):
    mock_role_manager.prune_excess_fulfilled_capacity = mock.Mock()

    mock_role_manager._constrain_target_capacity = mock.Mock(return_value=constrain_return)
    mock_role_manager._compute_new_resource_group_targets = mock.Mock(return_value=[
        (3, 3), (0, 0), (1, 1), (6, 6), (4, 4), (2, 2), (5, 5),
    ])
    assert mock_role_manager.modify_target_capacity(100) == constrain_return
    assert mock_role_manager._constrain_target_capacity.call_count == 1
    assert mock_role_manager.prune_excess_fulfilled_capacity.call_count == 0
    assert mock_role_manager._compute_new_resource_group_targets.call_count == 1
    for i, group in enumerate(mock_role_manager.resource_groups):
        assert group.modify_target_capacity.call_count == 1
        assert group.modify_target_capacity.call_args[0][0] == i


@mock.patch('clusterman.mesos.mesos_role_manager.logger')
@mock.patch('clusterman.mesos.mesos_role_manager.find_largest_capacity_market')
@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._mark_instance_for_removal')
@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._get_market_capacities')
@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._idle_agents_by_market')
@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager.target_capacity', mock.PropertyMock(return_value=100))
@mock.patch(
    'clusterman.mesos.mesos_role_manager.MesosRoleManager.fulfilled_capacity',
    mock.PropertyMock(return_value=100),
)
class TestPruneFulfilledCapacity:
    def test_under_capacity(self, mock_idle_agents_by_market, mock_get_market_capacities,
                            mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                            mock_logger, mock_role_manager):
        mock_role_manager.prune_excess_fulfilled_capacity(110)
        assert mock_find_largest_capacity_market.call_count == 0
        assert mock_mark_instance_for_removal.call_count == 0
        assert mock_logger.warn.call_count == 0

    def test_no_idle_instances(self, mock_idle_agents_by_market, mock_get_market_capacities,
                               mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                               mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {}
        mock_find_largest_capacity_market.return_value = (None, 0)
        mock_role_manager.prune_excess_fulfilled_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 1
        assert mock_mark_instance_for_removal.call_count == 0
        assert mock_logger.warn.call_count == 1

    def test_instance_error(self, mock_idle_agents_by_market, mock_get_market_capacities,
                            mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                            mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_find_largest_capacity_market.return_value = ('market-1', 70)
        mock_mark_instance_for_removal.side_effect = [MesosRoleManagerError('something bad happened'), 10]
        mock_role_manager.prune_excess_fulfilled_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 2
        assert mock_mark_instance_for_removal.call_count == 2
        assert type(mock_logger.warn.call_args_list[0][0][0]) == MesosRoleManagerError

    def test_protected_market(self, mock_idle_agents_by_market, mock_get_market_capacities,
                              mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                              mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_get_market_capacities.return_value = {'market-1': 70, 'market-2': 30}
        mock_find_largest_capacity_market.side_effect = [('market-1', 70), ('market-2', 30)]
        mock_mark_instance_for_removal.side_effect = [MarketProtectedException('market-1 is full'), 10]
        mock_role_manager.prune_excess_fulfilled_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 2
        assert mock_mark_instance_for_removal.call_count == 2
        assert mock_get_market_capacities.return_value == {'market-2': 20}

    def test_protected_group(self, mock_idle_agents_by_market, mock_get_market_capacities,
                             mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                             mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_get_market_capacities.return_value = {'market-1': 70, 'market-2': 30}
        mock_find_largest_capacity_market.side_effect = [('market-1', 70), ('market-2', 0)]
        mock_mark_instance_for_removal.side_effect = [ResourceGroupProtectedException('group-1 is full')]
        mock_role_manager.prune_excess_fulfilled_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 2
        assert mock_mark_instance_for_removal.call_count == 1
        assert mock_get_market_capacities.return_value == {'market-1': 70, 'market-2': 30}


def test_compute_new_resource_group_targets_no_unfilled_capacity(mock_role_manager):
    assert mock_role_manager._compute_new_resource_group_targets(mock_role_manager.target_capacity) == [
        (i, group.target_capacity)
        for i, group in enumerate(mock_role_manager.resource_groups)
    ]


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal(orig_targets, mock_role_manager):
    for group in mock_role_manager.resource_groups:
        group.target_capacity = orig_targets

    num_groups = len(mock_role_manager.resource_groups)
    new_targets = mock_role_manager._compute_new_resource_group_targets(105)
    assert sorted(target for __, target in new_targets) == [15] * num_groups


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal_with_remainder(orig_targets, mock_role_manager):
    for group in mock_role_manager.resource_groups:
        group.target_capacity = orig_targets

    new_targets = mock_role_manager._compute_new_resource_group_targets(107)
    assert sorted(target for __, target in new_targets) == [15, 15, 15, 15, 15, 16, 16]


def test_compute_new_resource_group_targets_uneven_scale_up(mock_role_manager):
    new_targets = mock_role_manager._compute_new_resource_group_targets(304)
    assert sorted(target for __, target in new_targets) == [43, 43, 43, 43, 44, 44, 44]


def test_compute_new_resource_group_targets_uneven_scale_down(mock_role_manager):
    for group in mock_role_manager.resource_groups:
        group.target_capacity += 20

    new_targets = mock_role_manager._compute_new_resource_group_targets(10)
    assert sorted(target for __, target in new_targets) == [1, 1, 1, 1, 2, 2, 2]


def test_compute_new_resource_group_targets_above_delta_scale_up(mock_role_manager):
    new_targets = mock_role_manager._compute_new_resource_group_targets(62)
    assert sorted(target for __, target in new_targets) == [7, 7, 7, 8, 9, 11, 13]


def test_compute_new_resource_group_targets_below_delta_scale_down(mock_role_manager):
    new_targets = mock_role_manager._compute_new_resource_group_targets(30)
    assert sorted(target for __, target in new_targets) == [1, 3, 5, 5, 5, 5, 6]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up(mock_role_manager):
    for group in mock_role_manager.resource_groups[3:]:
        group.target_capacity = 20

    new_targets = mock_role_manager._compute_new_resource_group_targets(100)
    assert sorted(target for __, target in new_targets) == [6, 7, 7, 20, 20, 20, 20]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down(mock_role_manager):
    for group in mock_role_manager.resource_groups[:3]:
        group.target_capacity = 1

    new_targets = mock_role_manager._compute_new_resource_group_targets(20)
    assert sorted(target for __, target in new_targets) == [1, 1, 1, 4, 4, 4, 5]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up_2(mock_role_manager):
    for group in mock_role_manager.resource_groups[3:]:
        group.target_capacity = 20

    new_targets = mock_role_manager._compute_new_resource_group_targets(145)
    assert sorted(target for __, target in new_targets) == [20, 20, 21, 21, 21, 21, 21]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down_2(mock_role_manager):
    for group in mock_role_manager.resource_groups[:3]:
        group.target_capacity = 1

    new_targets = mock_role_manager._compute_new_resource_group_targets(9)
    assert sorted(target for __, target in new_targets) == [1, 1, 1, 1, 1, 2, 2]


def test_constrain_target_capacity(mock_role_manager):
    with mock.patch('clusterman.mesos.mesos_role_manager.logger') as mock_logger:
        assert mock_role_manager._constrain_target_capacity(1000) == 345
        assert mock_role_manager._constrain_target_capacity(1) == 3
        assert mock_role_manager._constrain_target_capacity(42) == 42
        assert mock_logger.warn.call_count == 2


def test_mark_instance_for_removal_idle_markets_empty(mock_role_manager):
    with pytest.raises(MarketProtectedException):
        mock_role_manager._mark_instance_for_removal({'market-1': []}, {}, 'market-1', 1234)


@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._find_resource_group')
def test_mark_instance_for_removal_invalid_instance(mock_find_resource_group, mock_role_manager):
    mock_find_resource_group.return_value = None
    with pytest.raises(MesosRoleManagerError):
        mock_role_manager._mark_instance_for_removal({'market-1': ['asdf']}, {}, 'market-1', 1234)


@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._find_resource_group')
def test_mark_instance_for_removal_capacity_low(mock_find_resource_group, mock_role_manager):
    mock_find_resource_group.return_value.market_weight.return_value = 1000
    with pytest.raises(MarketProtectedException):
        mock_role_manager._mark_instance_for_removal({'market-1': ['asdf']}, {}, 'market-1', 1)


@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._find_resource_group')
def test_mark_instance_for_removal_group_has_one_instance(mock_find_resource_group, mock_role_manager):
    mock_resource_group = mock.Mock()
    mock_resource_group.market_weight.return_value = 7
    mock_resource_group.instances = ['asdf']
    mock_find_resource_group.return_value = mock_resource_group
    marked_instances = {mock_resource_group: []}
    with pytest.raises(ResourceGroupProtectedException):
        mock_role_manager._mark_instance_for_removal({'market-1': ['asdf']}, marked_instances, 'market-1', 1234)


@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._find_resource_group')
def test_mark_instance_for_removal(mock_find_resource_group, mock_role_manager):
    mock_resource_group = mock.Mock()
    mock_resource_group.market_weight.return_value = 7
    mock_resource_group.instances = ['asdf', 'qwerty']
    mock_find_resource_group.return_value = mock_resource_group
    marked_instances = {mock_resource_group: []}
    assert mock_role_manager._mark_instance_for_removal({'market-1': ['asdf']}, marked_instances, 'market-1', 10) == 7
    assert marked_instances[mock_resource_group] == ['asdf']


def test_find_resource_group(mock_role_manager):
    group = mock_role_manager._find_resource_group('i-3')
    assert group.id == 'sfr-3'
    group = mock_role_manager._find_resource_group('i-9')
    assert group is None


def test_get_market_capacities(mock_role_manager):
    assert mock_role_manager._get_market_capacities() == {
        'market-1': sum(i for i in range(7)),
        'market-2': sum(i * 2 for i in range(7)),
        'market-3': sum(i * 3 for i in range(7)),
    }
    assert mock_role_manager._get_market_capacities(market_filter='market-2') == {
        'market-2': sum(i * 2 for i in range(7)),
    }


def test_target_capacity(mock_role_manager):
    assert mock_role_manager.target_capacity == sum(2 * i + 1 for i in range(7))


def test_fulfilled_capacity(mock_role_manager):
    assert mock_role_manager.fulfilled_capacity == sum(i * 6 for i in range(7))


@mock_ec2
def test_idle_agents_by_market(mock_role_manager):
    reservations = ec2.run_instances(ImageId='ami-foobar', MinCount=3, MaxCount=3, InstanceType='t2.nano')
    agents_list = [
        {'agent_info': {'hostname': instance['PrivateIpAddress']}}
        for instance in reservations['Instances']
    ]
    agents_list.append({'agent_info': {'hostname': '12.34.56.78'}})  # This IP doesn't exist for AWS
    mock_agents = mock.PropertyMock(return_value=agents_list)

    with mock.patch('clusterman.mesos.mesos_role_manager.socket.gethostbyname', lambda x: x), \
            mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._agents', mock_agents), \
            mock.patch('clusterman.mesos.mesos_role_manager.allocated_cpu_resources') as mock_cpu:
        mock_cpu.side_effect = [0, 1, 0, 0]  # Three idle instances, but one AWS doesn't know about
        idle_agents_by_market = mock_role_manager._idle_agents_by_market()
        assert(len(list(idle_agents_by_market.values())[0]) == 2)


@mock.patch('clusterman.mesos.mesos_role_manager.requests.post')
class TestAgentListing:
    def test_agent_list_error(self, mock_post, mock_role_manager):
        mock_post.return_value.ok = False
        mock_post.return_value.text = 'dummy error'
        with pytest.raises(MesosRoleManagerError):
            mock_role_manager._agents

    def test_filter_roles(self, mock_post, mock_agents_dict, mock_role_manager):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = mock_agents_dict
        agents = mock_role_manager._agents
        assert len(agents) == 1
        assert agents[0]['agent_info']['hostname'] == 'im-in-the-role.yelpcorp.com'

        # Multiple calls should have the same result.
        assert agents == mock_role_manager._agents
        assert mock_post.call_count == 2  # cache expires immediately in tests


class TestResources:
    @pytest.fixture
    def mock_agents(self, mock_role_manager):
        with mock.patch(
            'clusterman.mesos.mesos_role_manager.MesosRoleManager._agents',
            new_callable=mock.PropertyMock
        ) as mock_agents:
            mock_agents.return_value = [
                {
                    'agent_info': {
                        'id': {'value': 'idle'},
                    },
                    'total_resources': [
                        {'name': 'cpus', 'scalar': {'value': 4}, 'type': 'SCALAR'},
                        {'name': 'gpus', 'scalar': {'value': 2}, 'type': 'SCALAR'},
                    ],
                },
                {
                    'agent_info': {
                        'id': {'value': 'no-gpus'},
                    },
                    'total_resources': [
                        {'name': 'cpus', 'scalar': {'value': 8}, 'type': 'SCALAR'},
                    ],
                    'allocated_resources': [
                        {'name': 'cpus', 'scalar': {'value': 1.5}, 'type': 'SCALAR'},
                    ],
                },
                {
                    'agent_info': {
                        'id': {'value': 'gpus-1'},
                    },
                    'total_resources': [
                        {'name': 'gpus', 'scalar': {'value': 2}, 'type': 'SCALAR'},
                    ],
                    'allocated_resources': [
                        {'name': 'gpus', 'scalar': {'value': 1}, 'type': 'SCALAR'},
                    ],
                },
                {
                    'agent_info': {
                        'id': {'value': 'gpus-2'},
                    },
                    'total_resources': [
                        {'name': 'gpus', 'scalar': {'value': 4}, 'type': 'SCALAR'},
                    ],
                    'allocated_resources': [
                        {'name': 'gpus', 'scalar': {'value': 0.2}, 'type': 'SCALAR'},
                    ],
                },
            ]
            yield mock_role_manager

    @pytest.mark.parametrize('resource_name,expected', [
        ('cpus', 1.5),
        ('gpus', 1.2),
    ])
    def test_allocation(self, mock_agents, resource_name, expected):
        assert mock_agents.get_resource_allocation(resource_name) == expected

    @pytest.mark.parametrize('resource_name,expected', [
        ('cpus', 12),
        ('gpus', 8),
    ])
    def test_total_cpus(self, mock_agents, resource_name, expected):
        assert mock_agents.get_resource_total(resource_name) == expected

    @pytest.mark.parametrize('resource_name,expected', [
        ('mem', 0),
        ('cpus', 0.125),
        ('gpus', 0.15),
    ])
    def test_average_allocation(self, mock_agents, resource_name, expected):
        assert mock_agents.get_average_resource_allocation(resource_name) == expected
