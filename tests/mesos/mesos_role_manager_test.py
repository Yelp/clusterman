import mock
import pytest
import staticconf.testing
from moto import mock_ec2

from clusterman.aws.client import ec2
from clusterman.exceptions import MarketProtectedException
from clusterman.exceptions import MesosRoleManagerError
from clusterman.exceptions import ResourceGroupProtectedException
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.mesos_role_manager import NAMESPACE
from clusterman.mesos.mesos_role_manager import SERVICES_FILE
from tests.mesos.conftest import mock_open


@pytest.fixture
def mock_config_file():
    mock_config = {
        'defaults': {
            'min_capacity': 3,
            'max_capacity': 345,
        },
        'mesos': {
            'master_discovery': 'the.mesos.master',
            'resource_groups': {
                's3': {
                    'bucket': 'dummy-bucket',
                    'prefix': 'nowhere',
                }
            }
        }
    }

    with staticconf.testing.MockConfiguration(mock_config, namespace=NAMESPACE):
        yield


@pytest.fixture
def mock_resource_groups():
    return [
        mock.Mock(
            id=f'sfr-{i}',
            instances=f'i-{i}',
            target_capacity=i * 2,
            fulfilled_capacity=i,
            market_capacities={'market-1': i, 'market-2': i * 2, 'market-3': i * 3},
        )
        for i in range(7)
    ]


@pytest.fixture
def mock_role_manager(mock_config_file, mock_resource_groups):
    with mock.patch('clusterman.mesos.mesos_role_manager.staticconf.YamlConfiguration'), \
            mock_open(SERVICES_FILE, 'the.mesos.master:\n  host: foo\n  port: 1234'), \
            mock.patch('clusterman.mesos.mesos_role_manager.load_spot_fleets_from_s3') as mock_load:
        mock_load.return_value = []
        manager = MesosRoleManager('baz', 'dummy-file.yaml')
        manager.resource_groups = mock_resource_groups
        return manager


def test_mesos_role_manager_init(mock_role_manager):
    assert mock_role_manager.name == 'baz'
    assert mock_role_manager.api_endpoint == 'http://foo:1234/api/v1'


def test_modify_target_capacity_no_resource_groups(mock_role_manager):
    mock_role_manager.resource_groups = []
    with pytest.raises(MesosRoleManagerError):
        mock_role_manager.modify_target_capacity(1234)


@mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager.target_capacity', mock.PropertyMock(return_value=100))
def test_modify_target_capacity(mock_role_manager):
    mock_role_manager._increase_capacity = mock.Mock()
    mock_role_manager._decrease_capacity = mock.Mock()

    mock_role_manager.modify_target_capacity(100)
    assert mock_role_manager._decrease_capacity.call_count == 0
    assert mock_role_manager._increase_capacity.call_count == 0
    mock_role_manager.modify_target_capacity(200)
    assert mock_role_manager._increase_capacity.call_count == 1
    mock_role_manager.modify_target_capacity(50)
    assert mock_role_manager._decrease_capacity.call_count == 1


@pytest.mark.parametrize('new_target_capacity', [1, 10000])
def test_change_capacity_invalid(new_target_capacity, mock_role_manager):
    with pytest.raises(MesosRoleManagerError):
        mock_role_manager._increase_capacity(new_target_capacity)
    with pytest.raises(MesosRoleManagerError):
        mock_role_manager._decrease_capacity(new_target_capacity)


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
class TestDecreaseCapacity:
    def test_decrease_capacity_no_idle_instances(self, mock_idle_agents_by_market, mock_get_market_capacities,
                                                 mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                                                 mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_find_largest_capacity_market.return_value = (None, 0)
        mock_role_manager._decrease_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 1
        assert mock_mark_instance_for_removal.call_count == 0
        assert mock_logger.warn.call_count == 2

    def test_decrease_capacity_instance_error(self, mock_idle_agents_by_market, mock_get_market_capacities,
                                              mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                                              mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_find_largest_capacity_market.return_value = ('market-1', 70)
        mock_mark_instance_for_removal.side_effect = [MesosRoleManagerError('something bad happened'), 10]
        mock_role_manager._decrease_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 2
        assert mock_mark_instance_for_removal.call_count == 2
        assert type(mock_logger.warn.call_args_list[0][0][0]) == MesosRoleManagerError

    def test_decrease_capacity_protected_market(self, mock_idle_agents_by_market, mock_get_market_capacities,
                                                mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                                                mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_get_market_capacities.return_value = {'market-1': 70, 'market-2': 30}
        mock_find_largest_capacity_market.side_effect = [('market-1', 70), ('market-2', 30)]
        mock_mark_instance_for_removal.side_effect = [MarketProtectedException('market-1 is full'), 10]
        mock_role_manager._decrease_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 2
        assert mock_mark_instance_for_removal.call_count == 2
        assert mock_get_market_capacities.return_value == {'market-2': 20}

    def test_decrease_capacity_protected_group(self, mock_idle_agents_by_market, mock_get_market_capacities,
                                               mock_mark_instance_for_removal, mock_find_largest_capacity_market,
                                               mock_logger, mock_role_manager):
        mock_idle_agents_by_market.return_value = {'market-1': ['agent-1', 'agent-2'], 'market-2': ['agent-3']}
        mock_get_market_capacities.return_value = {'market-1': 70, 'market-2': 30}
        mock_find_largest_capacity_market.side_effect = [('market-1', 70), ('market-2', 0)]
        mock_mark_instance_for_removal.side_effect = [ResourceGroupProtectedException('group-1 is full')]
        mock_role_manager._decrease_capacity(90)
        assert mock_find_largest_capacity_market.call_count == 2
        assert mock_mark_instance_for_removal.call_count == 1
        assert mock_get_market_capacities.return_value == {'market-1': 70, 'market-2': 30}


def test_compute_new_resource_group_targets_no_unfilled_capacity(mock_role_manager):
    assert mock_role_manager._compute_new_resource_group_targets(0) == [
        [i, group.target_capacity]
        for i, group in enumerate(mock_role_manager.resource_groups)
    ]


def test_compute_new_resource_group_targets_all_equal(mock_role_manager):
    for group in mock_role_manager.resource_groups:
        group.target_capacity = 10

    num_groups = len(mock_role_manager.resource_groups)
    assert mock_role_manager._compute_new_resource_group_targets(5 * num_groups) == [[i, 15] for i in range(num_groups)]


def test_compute_new_resource_group_targets_all_equal_with_remainder(mock_role_manager):
    for group in mock_role_manager.resource_groups:
        group.target_capacity = 10

    num_groups = len(mock_role_manager.resource_groups)
    assert mock_role_manager._compute_new_resource_group_targets(5 * num_groups + 2) == [
        [i, 16 if i < 2 else 15] for i in range(num_groups)
    ]


def test_compute_new_resource_group_targets_uneven(mock_role_manager):
    num_groups = len(mock_role_manager.resource_groups)
    assert mock_role_manager._compute_new_resource_group_targets(262) == [
        [i, 44 if i < 3 else 43] for i in range(num_groups)
    ]


def test_compute_new_resource_group_targets_above_delta(mock_role_manager):
    assert mock_role_manager._compute_new_resource_group_targets(10) == [
        [0, 6], [1, 5], [2, 5], [3, 6], [4, 8], [5, 10], [6, 12]
    ]


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
        mock_role_manager._mark_instance_for_removal({'market-1': ['asdf']}, {}, 'market-1', 1001)


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
    assert mock_role_manager.target_capacity == sum(2 * i for i in range(7))


def test_fulfilled_capacity(mock_role_manager):
    assert mock_role_manager.fulfilled_capacity == sum(i for i in range(7))


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
class TestAgentGenerator:
    def test_agent_generator_error(self, mock_post, mock_role_manager):
        mock_post.return_value.ok = False
        mock_post.return_value.text = 'dummy error'
        with pytest.raises(MesosRoleManagerError):
            for a in mock_role_manager._agents:
                print(a)

    def test_agent_generator_filter_roles(self, mock_post, mock_agents_dict, mock_role_manager):
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = mock_agents_dict
        agents = list(mock_role_manager._agents)
        assert len(agents) == 1
        assert agents[0]['agent_info']['hostname'] == 'im-in-the-role.yelpcorp.com'
