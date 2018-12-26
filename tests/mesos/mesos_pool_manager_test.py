import mock
import pytest
import staticconf
import staticconf.testing
from moto import mock_ec2

from clusterman.aws.client import ec2
from clusterman.exceptions import AllResourceGroupsAreStaleError
from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.mesos_pool_manager import InstanceMetadata
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.util import MesosAgentState


def _check_metadata(metadata, state, task_count=0, batch_task_count=0):
    assert metadata.mesos_state == state
    assert metadata.task_count == task_count


def _make_metadata(rg_id, instance_id, state=MesosAgentState.RUNNING, is_stale=False, weight=1, tasks=5, batch_tasks=0):
    return InstanceMetadata(
        allocated_resources=(0, 0, 0) if state in {MesosAgentState.ORPHANED, MesosAgentState.IDLE} else (10, 0, 0),
        aws_state='running',
        group_id=rg_id,
        hostname='host1',
        instance_id=instance_id,
        instance_ip='1.2.3.4',
        is_resource_group_stale=is_stale,
        market='market-1',
        mesos_state=state,
        task_count=0 if state in {MesosAgentState.ORPHANED, MesosAgentState.IDLE} else max(tasks, batch_tasks),
        batch_task_count=batch_tasks,
        total_resources=(10, 10, 10),
        uptime=1000,
        weight=weight,
    )


@pytest.fixture
def mock_resource_groups():
    return {
        f'sfr-{i}': mock.Mock(
            id=f'sfr-{i}',
            instance_ids=[f'i-{i}'],
            target_capacity=i * 2 + 1,
            fulfilled_capacity=i * 6,
            market_capacities={'market-1': i, 'market-2': i * 2, 'market-3': i * 3},
            is_stale=False,
            market_weight=mock.Mock(return_value=1.0),
            terminate_instances_by_id=mock.Mock(return_value=[]),
            spec=MesosPoolResourceGroup,
        )
        for i in range(7)
    }


@pytest.fixture
def mock_pool_manager(mock_resource_groups):
    with mock.patch(
        'clusterman.mesos.spot_fleet_resource_group.SpotFleetResourceGroup.load',
        return_value={},
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.DrainingClient', autospec=True
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.reload_state'
    ):
        manager = MesosPoolManager('mesos-test', 'bar')
        manager.resource_groups = mock_resource_groups

        return manager


def test_mesos_pool_manager_init(mock_pool_manager):
    assert mock_pool_manager.pool == 'bar'
    assert mock_pool_manager.api_endpoint == 'http://the.mesos.leader:5050/'


def test_modify_target_capacity_no_resource_groups(mock_pool_manager):
    mock_pool_manager.resource_groups = []
    with pytest.raises(MesosPoolManagerError):
        mock_pool_manager.modify_target_capacity(1234)


@pytest.mark.parametrize('new_target,constrained_target', ((100, 90), (10, 49)))
def test_modify_target_capacity(new_target, constrained_target, mock_pool_manager):
    mock_pool_manager.prune_excess_fulfilled_capacity = mock.Mock()
    mock_pool_manager._constrain_target_capacity = mock.Mock(return_value=constrained_target)
    mock_pool_manager._compute_new_resource_group_targets = mock.Mock(return_value={f'sfr-{i}': i for i in range(7)})

    assert mock_pool_manager.modify_target_capacity(new_target) == constrained_target
    assert mock_pool_manager._constrain_target_capacity.call_count == 1
    assert mock_pool_manager.prune_excess_fulfilled_capacity.call_count == 1
    assert mock_pool_manager._compute_new_resource_group_targets.call_count == 1
    for i, group in enumerate(mock_pool_manager.resource_groups.values()):
        assert group.modify_target_capacity.call_count == 1
        assert group.modify_target_capacity.call_args[0][0] == i


class TestPruneExcessFulfilledCapacity:
    @pytest.fixture
    def mock_instances_to_prune(self):
        return {
            'sfr-1': [mock.Mock(instance_id=1)],
            'sfr-3': [mock.Mock(instance_id=4), mock.Mock(instance_id=5), mock.Mock(instance_id=6)],
        }

    @pytest.fixture
    def mock_pool_manager(self, mock_pool_manager, mock_instances_to_prune):
        mock_pool_manager._choose_instances_to_prune = mock.Mock(return_value=mock_instances_to_prune)
        mock_pool_manager.draining_client = mock.Mock()
        mock_pool_manager.terminate_instances_by_id = mock.Mock()
        return mock_pool_manager

    def test_dry_run(self, mock_pool_manager):
        mock_pool_manager.prune_excess_fulfilled_capacity(100, dry_run=True)
        assert mock_pool_manager.draining_client.submit_host_for_draining.call_count == 0
        assert mock_pool_manager.terminate_instances_by_id.call_count == 0

    def test_drain_queue(self, mock_pool_manager, mock_instances_to_prune):
        mock_pool_manager.draining_enabled = True
        mock_pool_manager.prune_excess_fulfilled_capacity(100)
        assert mock_pool_manager.draining_client.submit_host_for_draining.call_args_list == [
            mock.call(mock_instances_to_prune['sfr-1'][0], sender=MesosPoolResourceGroup),
            mock.call(mock_instances_to_prune['sfr-3'][0], sender=MesosPoolResourceGroup),
            mock.call(mock_instances_to_prune['sfr-3'][1], sender=MesosPoolResourceGroup),
            mock.call(mock_instances_to_prune['sfr-3'][2], sender=MesosPoolResourceGroup),
        ]

    def test_terminate_immediately(self, mock_pool_manager):
        mock_pool_manager.prune_excess_fulfilled_capacity(100)
        assert mock_pool_manager.resource_groups['sfr-1'].terminate_instances_by_id.call_args == mock.call([1])
        assert mock_pool_manager.resource_groups['sfr-3'].terminate_instances_by_id.call_args == mock.call([4, 5, 6])


@mock_ec2
def test_get_instance_metadatas(mock_pool_manager):
    reservations = ec2.run_instances(ImageId='ami-foo', MinCount=5, MaxCount=5, InstanceType='t2.nano')
    mock_pool_manager.resource_groups = {
        'sfr-1': mock.Mock(instance_ids=[i['InstanceId'] for i in reservations['Instances']]),
    }
    mock_pool_manager._count_batch_tasks_per_mesos_agent = mock.Mock(return_value={
        reservations['Instances'][1]['InstanceId']: 0,
        reservations['Instances'][2]['InstanceId']: 0,
        reservations['Instances'][3]['InstanceId']: 0,
        reservations['Instances'][4]['InstanceId']: 1,
    })
    mock_pool_manager._count_tasks_per_mesos_agent = mock.Mock(return_value={
        reservations['Instances'][1]['InstanceId']: 0,
        reservations['Instances'][2]['InstanceId']: 3,
        reservations['Instances'][3]['InstanceId']: 8,
        reservations['Instances'][4]['InstanceId']: 1,
    })
    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.agents',
        mock.PropertyMock(return_value=[
            {
                'pid': f'slave(1)@{reservations["Instances"][1]["PrivateIpAddress"]}:1',
                'id': f'{reservations["Instances"][1]["InstanceId"]}',
                'used_resources': {'cpus': 0},
                'hostname': 'host1',
            },
            {
                'pid': f'slave(1)@{reservations["Instances"][2]["PrivateIpAddress"]}:1',
                'id': f'{reservations["Instances"][2]["InstanceId"]}',
                'used_resources': {'cpus': 1},
                'hostname': 'host2',
            },
            {
                'pid': f'slave(1)@{reservations["Instances"][3]["PrivateIpAddress"]}:1',
                'id': f'{reservations["Instances"][3]["InstanceId"]}',
                'used_resources': {'cpus': 1},
                'hostname': 'host3',
            },
            {
                'pid': f'slave(1)@{reservations["Instances"][4]["PrivateIpAddress"]}:1',
                'id': f'{reservations["Instances"][4]["InstanceId"]}',
                'used_resources': {'cpus': 1},
                'hostname': 'host4',
            },
        ]),
    ):
        metadatas = mock_pool_manager.get_instance_metadatas()
        cancelled_metadatas = mock_pool_manager.get_instance_metadatas({'cancelled'})

    assert len(metadatas) == 5
    assert len(cancelled_metadatas) == 0
    _check_metadata(metadatas[0], state=MesosAgentState.ORPHANED)
    _check_metadata(metadatas[1], state=MesosAgentState.IDLE)
    _check_metadata(metadatas[2], state=MesosAgentState.RUNNING, task_count=3)
    _check_metadata(metadatas[3], state=MesosAgentState.RUNNING, task_count=8)
    _check_metadata(metadatas[4], state=MesosAgentState.RUNNING, task_count=1, batch_task_count=1)


def test_get_unknown_instance(mock_pool_manager):
    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.ec2_describe_instances',
    ) as mock_describe_instances, mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks', mock.PropertyMock(return_value=[]),
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.agents', mock.PropertyMock(return_value=[]),
    ):
        mock_pool_manager.resource_groups = {'rg1': mock.Mock(instance_ids=[1])}
        mock_describe_instances.return_value = [{  # missing the 'PrivateIpAddress' key
            'InstanceId': 1,
            'InstanceType': 't2.nano',
            'State': {
                'Name': 'running',
            },
            'LaunchTime': '2018-07-25T14:50:00Z',
        }]

        metadatas = mock_pool_manager.get_instance_metadatas()
        assert len(metadatas) == 1
        _check_metadata(metadatas[0], MesosAgentState.UNKNOWN)


@mock.patch('clusterman.mesos.mesos_pool_manager.logger', autospec=True)
class TestReloadResourceGroups:
    def test_malformed_config(self, mock_logger, mock_pool_manager):
        with staticconf.testing.MockConfiguration(
            {'resource_groups': ['asdf']},
            namespace='bar_config',
        ):
            mock_pool_manager.pool_config = staticconf.NamespaceReaders('bar_config')
            mock_pool_manager._reload_resource_groups()

        assert not mock_pool_manager.resource_groups
        assert 'Malformed config' in mock_logger.error.call_args[0][0]

    def test_unknown_rg_type(self, mock_logger, mock_pool_manager):
        with staticconf.testing.MockConfiguration(
            {'resource_groups': [{'fake_rg_type': 'bar'}]},
            namespace='bar_config',
        ):
            mock_pool_manager.pool_config = staticconf.NamespaceReaders('bar_config')
            mock_pool_manager._reload_resource_groups()

        assert not mock_pool_manager.resource_groups
        assert 'Unknown resource group' in mock_logger.error.call_args[0][0]

    def test_successful(self, mock_logger, mock_pool_manager):
        with mock.patch.dict(
            'clusterman.mesos.mesos_pool_manager.RESOURCE_GROUPS',
            {'sfr': mock.Mock(load=mock.Mock(return_value={'rg1': mock.Mock()}))},
        ):
            mock_pool_manager._reload_resource_groups()

        assert len(mock_pool_manager.resource_groups) == 1
        assert 'rg1' in mock_pool_manager.resource_groups


@mock.patch('clusterman.mesos.mesos_pool_manager.logger')
@pytest.mark.parametrize('force', [True, False])
class TestConstrainTargetCapacity:
    def test_positive_delta(self, mock_logger, force, mock_pool_manager):
        assert mock_pool_manager._constrain_target_capacity(100, force) == 100
        assert mock_pool_manager._constrain_target_capacity(1000, force) == (1000 if force else 249)
        mock_pool_manager.max_capacity = 97
        assert mock_pool_manager._constrain_target_capacity(1000, force) == (1000 if force else 97)
        assert mock_logger.warn.call_count == 2

    def test_negative_delta(self, mock_logger, force, mock_pool_manager):
        assert mock_pool_manager._constrain_target_capacity(40, force) == 40
        assert mock_pool_manager._constrain_target_capacity(20, force) == (20 if force else 39)
        mock_pool_manager.min_capacity = 45
        assert mock_pool_manager._constrain_target_capacity(20, force) == (20 if force else 45)
        assert mock_logger.warn.call_count == 2

    def test_zero_delta(self, mock_logger, force, mock_pool_manager):
        assert mock_pool_manager._constrain_target_capacity(49, force) == 49


@mock.patch('clusterman.mesos.mesos_pool_manager.logger', autospec=True)
class TestChooseInstancesToPrune:
    # fulfilled capacity of 126
    @pytest.fixture(autouse=True)
    def mock_nofc(self):
        with mock.patch(
            'clusterman.mesos.mesos_pool_manager.MesosPoolManager.non_orphan_fulfilled_capacity',
            mock.PropertyMock(return_value=126),
        ):
            yield

    def test_fulfilled_capacity_under_target(self, mock_logger, mock_pool_manager):
        assert mock_pool_manager._choose_instances_to_prune(300, None) == {}

    def test_no_instances_to_kill(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[])
        assert mock_pool_manager._choose_instances_to_prune(100, None) == {}

    def test_killable_instance_under_group_capacity(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=1000)
        ])
        assert mock_pool_manager._choose_instances_to_prune(100, None) == {}
        assert 'is at target capacity' in mock_logger.info.call_args[0][0]

    def test_killable_instance_too_many_tasks(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1')
        ])
        assert mock_pool_manager._choose_instances_to_prune(100, None) == {}
        assert 'would take us over our max_tasks_to_kill' in mock_logger.info.call_args[0][0]

    def test_killable_instances_under_target_capacity(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=2)
        ])
        mock_pool_manager.max_tasks_to_kill = 100
        assert mock_pool_manager._choose_instances_to_prune(125, None) == {}
        assert 'would take us under our target_capacity' in mock_logger.info.call_args[0][0]

    def test_kill_instance(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_instances = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=2)
        ])
        mock_pool_manager.max_tasks_to_kill = 100
        assert mock_pool_manager._choose_instances_to_prune(100, None)['sfr-1'][0].instance_id == 'i-1'


def test_compute_new_resource_group_targets_no_unfilled_capacity(mock_pool_manager):
    target_capacity = mock_pool_manager.target_capacity
    assert sorted(list(mock_pool_manager._compute_new_resource_group_targets(target_capacity).values())) == [
        group.target_capacity
        for group in (mock_pool_manager.resource_groups.values())
    ]


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal(orig_targets, mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.target_capacity = orig_targets

    num_groups = len(mock_pool_manager.resource_groups)
    new_targets = mock_pool_manager._compute_new_resource_group_targets(105)
    assert sorted(list(new_targets.values())) == [15] * num_groups


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal_with_remainder(orig_targets, mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.target_capacity = orig_targets

    new_targets = mock_pool_manager._compute_new_resource_group_targets(107)
    assert sorted(list(new_targets.values())) == [15, 15, 15, 15, 15, 16, 16]


def test_compute_new_resource_group_targets_uneven_scale_up(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(304)
    assert sorted(list(new_targets.values())) == [43, 43, 43, 43, 44, 44, 44]


def test_compute_new_resource_group_targets_uneven_scale_down(mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.target_capacity += 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(10)
    assert sorted(list(new_targets.values())) == [1, 1, 1, 1, 2, 2, 2]


def test_compute_new_resource_group_targets_above_delta_scale_up(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(62)
    assert sorted(list(new_targets.values())) == [7, 7, 7, 8, 9, 11, 13]


def test_compute_new_resource_group_targets_below_delta_scale_down(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(30)
    assert sorted(list(new_targets.values())) == [1, 3, 5, 5, 5, 5, 6]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[3:]:
        group.target_capacity = 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(100)
    assert sorted(list(new_targets.values())) == [6, 7, 7, 20, 20, 20, 20]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[:3]:
        group.target_capacity = 1

    new_targets = mock_pool_manager._compute_new_resource_group_targets(20)
    assert sorted(list(new_targets.values())) == [1, 1, 1, 4, 4, 4, 5]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up_2(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[3:]:
        group.target_capacity = 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(145)
    assert sorted(list(new_targets.values())) == [20, 20, 21, 21, 21, 21, 21]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down_2(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[:3]:
        group.target_capacity = 1

    new_targets = mock_pool_manager._compute_new_resource_group_targets(9)
    assert sorted(list(new_targets.values())) == [1, 1, 1, 1, 1, 2, 2]


def test_compute_new_resource_group_targets_all_rgs_are_stale(mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.is_stale = True

    with pytest.raises(AllResourceGroupsAreStaleError):
        mock_pool_manager._compute_new_resource_group_targets(9)


@pytest.mark.parametrize('non_stale_capacity', [1, 5])
def test_compute_new_resource_group_targets_scale_up_stale_pools_0(non_stale_capacity, mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[:3]:
        group.target_capacity = non_stale_capacity
    for group in list(mock_pool_manager.resource_groups.values())[3:]:
        group.target_capacity = 3
        group.is_stale = True

    new_targets = mock_pool_manager._compute_new_resource_group_targets(6)
    assert new_targets == {'sfr-0': 2, 'sfr-1': 2, 'sfr-2': 2, 'sfr-3': 0, 'sfr-4': 0, 'sfr-5': 0, 'sfr-6': 0}


def test_get_market_capacities(mock_pool_manager):
    assert mock_pool_manager.get_market_capacities() == {
        'market-1': sum(i for i in range(7)),
        'market-2': sum(i * 2 for i in range(7)),
        'market-3': sum(i * 3 for i in range(7)),
    }
    assert mock_pool_manager.get_market_capacities(market_filter='market-2') == {
        'market-2': sum(i * 2 for i in range(7)),
    }


def test_target_capacity(mock_pool_manager):
    assert mock_pool_manager.target_capacity == sum(2 * i + 1 for i in range(7))


def test_fulfilled_capacity(mock_pool_manager):
    assert mock_pool_manager.fulfilled_capacity == sum(i * 6 for i in range(7))


def test_instance_kill_order(mock_pool_manager):
    mock_pool_manager.get_instance_metadatas = mock.Mock(return_value=[
        _make_metadata('sfr-0', 'i-7', batch_tasks=100),
        _make_metadata('sfr-0', 'i-0', state=MesosAgentState.ORPHANED),
        _make_metadata('sfr-0', 'i-2', tasks=1, is_stale=True),
        _make_metadata('sfr-0', 'i-1', state=MesosAgentState.IDLE),
        _make_metadata('sfr-0', 'i-4', tasks=1),
        _make_metadata('sfr-0', 'i-5', tasks=100),
        _make_metadata('sfr-0', 'i-3', tasks=100, is_stale=True),
        _make_metadata('sfr-0', 'i-6', batch_tasks=1),
        _make_metadata('sfr-0', 'i-8', state=MesosAgentState.UNKNOWN),
        _make_metadata('sfr-0', 'i-9', tasks=100000),
    ])
    mock_pool_manager.max_tasks_to_kill = 1000
    killable_instances = mock_pool_manager._get_prioritized_killable_instances()
    killable_instance_ids = [instance.instance_id for instance in killable_instances]
    assert killable_instance_ids == [f'i-{i}' for i in range(8)]


def test_count_tasks_by_agent(mock_pool_manager):
    tasks = [
        {'slave_id': 1, 'state': 'TASK_RUNNING'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'},
        {'slave_id': 3, 'state': 'TASK_FINISHED'},
        {'slave_id': 1, 'state': 'TASK_FAILED'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'}
    ]
    mock_tasks = mock.PropertyMock(return_value=tasks)
    with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks', mock_tasks):
        assert mock_pool_manager._count_tasks_per_mesos_agent() == {1: 1, 2: 2}


def test_count_batch_tasks_by_agent(mock_pool_manager):
    tasks = [
        {'slave_id': '1', 'state': 'TASK_RUNNING', 'framework_id': '2'},
        {'slave_id': '2', 'state': 'TASK_RUNNING', 'framework_id': '2'},
        {'slave_id': '3', 'state': 'TASK_FINISHED', 'framework_id': '2'},
        {'slave_id': '1', 'state': 'TASK_FAILED', 'framework_id': '2'},
        {'slave_id': '2', 'state': 'TASK_RUNNING', 'framework_id': '1'}
    ]
    mock_tasks = mock.PropertyMock(return_value=tasks)
    mock_frameworks = mock.PropertyMock(return_value={
        'frameworks': [{'id': '1', 'name': 'chronos'}, {'id': '2', 'name': 'marathon123'}]
    })

    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.tasks', mock_tasks
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager.frameworks', mock_frameworks
    ):
        ret = mock_pool_manager._count_batch_tasks_per_mesos_agent()
        assert ret == {'2': 1}
        assert ret['1'] == 0


def test_is_batch_task(mock_pool_manager):
    mock_pool_manager.non_batch_framework_prefixes = ('marathon', 'paasta')
    framework_id_to_name = {
        '1': 'marathon123',
        '2': 'paasta123',
        '3': 'chronos',
    }
    assert mock_pool_manager._is_batch_task({'framework_id': '3'}, framework_id_to_name)
    assert not mock_pool_manager._is_batch_task({'framework_id': '2'}, framework_id_to_name)
    assert not mock_pool_manager._is_batch_task({'framework_id': '1'}, framework_id_to_name)


@mock.patch('clusterman.mesos.mesos_pool_manager.mesos_post')
class TestAgentListing:
    def test_agent_list_error(self, mock_post, mock_pool_manager):
        mock_post.side_effect = MesosPoolManagerError('dummy error')
        with pytest.raises(MesosPoolManagerError):
            mock_pool_manager.agents

    def test_filter_pools(self, mock_post, mock_agents_response, mock_pool_manager):
        mock_post.return_value = mock_agents_response
        agents = mock_pool_manager.agents
        assert len(agents) == 1
        assert agents[0]['hostname'] == 'im-in-the-pool.yelpcorp.com'

        # Multiple calls should have the same result.
        assert agents == mock_pool_manager.agents
        assert mock_post.call_count == 2  # cache expires immediately in tests


class TestResources:
    @pytest.fixture
    def mock_agents(self, mock_pool_manager):
        with mock.patch(
            'clusterman.mesos.mesos_pool_manager.MesosPoolManager.agents',
            new_callable=mock.PropertyMock
        ) as mock_agents:
            mock_agents.return_value = [
                {
                    'id': 'idle',
                    'resources': {'cpus': 4, 'gpus': 2},
                },
                {
                    'id': 'no-gpus',
                    'resources': {'cpus': 8},
                    'used_resources': {'cpus': 1.5},
                },
                {
                    'id': 'gpus-1',
                    'resources': {'gpus': 2},
                    'used_resources': {'gpus': 1},
                },
                {
                    'id': 'gpus-2',
                    'resources': {'gpus': 4},
                    'used_resources': {'gpus': 0.2},
                },
            ]
            yield mock_pool_manager

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
        assert mock_agents.get_percent_resource_allocation(resource_name) == expected
