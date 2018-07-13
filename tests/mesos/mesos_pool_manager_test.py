from contextlib import contextmanager

import mock
import pytest
from moto import mock_ec2

from clusterman.aws.client import ec2
from clusterman.aws.markets import get_instance_market
from clusterman.exceptions import AllResourceGroupsAreStaleError
from clusterman.exceptions import MesosPoolManagerError
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.mesos.mesos_pool_manager import PoolInstance
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.util import MesosAgentState
from tests.conftest import clusterman_pool_config


pytest.mark.usefixtures(clusterman_pool_config)


@pytest.fixture
def mock_resource_groups():
    return [
        mock.Mock(
            id=f'sfr-{i}',
            instance_ids=[f'i-{i}'],
            target_capacity=i * 2 + 1,
            fulfilled_capacity=i * 6,
            market_capacities={'market-1': i, 'market-2': i * 2, 'market-3': i * 3},
            is_stale=False,
            market_weight=mock.Mock(return_value=1.0),
        )
        for i in range(7)
    ]


@pytest.fixture
def mock_pool_manager(mock_resource_groups):
    class FakeResourceGroupClass(MesosPoolResourceGroup):

        @staticmethod
        def load(cluster, pool, config):
            return []

    with mock.patch.dict(
        'clusterman.mesos.mesos_pool_manager.RESOURCE_GROUPS',
        {"sfr": FakeResourceGroupClass}
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


@pytest.mark.parametrize('new_target,constrain_return', ((100, 90), (10, 49)))
def test_modify_target_capacity(new_target, constrain_return, mock_pool_manager):
    mock_pool_manager.prune_excess_fulfilled_capacity = mock.Mock()

    mock_pool_manager._constrain_target_capacity = mock.Mock(return_value=constrain_return)
    mock_pool_manager._compute_new_resource_group_targets = mock.Mock(return_value=[0, 1, 2, 3, 4, 5, 6])
    assert mock_pool_manager.modify_target_capacity(new_target) == constrain_return
    assert mock_pool_manager._constrain_target_capacity.call_count == 1
    assert mock_pool_manager.prune_excess_fulfilled_capacity.call_count == 1
    assert mock_pool_manager._compute_new_resource_group_targets.call_count == 1
    for i, group in enumerate(mock_pool_manager.resource_groups):
        assert group.modify_target_capacity.call_count == 1
        assert group.modify_target_capacity.call_args[0][0] == i


@mock.patch('clusterman.mesos.mesos_pool_manager.logger')
@mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager.get_prioritized_killable_instances')
@mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager.non_orphan_fulfilled_capacity',
            mock.PropertyMock(return_value=100))
@mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager.target_capacity', mock.PropertyMock(return_value=50))
@mock.patch(
    'clusterman.mesos.mesos_pool_manager.MesosPoolManager.fulfilled_capacity',
    mock.PropertyMock(return_value=100),
)
class TestPruneFulfilledCapacity:

    def create_pool_instance(self, **kwargs):
        base_attributes = {
            'instance_id': 'instance-1',
            'state': MesosAgentState.RUNNING,
            'task_count': 0,
            'market': 'market-1',
            'instance_dict': {},
            'agent': {},
            'resource_group': mock.Mock(is_stale=False),
        }
        base_attributes.update(**kwargs)
        return PoolInstance(**base_attributes)

    def test_no_killable_instances(self, mock_get_prioritized_killable_instances, mock_logger,
                                   mock_pool_manager):
        mock_get_prioritized_killable_instances.return_value = []
        assert not mock_pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=mock_pool_manager.target_capacity)

    def test_protected_group(self, mock_get_prioritized_killable_instances, mock_logger,
                             mock_pool_manager):
        mock_get_prioritized_killable_instances.return_value = [
            self.create_pool_instance(resource_group=mock_pool_manager.resource_groups[6]),
        ]
        res_group = mock_pool_manager.resource_groups[6]
        res_group.market_weight.return_value = 10000
        assert not mock_pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=mock_pool_manager.target_capacity)

    def test_can_prune(self, mock_get_prioritized_killable_instances, mock_logger,
                       mock_pool_manager):
        mock_get_prioritized_killable_instances.return_value = [
            self.create_pool_instance(
                instance_id='instance-1',
                task_count=0,
                market='market-1',
                resource_group=mock_pool_manager.resource_groups[6],
            ),
            self.create_pool_instance(
                instance_id='instance-2',
                task_count=0,
                market='market-1',
                resource_group=mock_pool_manager.resource_groups[6],
            ),
            self.create_pool_instance(
                instance_id='instance-3',
                task_count=0,
                market='market-2',
                resource_group=mock_pool_manager.resource_groups[6],
            ),
        ]
        res_group = mock_pool_manager.resource_groups[6]
        res_group.market_weight.return_value = 1
        res_group.terminate_instances_by_id.side_effect = lambda x: x
        assert set(mock_pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=mock_pool_manager.target_capacity)) == {
            'instance-1',
            'instance-2',
            'instance-3',
        }

    def test_max_tasks_to_kill(self, mock_get_prioritized_killable_instances, mock_logger,
                               mock_pool_manager):
        mock_pool_manager.max_tasks_to_kill = 10
        mock_get_prioritized_killable_instances.return_value = [
            self.create_pool_instance(
                instance_id='instance-1',
                task_count=4,
                market='market-1',
                resource_group=mock_pool_manager.resource_groups[6],
            ),
            self.create_pool_instance(
                instance_id='instance-2',
                task_count=5,
                market='market-2',
                resource_group=mock_pool_manager.resource_groups[6],
            ),
            self.create_pool_instance(
                instance_id='instance-3',
                task_count=6,
                market='market-1',
                resource_group=mock_pool_manager.resource_groups[6],
            ),
        ]
        res_group = mock_pool_manager.resource_groups[6]
        res_group.market_weight.return_value = 1
        res_group.terminate_instances_by_id.side_effect = lambda x: x
        assert set(mock_pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=mock_pool_manager.target_capacity)) == {
            'instance-1',
            'instance-2',
        }

    def test_nothing_to_prune(self, mock_get_killable_instance_in_kill_order, mock_logger,
                              mock_pool_manager):
        # Override global PropertyMock above
        with mock.patch(
            'clusterman.mesos.mesos_pool_manager.MesosPoolManager.fulfilled_capacity',
            mock.PropertyMock(return_value=10),
        ):
            mock_pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=mock_pool_manager.target_capacity)

        for group in mock_pool_manager.resource_groups:
            assert group.terminate_instances_by_id.call_count == 0


class TestChooseInstancesToPrune:
    def make_resource_groups_and_instances(
        self,
        count,
        id_prefix,
        is_stale,
        fulfilled_capacity,
        target_capacity,
        orphaned_instances=0,
    ):
        resource_groups = [
            mock.Mock(
                id=f'sfr-{id_prefix}{i}',
                instance_ids=[f'i-{id_prefix}{i}{j}' for j in range(fulfilled_capacity)],
                target_capacity=0 if is_stale else target_capacity,
                fulfilled_capacity=fulfilled_capacity,
                market_capacities={'market-1': fulfilled_capacity},
                is_stale=True,
                market_weight=mock.Mock(return_value=1.0),
            )
            for i in range(count)
        ]
        instances = []
        for resource_group in resource_groups:
            for i, instance_id in enumerate(resource_group.instance_ids):
                orphaned = (len(instances) < orphaned_instances)
                instances.append(PoolInstance(
                    instance_id=instance_id,
                    state=MesosAgentState.ORPHANED if orphaned else MesosAgentState.RUNNING,
                    task_count=0 if orphaned else 5,
                    market=f'market-1',
                    instance_dict={
                        'InstanceId': instance_id,
                    },
                    agent={'id': instance_id},  # this should actually be something else but ¯\_(ツ)_/¯
                    resource_group=resource_group,
                ))

        return resource_groups, instances

    @pytest.mark.parametrize('num_new_instances_up', range(10))
    def test_dont_kill_stale_instances_until_nonstale_instances_are_up(
        self,
        mock_pool_manager,
        num_new_instances_up
    ):
        stale_resource_groups, stale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='stale',
            is_stale=True,
            fulfilled_capacity=3,
            target_capacity=0,
        )
        nonstale_rgs, nonstale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='nonstale',
            is_stale=False,
            fulfilled_capacity=3,
            target_capacity=3,
            orphaned_instances=3 * 3 - num_new_instances_up,
        )

        mock_pool_manager.resource_groups = stale_resource_groups + nonstale_rgs
        mock_pool_manager.get_instances = mock.Mock(
            return_value=stale_instances + nonstale_instances,
        )
        mock_pool_manager.max_tasks_to_kill = float('Inf')  # Only test the stale & orphaned logic.

        instances_to_prune = mock_pool_manager.choose_instances_to_prune(
            new_target_capacity=sum(g.target_capacity for g in nonstale_rgs),
            group_targets=None,
        )

        # Since the total fulfilled capacity on stale resource groups is equal to the total target_capacity, we should kill exactly
        # as many stale instances as we have nonstale instances up & in mesos (non-orphaned).
        assert sum(len(instance_ids) for instance_ids in instances_to_prune.values()) == num_new_instances_up
        # All instances to kill should belong to stale resource groups.
        assert set(instances_to_prune.keys()) <= set(stale_resource_groups)

    @pytest.mark.parametrize('num_new_instances_up', range(10))
    def test_scale_down_while_we_have_both_stale_and_nonstale(
        self,
        mock_pool_manager,
        num_new_instances_up
    ):
        """This tests the scenario where we've recently re-created the SFRs, but then clusterman has decided to scale down. We
        start with 9 stale boxes (all running) among 3 RGs, and 9 nonstale boxes among 3 RGs, of which num_new_instances_are_up are
        up. We set up the new RGs to have target_capacity of 2 each to indicate that we want to scale down."""
        stale_resource_groups, stale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='stale',
            is_stale=True,
            fulfilled_capacity=3,
            target_capacity=0,
        )
        nonstale_rgs, nonstale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='nonstale',
            is_stale=False,
            fulfilled_capacity=3,
            target_capacity=2,
            orphaned_instances=3 * 3 - num_new_instances_up,
        )

        mock_pool_manager.resource_groups = stale_resource_groups + nonstale_rgs
        mock_pool_manager.get_instances = mock.Mock(
            return_value=stale_instances + nonstale_instances,
        )
        mock_pool_manager.max_tasks_to_kill = float('Inf')  # Only test the stale & orphaned logic.

        instances_to_prune = mock_pool_manager.choose_instances_to_prune(
            new_target_capacity=sum(g.target_capacity for g in nonstale_rgs),
            group_targets=None,
        )
        # Since the total fulfilled capacity on stale resource groups is more than the total target_capacity, we should kill a few
        # stale instances. (But we can only kill as many stale instances as we have, hence the min)
        expected_stale_instances_to_kill = min(len(stale_instances), 3 + num_new_instances_up)
        assert expected_stale_instances_to_kill == sum(
            len(instances_to_prune[stale_rg]) for stale_rg in stale_resource_groups
        )

        # We should also expect to kill some new instances. Calculating the exact number is tricky:
        # We should see at most 1 instance killed per nonstale_rg, since fulfilled_capacity-target_capacity==1
        # We can only kill RUNNING instances if we have at least 6 (2 per RG) other nonstale RUNNING instances.
        new_running_killed_so_far = 0
        max_new_running_to_kill = num_new_instances_up - 6
        for nonstale_rg in nonstale_rgs:
            if any(i.state == MesosAgentState.ORPHANED and i.resource_group == nonstale_rg for i in nonstale_instances):
                # Depending on num_new_instances_up, some RGs will have orphans and some will not.
                # This RG has at least one orphan, and we will kill one of them.
                assert len(instances_to_prune[nonstale_rg]) == 1
                assert instances_to_prune[nonstale_rg][0] in {
                    i.instance_id for i in nonstale_instances if i.state == MesosAgentState.ORPHANED
                }
            else:
                # This RG has no orphans, but we may kill a running new instance from this RG, as long as we have at least 6 other
                # running instances.
                if new_running_killed_so_far < max_new_running_to_kill:
                    new_running_killed_so_far += 1
                    assert len(instances_to_prune[nonstale_rg]) == 1
                    assert instances_to_prune[nonstale_rg][0] in {
                        i.instance_id for i in nonstale_instances if i.state == MesosAgentState.RUNNING
                    }
                else:
                    # We've already killed enough running instances, so we will not kill anything from this RG.
                    assert len(instances_to_prune[nonstale_rg]) == 0

    @pytest.mark.parametrize('num_new_instances_up', range(10))
    def test_still_kill_fewer_stale_instances_if_scaling_up(
        self,
        mock_pool_manager,
        num_new_instances_up
    ):
        stale_resource_groups, stale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='stale',
            is_stale=True,
            fulfilled_capacity=3,
            target_capacity=0,
        )
        nonstale_rgs, nonstale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='nonstale',
            is_stale=False,
            fulfilled_capacity=4,
            target_capacity=4,
            orphaned_instances=3 * 4 - num_new_instances_up,
        )

        mock_pool_manager.resource_groups = stale_resource_groups + nonstale_rgs
        mock_pool_manager.get_instances = mock.Mock(
            return_value=stale_instances + nonstale_instances,
        )
        mock_pool_manager.max_tasks_to_kill = float('Inf')  # Only test the stale & orphaned logic.

        instances_to_prune = mock_pool_manager.choose_instances_to_prune(
            new_target_capacity=sum(g.target_capacity for g in nonstale_rgs),
            group_targets=None,
        )
        # Since the total fulfilled capacity on stale resource groups is less than the total capacity
        assert max(0, num_new_instances_up - 3) == sum(
            len(instances_to_prune[stale_rg]) for stale_rg in stale_resource_groups
        )
        assert 0 == sum(
            len(instances_to_prune[nonstale_rg]) for nonstale_rg in nonstale_rgs
        )

    def test_dont_kill_everything_when_all_rgs_are_stale(
        self,
        mock_pool_manager,
    ):
        stale_resource_groups, stale_instances = self.make_resource_groups_and_instances(
            count=3,
            id_prefix='stale',
            is_stale=True,
            fulfilled_capacity=3,
            target_capacity=0,
        )

        mock_pool_manager.resource_groups = stale_resource_groups
        mock_pool_manager.get_instances = mock.Mock(
            return_value=stale_instances,
        )
        mock_pool_manager.max_tasks_to_kill = float('Inf')  # Only test the stale & orphaned logic.
        instances_to_prune = mock_pool_manager.choose_instances_to_prune(
            new_target_capacity=9,
            group_targets=None,
        )
        assert instances_to_prune == {}


def test_compute_new_resource_group_targets_no_unfilled_capacity(mock_pool_manager):
    assert mock_pool_manager._compute_new_resource_group_targets(mock_pool_manager.target_capacity) == [
        group.target_capacity
        for group in (mock_pool_manager.resource_groups)
    ]


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal(orig_targets, mock_pool_manager):
    for group in mock_pool_manager.resource_groups:
        group.target_capacity = orig_targets

    num_groups = len(mock_pool_manager.resource_groups)
    new_targets = mock_pool_manager._compute_new_resource_group_targets(105)
    assert sorted(new_targets) == [15] * num_groups


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal_with_remainder(orig_targets, mock_pool_manager):
    for group in mock_pool_manager.resource_groups:
        group.target_capacity = orig_targets

    new_targets = mock_pool_manager._compute_new_resource_group_targets(107)
    assert sorted(new_targets) == [15, 15, 15, 15, 15, 16, 16]


def test_compute_new_resource_group_targets_uneven_scale_up(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(304)
    assert sorted(new_targets) == [43, 43, 43, 43, 44, 44, 44]


def test_compute_new_resource_group_targets_uneven_scale_down(mock_pool_manager):
    for group in mock_pool_manager.resource_groups:
        group.target_capacity += 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(10)
    assert sorted(new_targets) == [1, 1, 1, 1, 2, 2, 2]


def test_compute_new_resource_group_targets_above_delta_scale_up(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(62)
    assert sorted(new_targets) == [7, 7, 7, 8, 9, 11, 13]


def test_compute_new_resource_group_targets_below_delta_scale_down(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(30)
    assert sorted(new_targets) == [1, 3, 5, 5, 5, 5, 6]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up(mock_pool_manager):
    for group in mock_pool_manager.resource_groups[3:]:
        group.target_capacity = 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(100)
    assert sorted(new_targets) == [6, 7, 7, 20, 20, 20, 20]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down(mock_pool_manager):
    for group in mock_pool_manager.resource_groups[:3]:
        group.target_capacity = 1

    new_targets = mock_pool_manager._compute_new_resource_group_targets(20)
    assert sorted(new_targets) == [1, 1, 1, 4, 4, 4, 5]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up_2(mock_pool_manager):
    for group in mock_pool_manager.resource_groups[3:]:
        group.target_capacity = 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(145)
    assert sorted(new_targets) == [20, 20, 21, 21, 21, 21, 21]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down_2(mock_pool_manager):
    for group in mock_pool_manager.resource_groups[:3]:
        group.target_capacity = 1

    new_targets = mock_pool_manager._compute_new_resource_group_targets(9)
    assert sorted(new_targets) == [1, 1, 1, 1, 1, 2, 2]


def test_compute_new_resource_group_targets_all_rgs_are_stale(mock_pool_manager):
    for group in mock_pool_manager.resource_groups:
        group.is_stale = True

    with pytest.raises(AllResourceGroupsAreStaleError):
        mock_pool_manager._compute_new_resource_group_targets(9)


@pytest.mark.parametrize('non_stale_capacity', [1, 5])
def test_compute_new_resource_group_targets_scale_up_stale_pools_0(non_stale_capacity, mock_pool_manager):
    for group in mock_pool_manager.resource_groups[:3]:
        group.target_capacity = non_stale_capacity
    for group in mock_pool_manager.resource_groups[3:]:
        group.target_capacity = 3
        group.is_stale = True

    new_targets = mock_pool_manager._compute_new_resource_group_targets(6)
    assert new_targets == [2, 2, 2, 0, 0, 0, 0]


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


def test_get_market_capacities(mock_pool_manager):
    assert mock_pool_manager._get_market_capacities() == {
        'market-1': sum(i for i in range(7)),
        'market-2': sum(i * 2 for i in range(7)),
        'market-3': sum(i * 3 for i in range(7)),
    }
    assert mock_pool_manager._get_market_capacities(market_filter='market-2') == {
        'market-2': sum(i * 2 for i in range(7)),
    }


def test_target_capacity(mock_pool_manager):
    assert mock_pool_manager.target_capacity == sum(2 * i + 1 for i in range(7))


def test_fulfilled_capacity(mock_pool_manager):
    assert mock_pool_manager.fulfilled_capacity == sum(i * 6 for i in range(7))


class TestGetInstances:

    @pytest.fixture(autouse=True)
    def setup_ec2(self):
        mock_ec2().start()
        yield
        mock_ec2().stop()

    @pytest.fixture
    def mock_pool_manager(self, mock_pool_manager):
        reservations = ec2.run_instances(ImageId='ami-blargh', MinCount=4, MaxCount=4, InstanceType='t2.nano')
        instance_ids = [i['InstanceId'] for i in reservations['Instances']]
        mock_pool_manager.resource_groups = [
            mock.Mock(id=1, instance_ids=instance_ids[0:2]),
            mock.Mock(id=2, instance_ids=instance_ids[2:]),
        ]

        agents = []
        tasks = []

        mock_pool_manager.resource_groups[0].instance_ids[0]  # orphaned

        mock_pool_manager.resource_groups[0].instance_ids[1]  # idle
        agents.append({
            'pid': f'slave(1)@{reservations["Instances"][1]["PrivateIpAddress"]}:1',
            'id': 'idle',
            'used_resources': {'cpus': 0}
        })

        mock_pool_manager.resource_groups[1].instance_ids[0]  # few tasks
        agents.append({
            'pid': f'slave(1)@{reservations["Instances"][2]["PrivateIpAddress"]}:1',
            'id': 'few_tasks',
            'used_resources': {'cpus': 1}
        })
        tasks.extend([{'slave_id': 'few_tasks', 'state': 'TASK_RUNNING'} for _ in range(3)])

        mock_pool_manager.resource_groups[1].instance_ids[0]  # many tasks
        agents.append({
            'pid': f'slave(1)@{reservations["Instances"][3]["PrivateIpAddress"]}:1',
            'id': 'many_tasks',
            'used_resources': {'cpus': 1}
        })
        tasks.extend([{'slave_id': 'many_tasks', 'state': 'TASK_RUNNING'} for _ in range(8)])

        mock_agents = mock.PropertyMock(return_value=agents)
        mock_tasks = mock.PropertyMock(return_value=tasks)
        with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._agents', mock_agents), \
                mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._tasks', mock_tasks):
            yield mock_pool_manager

    def build_mock_pool_instance(self, state, task_count, market):
        return PoolInstance(
            instance_id=mock.ANY,
            state=state,
            task_count=task_count,
            market=market,
            instance_dict=mock.ANY,
            agent=mock.ANY,
            resource_group=mock.ANY,
        )

    def test_get_instances(self, mock_pool_manager):
        pool_instances_by_resource_group = mock_pool_manager.get_instances_by_resource_group()
        pool_instances = mock_pool_manager.get_instances()

        assert len(pool_instances_by_resource_group) == 2
        assert len(pool_instances) == 4

        group_1_pool_instances = pool_instances_by_resource_group[1]
        assert len(group_1_pool_instances) == 2
        group_2_pool_instances = pool_instances_by_resource_group[2]
        assert len(group_2_pool_instances) == 2

        orphan_pool_instance = next(filter(lambda i: i.agent is None, group_1_pool_instances))
        idle_pool_instance = next(filter(lambda i: i.agent is not None, group_1_pool_instances))
        few_tasks_pool_instance = next(filter(lambda i: i.agent['id'] == 'few_tasks', group_2_pool_instances))
        many_tasks_pool_instance = next(filter(lambda i: i.agent['id'] == 'many_tasks', group_2_pool_instances))

        instance_ids = {i.instance_id for i in pool_instances}
        assert instance_ids == {
            orphan_pool_instance.instance_id,
            idle_pool_instance.instance_id,
            few_tasks_pool_instance.instance_id,
            many_tasks_pool_instance.instance_id,
        }

        assert orphan_pool_instance == self.build_mock_pool_instance(
            state=MesosAgentState.ORPHANED,
            task_count=0,
            market=get_instance_market(orphan_pool_instance.instance_dict),
        )
        assert orphan_pool_instance.agent is None

        assert idle_pool_instance == self.build_mock_pool_instance(
            state=MesosAgentState.IDLE,
            task_count=0,
            market=get_instance_market(idle_pool_instance.instance_dict),
        )
        assert idle_pool_instance.agent['id'] == 'idle'

        assert few_tasks_pool_instance == self.build_mock_pool_instance(
            state=MesosAgentState.RUNNING,
            task_count=3,
            market=get_instance_market(few_tasks_pool_instance.instance_dict),
        )
        assert many_tasks_pool_instance == self.build_mock_pool_instance(
            state=MesosAgentState.RUNNING,
            task_count=8,
            market=get_instance_market(many_tasks_pool_instance.instance_dict),
        )


def test_get_unknown_instance(mock_pool_manager):
    with mock.patch(
        'clusterman.mesos.mesos_pool_manager.ec2_describe_instances',
    ) as mock_describe_instances, mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager._tasks', mock.PropertyMock(return_value=[]),
    ), mock.patch(
        'clusterman.mesos.mesos_pool_manager.MesosPoolManager._agents', mock.PropertyMock(return_value=[]),
    ):
        mock_pool_manager.resource_groups = [mock.Mock(instance_ids=[1])]
        mock_describe_instances.return_value = [{  # missing the 'PrivateIpAddress' key
            'InstanceId': 1,
            'InstanceType': 't2.nano',
            'State': {
                'Name': 'running',
            },
        }]

        instances = mock_pool_manager.get_instances()
        assert len(instances) == 1
        assert instances[0].state == MesosAgentState.UNKNOWN


@mock_ec2
def test_instance_kill_order(mock_pool_manager):
    reservations = ec2.run_instances(ImageId='ami-barfood', MinCount=6, MaxCount=6, InstanceType='t2.nano')
    instance_ids = [i['InstanceId'] for i in reservations['Instances']]
    mock_pool_manager.resource_groups = [
        mock.Mock(name='non_stale', instance_ids=instance_ids[:4], is_stale=False),
        mock.Mock(name='stale', instance_ids=instance_ids[4:], is_stale=True),
    ]
    mock_pool_manager.max_tasks_to_kill = 10

    agents = []
    tasks = []

    orphan_instance_id = instance_ids[0]

    idle_instance_id = instance_ids[1]
    agents.append({
        'pid': f'slave(1)@{reservations["Instances"][1]["PrivateIpAddress"]}:1',
        'id': 'idle',
        'used_resources': {"cpus": 0},
    })

    few_tasks_instance_id = instance_ids[2]
    agents.append({
        'pid': f'slave(2)@{reservations["Instances"][2]["PrivateIpAddress"]}:1',
        'id': 'few_tasks',
        'used_resources': {"cpus": 3},
    })
    tasks.extend([{'slave_id': 'few_tasks', 'state': 'TASK_RUNNING'} for _ in range(3)])

    many_tasks_instance_id = instance_ids[3]
    agents.append({
        'pid': f'slave(3)@{reservations["Instances"][3]["PrivateIpAddress"]}:1',
        'id': 'many_tasks',
        'used_resources': {"cpus": 8},
    })
    tasks.extend([{'slave_id': 'many_tasks', 'state': 'TASK_RUNNING'} for _ in range(8)])

    few_tasks_stale_instance_id = instance_ids[4]
    agents.append({
        'pid': f'slave(4)@{reservations["Instances"][4]["PrivateIpAddress"]}:1',
        'id': 'few_tasks_stale',
        'used_resources': {"cpus": 3},
    })
    tasks.extend([{'slave_id': 'few_tasks_stale', 'state': 'TASK_RUNNING'} for _ in range(3)])

    many_tasks_stale_instance_id = instance_ids[5]
    agents.append({
        'pid': f'slave(5)@{reservations["Instances"][5]["PrivateIpAddress"]}:1',
        'id': 'many_tasks_stale',
        'used_resources': {"cpus": 8},
    })
    tasks.extend([{'slave_id': 'many_tasks_stale', 'state': 'TASK_RUNNING'} for _ in range(8)])

    mock_agents = mock.PropertyMock(return_value=agents)
    mock_tasks = mock.PropertyMock(return_value=tasks)
    with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._agents', mock_agents), \
            mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._tasks', mock_tasks):

        killable_instances = mock_pool_manager.get_prioritized_killable_instances()
        killable_instance_ids = [instance.instance_id for instance in killable_instances]
        expected_order = [
            orphan_instance_id,
            idle_instance_id,
            few_tasks_stale_instance_id,
            many_tasks_stale_instance_id,
            few_tasks_instance_id,
            many_tasks_instance_id,
        ]
        assert killable_instance_ids == expected_order


@mock_ec2
class TestInstanceKillability:

    @contextmanager
    def setup_pool_manager(self, pool_manager, has_agent, num_tasks):
        reservations = ec2.run_instances(ImageId='ami-foobar', MinCount=1, MaxCount=1, InstanceType='t2.nano')
        pool_manager.resource_groups = [
            mock.Mock(instance_ids=[i['InstanceId'] for i in reservations['Instances']]),
        ]

        tasks = []
        if has_agent:
            agents = [{'pid': f'slave(1)@{reservations["Instances"][0]["PrivateIpAddress"]}:1', 'id': 'agent_id'}]
            for _ in range(num_tasks):
                tasks.append({'slave_id': 'agent_id', 'state': 'TASK_RUNNING'})
        else:
            agents = []

        mock_agents = mock.PropertyMock(return_value=agents)
        mock_tasks = mock.PropertyMock(return_value=tasks)

        with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._agents', mock_agents), \
                mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._tasks', mock_tasks):
            yield

    def test_unknown_agent_state_is_not_killable(self, mock_pool_manager):
        with mock.patch(
            'clusterman.mesos.mesos_pool_manager.MesosPoolManager._get_instance_state'
        ) as mock_get_instance_state, self.setup_pool_manager(mock_pool_manager, has_agent=False, num_tasks=0):
            mock_get_instance_state.return_value = MesosAgentState.UNKNOWN
            killable_instances = mock_pool_manager.get_prioritized_killable_instances()
            assert len(killable_instances) == 0

    def test_agent_with_no_tasks_is_killable(self, mock_pool_manager):
        with self.setup_pool_manager(mock_pool_manager, has_agent=True, num_tasks=0):
            mock_pool_manager.max_tasks_to_kill = 0
            killable_instances = mock_pool_manager.get_prioritized_killable_instances()
            assert len(killable_instances) == 1

            instance_id = mock_pool_manager.resource_groups[0].instance_ids[0]
            assert killable_instances[0].instance_id == instance_id

    def test_agent_with_tasks_not_killable_if_over_max_tasks_to_kill(self, mock_pool_manager):
        with self.setup_pool_manager(mock_pool_manager, has_agent=True, num_tasks=1):
            mock_pool_manager.max_tasks_to_kill = 0
            killable_instances = mock_pool_manager.get_prioritized_killable_instances()
            assert len(killable_instances) == 0

    def test_agent_with_tasks_killable_if_nonzero_max_tasks_to_kill(self, mock_pool_manager):
        with self.setup_pool_manager(mock_pool_manager, has_agent=True, num_tasks=1):
            mock_pool_manager.max_tasks_to_kill = 10
            killable_instances = mock_pool_manager.get_prioritized_killable_instances()
            assert len(killable_instances) == 1

            instance_id = mock_pool_manager.resource_groups[0].instance_ids[0]
            assert killable_instances[0].instance_id == instance_id


def test_count_tasks_by_agent(mock_pool_manager):
    tasks = [
        {'slave_id': 1, 'state': 'TASK_RUNNING'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'},
        {'slave_id': 3, 'state': 'TASK_FINISHED'},
        {'slave_id': 1, 'state': 'TASK_FAILED'},
        {'slave_id': 2, 'state': 'TASK_RUNNING'}
    ]
    mock_tasks = mock.PropertyMock(return_value=tasks)
    with mock.patch('clusterman.mesos.mesos_pool_manager.MesosPoolManager._tasks', mock_tasks):
        assert mock_pool_manager._count_tasks_per_agent() == {1: 1, 2: 2}


@mock.patch('clusterman.mesos.mesos_pool_manager.mesos_post')
class TestAgentListing:
    def test_agent_list_error(self, mock_post, mock_pool_manager):
        mock_post.side_effect = MesosPoolManagerError('dummy error')
        with pytest.raises(MesosPoolManagerError):
            mock_pool_manager._agents

    def test_filter_pools(self, mock_post, mock_agents_response, mock_pool_manager):
        mock_post.return_value = mock_agents_response
        agents = mock_pool_manager._agents
        assert len(agents) == 1
        assert agents[0]['hostname'] == 'im-in-the-pool.yelpcorp.com'

        # Multiple calls should have the same result.
        assert agents == mock_pool_manager._agents
        assert mock_post.call_count == 2  # cache expires immediately in tests


class TestResources:
    @pytest.fixture
    def mock_agents(self, mock_pool_manager):
        with mock.patch(
            'clusterman.mesos.mesos_pool_manager.MesosPoolManager._agents',
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
