from argparse import Namespace

import mock
import pytest

from clusterman.mesos.manage import get_target_capacity_value
from clusterman.mesos.manage import main


@pytest.fixture
def args():
    return Namespace(
        cluster='foo',
        pool='bar',
        target_capacity='123',
        recycle=False,
        dry_run=False,
        update_ami_to_latest=None,
        update_ami_to=None
    )


def test_get_target_capacity_value_min():
    assert get_target_capacity_value('mIN', 'bar') == 3


def test_get_target_capacity_value_max():
    assert get_target_capacity_value('mAx', 'bar') == 345


def test_get_target_capacity_value_number():
    assert get_target_capacity_value('123', 'bar') == 123


def test_get_target_capacity_value_invalid():
    with pytest.raises(ValueError):
        get_target_capacity_value('asdf', 'bar')


@mock.patch('clusterman.mesos.manage.logger')
@mock.patch('clusterman.mesos.manage.ask_for_confirmation')
@mock.patch('clusterman.mesos.manage.MesosPoolManager')
@mock.patch('clusterman.mesos.manage.get_target_capacity_value')
@mock.patch('clusterman.mesos.manage._recycle_cluster')
@mock.patch('clusterman.mesos.manage.update_ami')
@mock.patch('clusterman.mesos.manage.log_to_scribe')
class TestMain:
    @pytest.mark.parametrize('dry_run', [True, False])
    def test_manage(
        self,
        mock_log_to_scribe,
        mock_update_ami,
        mock_recycle_cluster,
        mock_target_capacity,
        mock_manager,
        mock_confirm,
        mock_logger,
        args,
        dry_run
    ):
        args.dry_run = dry_run
        mock_target_capacity.return_value = 123

        main(args)
        assert mock_confirm.call_count == 0 if dry_run else 1
        assert mock_manager.return_value.modify_target_capacity.call_args == mock.call(123, dry_run)
        assert mock_manager.return_value.modify_target_capacity.call_count == 1
        assert mock_log_to_scribe.call_count == 0 if dry_run is True else 1

    def test_abort_manage(
        self,
        mock_log_to_scribe,
        mock_update_ami,
        mock_recycle_cluster,
        mock_target_capacity,
        mock_manager,
        mock_confirm,
        mock_logger,
        args
    ):
        mock_target_capacity.return_value = 123
        mock_confirm.return_value = False

        main(args)
        assert mock_confirm.call_count == 1
        assert mock_manager.return_value.modify_target_capacity.call_count == 0
        assert mock_log_to_scribe.call_count == 0

    @pytest.mark.parametrize('dry_run', [True, False])
    def test_update_ami_to_latest(
        self,
        mock_log_to_scribe,
        mock_update_ami,
        mock_recycle_cluster,
        mock_target_capacity,
        mock_manager,
        mock_confirm,
        mock_logger,
        args,
        dry_run
    ):
        args.target_capacity = None
        args.dry_run = dry_run
        args.update_ami_to_latest = 'test_ami'

        with mock.patch('clusterman.mesos.manage.get_latest_ami', return_value='abc-123'):
            main(args)

        if dry_run is False:
            assert mock_update_ami.call_count == 1
            assert mock_update_ami.call_args == mock.call('abc-123', 'foo', 'bar')
            assert mock_log_to_scribe.call_count == 1
        else:
            assert mock_update_ami.call_count == 0
            assert mock_log_to_scribe.call_count == 0

    @pytest.mark.parametrize('dry_run', [True, False])
    def test_update_ami_to(
        self,
        mock_log_to_scribe,
        mock_update_ami,
        mock_recycle_cluster,
        mock_target_capacity,
        mock_manager,
        mock_confirm,
        mock_logger,
        args,
        dry_run
    ):
        args.target_capacity = None
        args.dry_run = dry_run
        args.update_ami_to = 'abc-123'

        main(args)

        if dry_run is False:
            assert mock_update_ami.call_count == 1
            assert mock_update_ami.call_args == mock.call('abc-123', 'foo', 'bar')
            assert mock_log_to_scribe.call_count == 1
        else:
            assert mock_update_ami.call_count == 0
            assert mock_log_to_scribe.call_count == 0

    def test_recycle_cluster(
        self,
        mock_log_to_scribe,
        mock_update_ami,
        mock_recycle_cluster,
        mock_target_capacity,
        mock_manager,
        mock_confirm,
        mock_logger,
        args,
    ):
        args.target_capacity = None
        args.recycle = True

        main(args)

        assert mock_recycle_cluster.call_count == 1
        assert mock_log_to_scribe.call_count == 0
