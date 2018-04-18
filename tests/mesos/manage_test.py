import sys
from argparse import Namespace

import mock
import pytest

from clusterman.mesos.manage import get_target_capacity_value
from clusterman.mesos.manage import main


@pytest.fixture
def args():
    return Namespace(cluster='foo', pool='bar', target_capacity='123', recycle=False, dry_run=False)


@pytest.fixture(autouse=True)
def mock_clog():
    sys.modules['clog'] = mock.Mock()  # clog is imported in the main function so this is how we mock it
    yield


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
class TestMain:
    @pytest.mark.parametrize('dry_run', [True, False])
    def test_manage(self, mock_target_capacity, mock_manager, mock_confirm, mock_logger, args, dry_run):
        args.dry_run = dry_run
        mock_target_capacity.return_value = 123

        main(args)
        assert mock_confirm.call_count == 0 if dry_run else 1
        assert sys.modules['clog'].log_line.call_count == 0 if dry_run else 1
        assert mock_manager.return_value.modify_target_capacity.call_args == mock.call(123, dry_run)
        assert mock_manager.return_value.modify_target_capacity.call_count == 1

    def test_abort_manage(self, mock_target_capacity, mock_manager, mock_confirm, mock_logger, args):
        mock_target_capacity.return_value = 123
        mock_confirm.return_value = False

        main(args)
        assert mock_confirm.call_count == 1
        assert sys.modules['clog'].log_line.call_count == 0
        assert mock_manager.return_value.modify_target_capacity.call_count == 0
