import mock
import pytest

from clusterman.spotinst.utils import create_new_eg
from clusterman.spotinst.utils import update_ami


def test_update_ami_no_eg_found():
    mock_client = mock.Mock()
    with mock.patch('clusterman.spotinst.utils.get_spotinst_client', return_value=mock_client) as mock_get_spotinst_client:
        with mock.patch('clusterman.spotinst.utils.load_elastigroups', return_value=[]) as mock_load_elastigroups:
            update_ami('abc-123', 'foo', 'bar')

    assert mock_get_spotinst_client.call_count == 1
    assert mock_load_elastigroups.call_count == 1
    assert mock_client.update_elastigroup.call_count == 0


def test_update_ami_eg_found():
    mock_client = mock.Mock()
    with mock.patch('clusterman.spotinst.utils.get_spotinst_client', return_value=mock_client) as mock_get_spotinst_client:
        with mock.patch('clusterman.spotinst.utils.load_elastigroups', return_value=[mock.Mock()]) as mock_load_elastigroups:
            update_ami('abc-123', 'foo', 'bar')

    assert mock_get_spotinst_client.call_count == 1
    assert mock_load_elastigroups.call_count == 1
    assert mock_client.update_elastigroup.call_count == 1


def test_create_new_eg_ami_type_and_ami_id_specified():
    mock_client = mock.Mock()
    mock_config = {
        'compute': {
            'launchSpecification': {
                'imageId': 'abc-123',
                'amiType': 'paasta_hvm'
            }

        }

    }

    with mock.patch('clusterman.spotinst.utils.get_spotinst_client', return_value=mock_client) as mock_get_spotinst_client:
        with mock.patch('clusterman.spotinst.utils.get_latest_ami') as mock_get_latest_ami:
            with pytest.raises(Exception):
                create_new_eg('mock_name', mock_config)

    assert mock_get_spotinst_client.call_count == 1
    assert mock_get_latest_ami.call_count == 0
    assert mock_client.create_elastigroup.call_count == 0


def test_create_new_eg_neither_ami_type_nor_ami_id_specified():
    mock_client = mock.Mock()
    mock_config = {'compute': {'launchSpecification': {}}}

    with mock.patch('clusterman.spotinst.utils.get_spotinst_client', return_value=mock_client) as mock_get_spotinst_client:
        with mock.patch('clusterman.spotinst.utils.get_latest_ami') as mock_get_latest_ami:
            with pytest.raises(Exception):
                create_new_eg('mock_name', mock_config)

    assert mock_get_spotinst_client.call_count == 1
    assert mock_get_latest_ami.call_count == 0
    assert mock_client.create_elastigroup.call_count == 0
