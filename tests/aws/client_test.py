import math

import mock
import pytest
from mock import call

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import get_latest_ami
from clusterman.aws.client import MAX_PAGE_SIZE


def test_empty_instance_ids():
    assert ec2_describe_instances(instance_ids=None) == []
    assert ec2_describe_instances(instance_ids=[]) == []


def test_get_latest_ami_no_images_found():
    with mock.patch(
        'clusterman.aws.client.ec2'
    ) as mock_ec2:
        mock_ec2.describe_images = mock.Mock(
            return_value={'Images': []}
        )
        return_value = get_latest_ami('fake_ami_type')

        assert return_value is None
        mock_ec2.describe_images.call_count == 1


def test_get_latest_ami_images_found():
    with mock.patch(
        'clusterman.aws.client.ec2'
    ) as mock_ec2:
        mock_ec2.describe_images = mock.Mock(
            return_value={'Images':
                          [
                              {
                                  'CreationDate': '2001-01-30T11:49:18.000Z',
                                  'ImageId': 'abc-123'
                              },
                              {
                                  'CreationDate': '2001-01-30T11:49:17.000Z',
                                  'ImageId': 'xyz-123'
                              }
                          ]
                          }
        )
        return_value = get_latest_ami('fake_ami_type')

        assert return_value == 'abc-123'
        mock_ec2.describe_images.call_count == 1


@pytest.mark.parametrize('value_numbers', [200, 500, 1100])
def test_over_filter_limits(value_numbers):
    instance_ids = list(range(value_numbers))
    with mock.patch('clusterman.aws.client.ec2.describe_instances') as mock_describe_instances:
        ec2_describe_instances(instance_ids)
        target_call_count = math.ceil(value_numbers / MAX_PAGE_SIZE)
        assert mock_describe_instances.call_count == target_call_count
        assert mock_describe_instances.call_args_list == [
            call(InstanceIds=instance_ids[i * MAX_PAGE_SIZE:(i + 1) * MAX_PAGE_SIZE])
            for i in range(target_call_count)
        ]
