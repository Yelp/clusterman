import math

import mock
import pytest
from mock import call

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import FILTER_LIMIT
from clusterman.aws.client import MAX_PAGE_SIZE


def test_empty_instances_ids_empty_filters():
    instances = list(ec2_describe_instances(None, None))
    assert len(instances) == 0


def test_empty_filters():
    ids = ["fake-instance-ids-1", "fake-instance-ids-2"]
    with mock.patch('clusterman.aws.client.ec2.get_paginator') as mock_paginator:
        list(ec2_describe_instances(instance_ids=ids))
        assert mock_paginator.return_value.paginate.call_count == 1
        mock_paginator.assert_has_calls([call().paginate(
            Filters=[],
            InstanceIds=ids,
            PaginationConfig={'PageSize': 500}
        )])


@pytest.mark.parametrize('value_numbers', [100, 200, 300, 400, 500])
def test_over_filter_limits(value_numbers):
    fake_values = list(range(value_numbers))
    fake_filters = [{'Values': fake_values}]
    with mock.patch('clusterman.aws.client.ec2.get_paginator') as mock_paginator:
        list(ec2_describe_instances(filters=fake_filters))
        target_call_count = math.ceil(value_numbers / FILTER_LIMIT)
        assert mock_paginator.return_value.paginate.call_count == target_call_count
        for i in range(target_call_count):
            mock_paginator.assert_has_calls(
                [call().paginate(
                    Filters=[{'Values': fake_values[i * FILTER_LIMIT:(i + 1) * FILTER_LIMIT]}],
                    InstanceIds=[],
                    PaginationConfig={'PageSize': MAX_PAGE_SIZE},
                )]
            )
