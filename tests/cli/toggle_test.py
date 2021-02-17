# Copyright 2021 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from argparse import Namespace

import mock
import pytest

from clusterman.cli.toggle import check_account_id, enable, disable

@pytest.fixture
def args():
    return Namespace(
        cluster='foo',
        pool='bar',
        scheduler='mesos',
        until=False,
    )


@mock.patch('clusterman.cli.toggle.sts')
@mock.patch('clusterman.cli.toggle.staticconf')
@mock.patch('clusterman.cli.toggle.logger')
class TestManageMethods:
    def test_check_account_id(self, mock_logger, mock_staticconf, mock_sts, *extra_args):
        # Note the different values
        mock_sts.get_caller_identity.return_value = "123"
        mock_staticconf.read_string.return_value = "456"

        check_account_id("sample_cluster")

        assert mock_logger.warning.call_count == 1

    @mock.patch('clusterman.cli.toggle.autoscaling_is_paused')
    @mock.patch('clusterman.cli.toggle.dynamodb')
    def test_enable(self, mock_dynamodb, mock_autoscaling_is_paused, mock_logger, mock_staticconf, mock_sts, args, *extra_args):
        # Note the different values
        mock_sts.get_caller_identity.return_value = "123"
        mock_staticconf.read_string.return_value = "456"

        mock_dynamodb.put_item = mock.Mock()

        mock_autoscaling_is_paused.return_value = False

        enable(args)

        assert mock_logger.warning.call_count == 1

    @mock.patch('clusterman.cli.toggle.autoscaling_is_paused')
    @mock.patch('clusterman.cli.toggle.dynamodb')
    def test_disable(self, mock_dynamodb, mock_autoscaling_is_paused, mock_logger, mock_staticconf, mock_sts, args, *extra_args):
        # Note the different values
        mock_sts.get_caller_identity.return_value = "123"
        mock_staticconf.read_string.return_value = "456"

        mock_dynamodb.delete_item = mock.Mock()

        mock_autoscaling_is_paused.return_value = True

        disable(args)

        assert mock_logger.warning.call_count == 1
