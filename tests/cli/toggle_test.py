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

from clusterman.cli.toggle import disable
from clusterman.cli.toggle import enable
from clusterman.cli.toggle import ensure_account_id
from clusterman.exceptions import AccountNumberMistmatchError


@pytest.fixture
def args():
    return Namespace(cluster="foo", pool="bar", scheduler="mesos", until=False,)


@mock.patch("clusterman.cli.toggle.sts")
@mock.patch("clusterman.cli.toggle.staticconf")
@mock.patch("clusterman.cli.toggle.logger")
class TestManageMethods:
    def test_ensure_account_id(self, mock_logger, mock_staticconf, mock_sts, *extra_args):
        # Note the different values
        mock_sts.get_caller_identity.return_value = {"Account": "123"}
        mock_staticconf.read_string.return_value = "456"

        with pytest.raises(AccountNumberMistmatchError):
            ensure_account_id("sample_cluster")

    @mock.patch("clusterman.cli.toggle.autoscaling_is_paused")
    @mock.patch("clusterman.cli.toggle.dynamodb")
    def test_enable(
        self, mock_dynamodb, mock_autoscaling_is_paused, mock_logger, mock_staticconf, mock_sts, args, *extra_args,
    ):
        # Note the different values
        mock_sts.get_caller_identity.return_value = {"Account": "123"}
        mock_staticconf.read_string.return_value = "456"

        mock_dynamodb.put_item = mock.Mock()

        mock_autoscaling_is_paused.return_value = False

        with pytest.raises(AccountNumberMistmatchError):
            enable(args)

    @mock.patch("clusterman.cli.toggle.autoscaling_is_paused")
    @mock.patch("clusterman.cli.toggle.dynamodb")
    def test_disable(
        self, mock_dynamodb, mock_autoscaling_is_paused, mock_logger, mock_staticconf, mock_sts, args, *extra_args,
    ):
        # Note the different values
        mock_sts.get_caller_identity.return_value = {"Account": "123"}
        mock_staticconf.read_string.return_value = "456"

        mock_dynamodb.delete_item = mock.Mock()

        mock_autoscaling_is_paused.return_value = True

        with pytest.raises(AccountNumberMistmatchError):
            disable(args)

    @mock.patch("clusterman.cli.toggle.autoscaling_is_paused")
    @mock.patch("clusterman.cli.toggle.dynamodb")
    def test_disable_until(
        self,
        mock_dynamodb,
        mock_autoscaling_is_paused,
        mock_logger,
        mock_staticconf,
        mock_sts,
        args,
        capsys,
        *extra_args,
    ):
        """Test default until value, and test it shoes a message correctly"""
        # Note the different values
        mock_sts.get_caller_identity.return_value = {"Account": "123"}
        mock_staticconf.read_string.return_value = "123"

        mock_dynamodb.delete_item = mock.Mock()

        mock_autoscaling_is_paused.return_value = True

        disable(args)
        # https://docs.pytest.org/en/6.2.x/capture.html#accessing-captured-output-from-a-test-function
        captured = capsys.readouterr()
        assert "Default has changed; autoscaler" in captured.out
