# Copyright 2019 Yelp Inc.
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
from unittest import mock

import arrow
import pytest

from clusterman.autoscaler.toggle import autoscaling_is_paused


def test_is_paused_no_data_for_cluster():
    with mock.patch("clusterman.autoscaler.toggle.dynamodb") as mock_dynamo:
        mock_dynamo.get_item.return_value = {"ResponseMetadata": {"foo": "asdf"}}
        assert not autoscaling_is_paused("mesos-test", "bar", "mesos", arrow.get(300))


@pytest.mark.parametrize("exp_timestamp", [None, "100", "400"])
def test_is_paused_with_expiration_timestamp(exp_timestamp):
    with mock.patch("clusterman.autoscaler.toggle.dynamodb") as mock_dynamo:
        mock_dynamo.get_item.return_value = {
            "ResponseMetadata": {"foo": "asdf"},
            "Item": {
                "state": {"S": "autoscaler_paused"},
                "entity": {"S": "mesos-test.bar.mesos"},
            },
        }
        if exp_timestamp:
            mock_dynamo.get_item.return_value["Item"]["expiration_timestamp"] = {"N": exp_timestamp}
        assert autoscaling_is_paused("mesos-test", "bar", "mesos", arrow.get(300)) == (
            not exp_timestamp or int(exp_timestamp) > 300
        )
