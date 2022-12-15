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

from clusterman.autoscaler.setpoint import get_setpoint_override
from clusterman.autoscaler.setpoint import remove_setpoint_override
from clusterman.autoscaler.setpoint import set_setpoint_override


@pytest.mark.parametrize("until", (1671030903, "2022-12-14T15:15:03+00:00"))
def test_set_setpoint_override(until):
    with mock.patch("clusterman.autoscaler.setpoint.dynamodb") as mock_dynamo:
        with mock.patch("clusterman.autoscaler.setpoint.time") as mock_time:
            mock_time.time.return_value = 1234567890
            set_setpoint_override("mesos-test", "bar", "mesos", until, 0.9)
            mock_dynamo.put_item.assert_called_once_with(
                TableName="clusterman_cluster_state",
                Item={
                    "state": {"S": "autoscaler_setpoint_override"},
                    "entity": {"S": "mesos-test.bar.mesos"},
                    "timestamp": {"N": "1234567890"},
                    "expiration_timestamp": {"N": "1671030903"},
                    "setpoint": {"N": "0.9"},
                },
            )


def test_remove_setpoint_override():
    with mock.patch("clusterman.autoscaler.setpoint.dynamodb") as mock_dynamo:
        remove_setpoint_override("mesos-test", "bar", "mesos")
        mock_dynamo.delete_item.assert_called_once_with(
            TableName="clusterman_cluster_state",
            Key={"state": {"S": "autoscaler_setpoint_override"}, "entity": {"S": "mesos-test.bar.mesos"}},
        )


def test_is_paused_no_data_for_cluster():
    with mock.patch("clusterman.autoscaler.setpoint.dynamodb") as mock_dynamo:
        mock_dynamo.get_item.return_value = {"ResponseMetadata": {"foo": "asdf"}}
        assert get_setpoint_override("mesos-test", "bar", "mesos", arrow.get(300)) is None


@pytest.mark.parametrize("exp_timestamp,expected_value", ((None, 0.8), ("100", None), ("400", 0.8)))
def test_is_paused_with_expiration_timestamp(exp_timestamp, expected_value):
    with mock.patch("clusterman.autoscaler.setpoint.dynamodb") as mock_dynamo:
        mock_dynamo.get_item.return_value = {
            "ResponseMetadata": {"foo": "asdf"},
            "Item": {
                "state": {"S": "autoscaler_paused"},
                "entity": {"S": "mesos-test.bar.mesos"},
                "setpoint": {"N": "0.8"},
            },
        }
        if exp_timestamp:
            mock_dynamo.get_item.return_value["Item"]["expiration_timestamp"] = {"N": exp_timestamp}
        assert get_setpoint_override("mesos-test", "bar", "mesos", arrow.get(300)) == expected_value
