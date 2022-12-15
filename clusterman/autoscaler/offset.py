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
import time
from typing import Union

import arrow
import staticconf

from clusterman.aws.client import dynamodb
from clusterman.util import CLUSTERMAN_STATE_TABLE
from clusterman.util import parse_time_string


AUTOSCALER_CAPACITY_OFFSET_KEY = "autoscaler_capacity_offset"


def set_capacity_offset(cluster: str, pool: str, scheduler: str, until: Union[str, int, float], value: float):
    """Set temporary capacity offset for a pool

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    :param str until: how long should the override last
    :param float value: offset value
    """
    expiration = parse_time_string(until).timestamp if isinstance(until, str) else int(until)
    state = {
        "state": {"S": AUTOSCALER_CAPACITY_OFFSET_KEY},
        "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        "timestamp": {"N": str(int(time.time()))},
        "expiration_timestamp": {"N": str(expiration)},
        "offset": {"N": str(value)},
    }
    dynamodb.put_item(
        TableName=staticconf.read("aws.state_table", default=CLUSTERMAN_STATE_TABLE),
        Item=state,
    )


def remove_capacity_offset(cluster: str, pool: str, scheduler: str):
    """Remove temporary capacity offset

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    """
    dynamodb.delete_item(
        TableName=staticconf.read("aws.state_table", default=CLUSTERMAN_STATE_TABLE),
        Key={
            "state": {"S": AUTOSCALER_CAPACITY_OFFSET_KEY},
            "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        },
    )


def get_capacity_offset(cluster: str, pool: str, scheduler: str, timestamp: arrow.Arrow) -> float:
    """Get, if present, temporary capacity offset

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    :param Arrow timestamp: threshold time
    :return: value of capacity offset if present, or 0
    """
    response = dynamodb.get_item(
        TableName=CLUSTERMAN_STATE_TABLE,
        Key={
            "state": {"S": AUTOSCALER_CAPACITY_OFFSET_KEY},
            "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        },
        ConsistentRead=True,
    )
    return (
        float(response["Item"]["offset"]["N"])
        if (
            "Item" in response
            and (
                "expiration_timestamp" not in response["Item"]
                or timestamp.timestamp <= int(response["Item"]["expiration_timestamp"]["N"])
            )
        )
        else 0
    )
