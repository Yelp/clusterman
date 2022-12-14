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


AUTOSCALER_PAUSED = "autoscaler_paused"


def disable_autoscaling(cluster: str, pool: str, scheduler: str, until: Union[str, int, float]):
    """Disable autoscaling for a pool

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    :param str until: how long should it remain disabled
    """
    expiration = parse_time_string(until).timestamp if isinstance(until, str) else int(until)
    state = {
        "state": {"S": AUTOSCALER_PAUSED},
        "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        "timestamp": {"N": str(int(time.time()))},
        "expiration_timestamp": {"N": str(expiration)},
    }
    dynamodb.put_item(
        TableName=staticconf.read("aws.state_table", default=CLUSTERMAN_STATE_TABLE),
        Item=state,
    )


def enable_autoscaling(cluster: str, pool: str, scheduler: str):
    """Re-enable autoscaling for a pool

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    """
    dynamodb.delete_item(
        TableName=staticconf.read("aws.state_table", default=CLUSTERMAN_STATE_TABLE),
        Key={
            "state": {"S": AUTOSCALER_PAUSED},
            "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        },
    )


def autoscaling_is_paused(cluster: str, pool: str, scheduler: str, timestamp: arrow.Arrow) -> bool:
    """Check if autoscaling is disabled

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    :param Arrow timestamp: threshold time
    :return: True if paused
    """
    response = dynamodb.get_item(
        TableName=CLUSTERMAN_STATE_TABLE,
        Key={
            "state": {"S": AUTOSCALER_PAUSED},
            "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        },
        ConsistentRead=True,
    )
    if "Item" not in response:
        return False

    if "expiration_timestamp" in response["Item"] and timestamp.timestamp > int(
        response["Item"]["expiration_timestamp"]["N"]
    ):
        return False

    return True
