import time
from typing import Optional
from typing import Union

import arrow
import staticconf

from clusterman.aws.client import dynamodb
from clusterman.util import CLUSTERMAN_STATE_TABLE
from clusterman.util import parse_time_string


AUTOSCALER_SETPOINT_OVERRIDE = "autoscaler_setpoint_override"


def set_setpoint_override(cluster: str, pool: str, scheduler: str, until: Union[str, int, float], value: float):
    """Set temporary setpoint value for a pool

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    :param str until: how long should the override last
    :param float value: setpoint value
    """
    expiration = parse_time_string(until).timestamp if isinstance(until, str) else int(until)
    state = {
        "state": {"S": AUTOSCALER_SETPOINT_OVERRIDE},
        "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        "timestamp": {"N": str(int(time.time()))},
        "expiration_timestamp": {"N": str(expiration)},
        "setpoint": {"N": str(value)},
    }
    dynamodb.put_item(
        TableName=staticconf.read("aws.state_table", default=CLUSTERMAN_STATE_TABLE),
        Item=state,
    )


def remove_setpoint_override(cluster: str, pool: str, scheduler: str):
    """Remove setpoint overrides

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    """
    dynamodb.delete_item(
        TableName=staticconf.read("aws.state_table", default=CLUSTERMAN_STATE_TABLE),
        Key={
            "state": {"S": AUTOSCALER_SETPOINT_OVERRIDE},
            "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        },
    )


def get_setpoint_override(cluster: str, pool: str, scheduler: str, timestamp: arrow.Arrow) -> Optional[float]:
    """Get, if present, setpoint override

    :param str cluster: name of the cluster
    :param str pool: name of the pool
    :param str scheduler: cluster scheduler
    :param Arrow timestamp: threshold time
    :return: if present, value of setpoint override
    """
    response = dynamodb.get_item(
        TableName=CLUSTERMAN_STATE_TABLE,
        Key={
            "state": {"S": AUTOSCALER_SETPOINT_OVERRIDE},
            "entity": {"S": f"{cluster}.{pool}.{scheduler}"},
        },
        ConsistentRead=True,
    )
    return (
        float(response["Item"]["setpoint"]["N"])
        if (
            "Item" in response
            and (
                "expiration_timestamp" not in response["Item"]
                or timestamp.timestamp <= int(response["Item"]["expiration_timestamp"]["N"])
            )
        )
        else None
    )
