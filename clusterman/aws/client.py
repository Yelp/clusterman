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
import os
import sys
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Sequence

import arrow
import boto3
import botocore.config
import botocore.exceptions
import colorlog
import staticconf
from cachetools.func import ttl_cache
from mypy_extensions import TypedDict
from retry import retry

from clusterman.config import CREDENTIALS_NAMESPACE

logger = colorlog.getLogger(__name__)
_session = None
MAX_PAGE_SIZE = 500

FleetInstanceDict = TypedDict(
    "FleetInstanceDict",
    {
        "InstanceId": str,
        "InstanceType": str,
        "SpotInstanceRequestId": str,
        "InstanceHealth": str,
    },
)

InstanceStateDict = TypedDict(
    "InstanceStateDict",
    {
        "Name": str,
    },
)

InstanceDict = TypedDict(
    "InstanceDict",
    {
        "InstanceId": str,
        "InstanceType": str,
        "PrivateIpAddress": str,
        "State": InstanceStateDict,
        "LaunchTime": str,
        "PrivateDnsName": str,
        "Tags": Sequence[Mapping[str, str]],
    },
)


class S3ObjectWrapper(NamedTuple):
    data: bytes
    last_modified: arrow.Arrow


def _init_session():
    global _session

    if not _session:
        _session = boto3.session.Session(
            staticconf.read_string("accessKeyId", namespace=CREDENTIALS_NAMESPACE),
            staticconf.read_string("secretAccessKey", namespace=CREDENTIALS_NAMESPACE),
            region_name="us-east-1",
        )


def get_main_module_name() -> str:
    main_spec = sys.modules["__main__"].__spec__ if "__main__" in sys.modules else None
    return main_spec.name if main_spec else ""


class _BotoForwarder(type):
    _client = None

    def __new__(cls, name, parents, dct):
        global _session
        cls._session = _session
        return super(_BotoForwarder, cls).__new__(cls, name, parents, dct)

    def __getattr__(cls, key):
        global _session
        if _session is None:
            _init_session()
        if cls._client is None:
            # Used for the dockerized cluster; endpoint_url needs to be a string containing '{svc}',
            # which will have the service name (ec2, s3, etc) substituted in here
            endpoint_url = staticconf.read_string("aws.endpoint_url", default=None)
            ua_extra = (
                os.getenv("PAASTA_INSTANCE", get_main_module_name())
                if staticconf.read_bool("aws.add_entrypoint_info", default=False)
                else None
            )
            if endpoint_url:
                endpoint_url = endpoint_url.format(svc=cls.client)
            cls._client = _session.client(
                cls.client,
                endpoint_url=endpoint_url,
                config=(botocore.config.Config(user_agent_extra=f"({ua_extra})") if ua_extra else None),
            )
        return getattr(cls._client, key)


class s3(metaclass=_BotoForwarder):
    client = "s3"


class ec2(metaclass=_BotoForwarder):
    client = "ec2"


class sqs(metaclass=_BotoForwarder):
    client = "sqs"


class sts(metaclass=_BotoForwarder):
    client = "sts"


class dynamodb(metaclass=_BotoForwarder):
    client = "dynamodb"


class autoscaling(metaclass=_BotoForwarder):
    client = "autoscaling"


# sometimes an instance has started but doesn't show up in DescribeInstances right away
@retry(exceptions=botocore.exceptions.ClientError, tries=3, delay=5)
def ec2_describe_instances(instance_ids: Sequence[str]) -> List[InstanceDict]:
    if instance_ids is None or len(instance_ids) == 0:
        return []

    # limit the page size to help prevent SSL read timeouts
    instance_id_pages = [instance_ids[i : i + MAX_PAGE_SIZE] for i in range(0, len(instance_ids), MAX_PAGE_SIZE)]
    return [
        instance
        for page in instance_id_pages
        for reservation in ec2.describe_instances(InstanceIds=page)["Reservations"]
        for instance in reservation["Instances"]
    ]


def ec2_describe_fleet_instances(fleet_id: str) -> List[FleetInstanceDict]:
    next_token = ""
    instances: List[FleetInstanceDict] = []
    while True:
        page = ec2.describe_fleet_instances(FleetId=fleet_id, NextToken=next_token, MaxResults=MAX_PAGE_SIZE)
        instances.extend(page["ActiveInstances"])
        next_token = page["NextToken"]
        if not next_token:
            break
    return instances


@ttl_cache(maxsize=8, ttl=10)
def cached_s3_get_object(bucket: str, object_key: str) -> S3ObjectWrapper:
    """S3 read through a small shortlived cache. Useful when reading
    multiple times the same object from separate methods without a trivial way
    to share data across them.

    :param str bucket: name of the S3 bucket
    :param str object_key: name of the object to fect
    :return: object data and when it was last updated
    """
    result = s3.get_object(Bucket=bucket, Key=object_key)
    return S3ObjectWrapper(
        data=result["Body"].read(),
        last_modified=arrow.get(result["LastModified"]),
    )
