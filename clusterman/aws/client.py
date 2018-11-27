from typing import List
from typing import Sequence

import boto3
import colorlog
import staticconf
from mypy_extensions import TypedDict

from clusterman.config import CREDENTIALS_NAMESPACE

_session = None
logger = colorlog.getLogger(__name__)

MAX_PAGE_SIZE = 500


InstanceStateDict = TypedDict(
    'InstanceStateDict',
    {
        'Name': str,
    },
)

InstanceDict = TypedDict(
    'InstanceDict',
    {
        'InstanceId': str,
        'InstanceType': str,
        'SubnetId': str,
        'PrivateIpAddress': str,
        'State': InstanceStateDict,
        'LaunchTime': str,
    },
)


def _init_session():
    global _session

    if not _session:
        _session = boto3.session.Session(
            staticconf.read_string('accessKeyId', namespace=CREDENTIALS_NAMESPACE),
            staticconf.read_string('secretAccessKey', namespace=CREDENTIALS_NAMESPACE),
            region_name=staticconf.read_string('aws.region')
        )


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
            endpoint_url = staticconf.read_string('aws.endpoint_url', default=None)
            if endpoint_url:
                endpoint_url = endpoint_url.format(svc=cls.client)
            cls._client = _session.client(
                cls.client,
                endpoint_url=endpoint_url,
            )
        return getattr(cls._client, key)


class s3(metaclass=_BotoForwarder):
    client = 's3'


class ec2(metaclass=_BotoForwarder):
    client = 'ec2'


class sqs(metaclass=_BotoForwarder):
    client = 'sqs'


class dynamodb(metaclass=_BotoForwarder):
    client = 'dynamodb'


def ec2_describe_instances(instance_ids: Sequence[str]) -> List[InstanceDict]:
    if instance_ids is None or len(instance_ids) == 0:
        return []

    # limit the page size to help prevent SSL read timeouts
    instance_id_pages = [
        instance_ids[i:i + MAX_PAGE_SIZE]
        for i in range(0, len(instance_ids), MAX_PAGE_SIZE)
    ]
    return [
        instance
        for page in instance_id_pages
        for reservation in ec2.describe_instances(InstanceIds=page)['Reservations']
        for instance in reservation['Instances']
    ]
