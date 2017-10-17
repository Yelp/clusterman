import json

import boto3
import staticconf

from clusterman.util import get_clusterman_logger

_session = None
logger = get_clusterman_logger(__name__)


def _init_session():
    global _session

    boto_creds_file = staticconf.read_string('aws.access_key_file')
    logger.debug(f'initializing AWS client from {boto_creds_file}')
    if not _session:
        with open(boto_creds_file) as f:
            creds = json.load(f)

        _session = boto3.session.Session(
            aws_access_key_id=creds['accessKeyId'],
            aws_secret_access_key=creds['secretAccessKey'],
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
            cls._client = _session.client(cls.client)
        return getattr(cls._client, key)


class s3(metaclass=_BotoForwarder):
    client = 's3'


class ec2(metaclass=_BotoForwarder):
    client = 'ec2'


def ec2_describe_instances(instance_ids):
    instance_paginator = ec2.get_paginator('describe_instances')
    for page in instance_paginator.paginate(InstanceIds=instance_ids):
        for reservation in page['Reservations']:
            for i in reservation['Instances']:
                yield i
