import json

import boto3

from clusterman.util import get_clusterman_logger

_BOTO_CREDENTIALS_FILE = '/etc/boto_cfg/clusterman.json'
_session = None
logger = get_clusterman_logger(__name__)


def _init_session():
    global _session

    logger.debug(f'initializing AWS client from {_BOTO_CREDENTIALS_FILE}')
    if not _session:
        with open(_BOTO_CREDENTIALS_FILE) as f:
            creds = json.load(f)
            creds['aws_access_key_id'] = creds.pop('accessKeyId')
            creds['aws_secret_access_key'] = creds.pop('secretAccessKey')
            creds['region_name'] = creds.pop('region')
            _session = boto3.session.Session(**creds)


class _BotoForwarder(type):
    def __new__(cls, name, parents, dct):
        global _session
        cls._session = _session
        return super(_BotoForwarder, cls).__new__(cls, name, parents, dct)

    def __getattr__(cls, key):
        global _session
        if _session is None:
            _init_session()
        return getattr(_session.client(cls.client), key)


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
