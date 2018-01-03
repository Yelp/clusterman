import json
from copy import deepcopy

import boto3
import staticconf

from clusterman.util import get_clusterman_logger

_session = None
logger = get_clusterman_logger(__name__)

FILTER_LIMIT = 200


def _init_session():
    global _session

    boto_creds_file = staticconf.read_string('aws.access_key_file')
    logger.info(f'initializing AWS client from {boto_creds_file}')
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


def ec2_describe_instances(instance_ids=None, filters=None):

    def _get_lindex_upper_bound(filter):
        if len(filters) == 0:
            return 1
        return max(1, len(filters[0]['Values']))

    instance_ids = instance_ids or []
    filters = filters or []
    filter_values = []
    # We handle couple of special scenarios here to generate ec2 instances we want.
    # First, if both of instance_ids and filters are none or empty, the AWS query will return
    # all instances in the region, so we just return None. Second, multiple filters case is not
    # implemented yet, Finally, if the number of filter value is larger than FILTER_LIMIT, we
    # break up the requests and limit by FILTER_LIMIT per request.

    if len(instance_ids) == 0 and len(filters) == 0:
        return None
    # TODO (CLUSTERMAN-116) need to support multiple filters case
    elif len(filters) > 1:
        raise NotImplementedError(f'Multiple filters is not yet supported')
    elif len(filters) == 1 and len(filters[0]['Values']) > FILTER_LIMIT:
        filter_values = filters[0]['Values']

    instance_paginator = ec2.get_paginator('describe_instances')

    for lindex in range(0, _get_lindex_upper_bound(filters), FILTER_LIMIT):
        partial_filters = deepcopy(filters)
        if lindex < len(filter_values):
            partial_filters[0]['Values'] = filter_values[lindex:lindex + FILTER_LIMIT]
        for page in instance_paginator.paginate(InstanceIds=instance_ids, Filters=partial_filters):
            for reservation in page['Reservations']:
                for i in reservation['Instances']:
                    yield i
