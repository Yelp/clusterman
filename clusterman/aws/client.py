import arrow
import boto3
import staticconf
from mypy_extensions import TypedDict

from clusterman.config import CREDENTIALS_NAMESPACE
from clusterman.util import get_clusterman_logger

_session = None
logger = get_clusterman_logger(__name__)

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
            cls._client = _session.client(cls.client)
        return getattr(cls._client, key)


class s3(metaclass=_BotoForwarder):
    client = 's3'


class ec2(metaclass=_BotoForwarder):
    client = 'ec2'


class dynamodb(metaclass=_BotoForwarder):
    client = 'dynamodb'


def get_latest_ami(ami_type):
    filters = [{
        'Name': 'name',
        'Values': [f'{ami_type}*']
    }, {
        'Name': 'state',
        'Values': ['available']
    }
    ]

    try:
        response = ec2.describe_images(Filters=filters)
    except Exception as e:
        logger.warning(f'Describe images call failed with {str(e)}')
        raise e

    if len(response['Images']) == 0:
        logger.warning(f'Could not find any images matching the constraints.')
        return

    latest = None
    for image in response['Images']:
        if not latest:
            latest = image
            continue

        if arrow.get(image['CreationDate']) > arrow.get(latest['CreationDate']):
            latest = image

    return latest['ImageId']


def ec2_describe_instances(instance_ids):
    if instance_ids is None or len(instance_ids) == 0:
        raise ValueError('instance_ids cannot be None or empty')

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
