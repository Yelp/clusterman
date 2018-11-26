from functools import lru_cache
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional

from clusterman.aws.client import ec2
from clusterman.aws.client import InstanceDict

InstanceResources = NamedTuple('InstanceResources', [
    ('cpus', float),
    ('mem', float),
    ('disk', Optional[float]),
])

_InstanceMarket = NamedTuple('_InstanceMarket', [
    ('instance', str),
    ('az', Optional[str]),
])


class InstanceMarket(_InstanceMarket):
    __slots__ = ()

    def __new__(cls, instance: str, az: Optional[str]):
        if (instance in EC2_INSTANCE_TYPES and az in EC2_AZS):
            return super().__new__(cls, instance, az)
        else:
            raise ValueError(f'Invalid AWS market specified: <{instance}, {az}> (choices from {EC2_AZS})')

    def __repr__(self) -> str:
        return f'<{self.instance}, {self.az}>'

    @classmethod
    def parse(cls, string: str):
        sans_brackets = string[1:-1]
        return cls(*sans_brackets.split(', '))


EC2_INSTANCE_TYPES: Mapping[str, InstanceResources] = {
    't2.nano': InstanceResources(1.0, 0.5, None),
    't2.micro': InstanceResources(1.0, 1.0, None),
    't2.small': InstanceResources(1.0, 2.0, None),
    't2.medium': InstanceResources(2.0, 4.0, None),
    't2.large': InstanceResources(2.0, 8.0, None),
    't2.xlarge': InstanceResources(4.0, 16.0, None),
    't2.2xlarge': InstanceResources(8.0, 32.0, None),
    'm5a.large': InstanceResources(2.0, 8.0, None),
    'm5a.xlarge': InstanceResources(4.0, 16.0, None),
    'm5a.2xlarge': InstanceResources(8.0, 32.0, None),
    'm5a.4xlarge': InstanceResources(16.0, 64.0, None),
    'm5a.12xlarge': InstanceResources(48.0, 192.0, None),
    'm5a.24xlarge': InstanceResources(96.0, 384.0, None),
    'm5d.large': InstanceResources(2.0, 8.0, 75.0),
    'm5d.xlarge': InstanceResources(4.0, 16.0, 150.0),
    'm5d.2xlarge': InstanceResources(8.0, 32.0, 300.0),
    'm5d.4xlarge': InstanceResources(16.0, 64.0, 600.0),
    'm5d.12xlarge': InstanceResources(48.0, 192.0, 1800.0),
    'm5d.24xlarge': InstanceResources(96.0, 384.0, 3600.0),
    'm5.large': InstanceResources(2.0, 8.0, None),
    'm5.xlarge': InstanceResources(4.0, 16.0, None),
    'm5.2xlarge': InstanceResources(8.0, 32.0, None),
    'm5.4xlarge': InstanceResources(16.0, 64.0, None),
    'm5.12xlarge': InstanceResources(48.0, 192.0, None),
    'm5.24xlarge': InstanceResources(96.0, 384.0, None),
    'm4.large': InstanceResources(2.0, 8.0, None),
    'm4.xlarge': InstanceResources(4.0, 16.0, None),
    'm4.2xlarge': InstanceResources(8.0, 32.0, None),
    'm4.4xlarge': InstanceResources(16.0, 64.0, None),
    'm4.10xlarge': InstanceResources(40.0, 160.0, None),
    'm4.16xlarge': InstanceResources(64.0, 256.0, None),
    'm3.medium': InstanceResources(1.0, 3.75, 4.0),
    'm3.large': InstanceResources(2.0, 7.5, 32.0),
    'm3.xlarge': InstanceResources(4.0, 15.0, 80.0),
    'm3.2xlarge': InstanceResources(8.0, 30.0, 160.0),
    'c5d.large': InstanceResources(2.0, 4.0, 50.0),
    'c5d.xlarge': InstanceResources(4.0, 8.0, 100.0),
    'c5d.2xlarge': InstanceResources(8.0, 16.0, 200.0),
    'c5d.4xlarge': InstanceResources(16.0, 32.0, 400.0),
    'c5d.9xlarge': InstanceResources(36.0, 72.0, 900.0),
    'c5d.18xlarge': InstanceResources(72.0, 144.0, 1800.0),
    'c5.large': InstanceResources(2.0, 4.0, None),
    'c5.xlarge': InstanceResources(4.0, 8.0, None),
    'c5.2xlarge': InstanceResources(8.0, 16.0, None),
    'c5.4xlarge': InstanceResources(16.0, 32.0, None),
    'c5.9xlarge': InstanceResources(36.0, 72.0, None),
    'c5.18xlarge': InstanceResources(72.0, 144.0, None),
    'c4.large': InstanceResources(2.0, 3.75, None),
    'c4.xlarge': InstanceResources(4.0, 7.5, None),
    'c4.2xlarge': InstanceResources(8.0, 15.0, None),
    'c4.4xlarge': InstanceResources(16.0, 30.0, None),
    'c4.8xlarge': InstanceResources(36.0, 60.0, None),
    'c3.large': InstanceResources(2.0, 3.75, 32.0),
    'c3.xlarge': InstanceResources(4.0, 7.5, 80.0),
    'c3.2xlarge': InstanceResources(8.0, 15.0, 160.0),
    'c3.4xlarge': InstanceResources(16.0, 30.0, 320.0),
    'c3.8xlarge': InstanceResources(32.0, 60.0, 640.0),
    'x1.32xlarge': InstanceResources(128.0, 1952.0, 3840.0),
    'x1.16xlarge': InstanceResources(64.0, 976.0, 1920.0),
    'r5a.large': InstanceResources(2.0, 16.0, None),
    'r5a.xlarge': InstanceResources(4.0, 32.0, None),
    'r5a.2xlarge': InstanceResources(8.0, 64.0, None),
    'r5a.4xlarge': InstanceResources(16.0, 128.0, None),
    'r5a.12xlarge': InstanceResources(48.0, 384.0, None),
    'r5a.24xlarge': InstanceResources(96.0, 768.0, None),
    'r5d.large': InstanceResources(2.0, 16.0, 75.0),
    'r5d.xlarge': InstanceResources(4.0, 32.0, 150.0),
    'r5d.2xlarge': InstanceResources(8.0, 64.0, 300.0),
    'r5d.4xlarge': InstanceResources(16.0, 128.0, 600.0),
    'r5d.12xlarge': InstanceResources(48.0, 384.0, 1800.0),
    'r5d.24xlarge': InstanceResources(96.0, 768.0, 3600.0),
    'r5.large': InstanceResources(2.0, 16.0, None),
    'r5.xlarge': InstanceResources(4.0, 32.0, None),
    'r5.2xlarge': InstanceResources(8.0, 64.0, None),
    'r5.4xlarge': InstanceResources(16.0, 128.0, None),
    'r5.12xlarge': InstanceResources(48.0, 384.0, None),
    'r5.24xlarge': InstanceResources(96.0, 768.0, None),
    'r4.large': InstanceResources(2.0, 15.25, None),
    'r4.xlarge': InstanceResources(4.0, 30.5, None),
    'r4.2xlarge': InstanceResources(8.0, 61.0, None),
    'r4.4xlarge': InstanceResources(16.0, 122.0, None),
    'r4.8xlarge': InstanceResources(32.0, 244.0, None),
    'r4.16xlarge': InstanceResources(64.0, 488.0, None),
    'r3.large': InstanceResources(2.0, 15.25, 32.0),
    'r3.xlarge': InstanceResources(4.0, 30.5, 80.0),
    'r3.2xlarge': InstanceResources(8.0, 61.0, 160.0),
    'r3.4xlarge': InstanceResources(16.0, 122.0, 320.0),
    'r3.8xlarge': InstanceResources(32.0, 244.0, 320.0),
    'i2.xlarge': InstanceResources(4.0, 30.5, 800.0),
    'i2.2xlarge': InstanceResources(8.0, 61.0, 1600.0),
    'i2.4xlarge': InstanceResources(16.0, 122.0, 3200.0),
    'i2.8xlarge': InstanceResources(32.0, 244.0, 6400.0),
    'i3.large': InstanceResources(2.0, 15.25, 0.475),
    'i3.xlarge': InstanceResources(4.0, 30.5, 0.95),
    'i3.2xlarge': InstanceResources(8.0, 61.0, 1.9),
    'i3.4xlarge': InstanceResources(16.0, 122.0, 3.8),
    'i3.8xlarge': InstanceResources(32.0, 244.0, 7.6),
    'i3.16xlarge': InstanceResources(64.0, 488.0, 15.2),
    'd2.xlarge': InstanceResources(4.0, 30.5, 6000.0),
    'd2.2xlarge': InstanceResources(8.0, 61.0, 12000.0),
    'd2.4xlarge': InstanceResources(16.0, 122.0, 24000.0),
    'd2.8xlarge': InstanceResources(36.0, 244.0, 48000.0),
    'z1d.large': InstanceResources(2.0, 16.0, 75.0),
    'z1d.xlarge': InstanceResources(4.0, 32.0, 150.0),
    'z1d.2xlarge': InstanceResources(8.0, 64.0, 300.0),
    'z1d.3xlarge': InstanceResources(12.0, 96.0, 450.0),
    'z1d.6xlarge': InstanceResources(24.0, 192.0, 900.0),
    'z1d.12xlarge': InstanceResources(48.0, 384.0, 1800.0),
    # No GPU instances in this list for now
}

EC2_AZS: List[Optional[str]] = [
    None,
    'us-east-1a',
    'us-east-1b',
    'us-east-1c',
    'us-west-1a',
    'us-west-1b',
    'us-west-1c',
    'us-west-2a',
    'us-west-2b',
    'us-west-2c',
]


def get_market_resources(market: InstanceMarket) -> InstanceResources:
    return EC2_INSTANCE_TYPES[market.instance]


def get_market(instance_type: str, subnet_id: Optional[str]) -> InstanceMarket:
    if subnet_id is not None:
        az = subnet_to_az(subnet_id)
    else:
        # `ignore` is a workaround for mypy insisting that `az` is `str` and not `Optional[str]`
        az = None  # type: ignore
    return InstanceMarket(instance_type, az)


def get_instance_market(aws_instance_object: InstanceDict) -> InstanceMarket:
    return get_market(
        aws_instance_object['InstanceType'],
        aws_instance_object.get('SubnetId'),
    )


@lru_cache(maxsize=32)
def subnet_to_az(subnet_id: str) -> str:
    return ec2.describe_subnets(SubnetIds=[subnet_id])['Subnets'][0]['AvailabilityZone']
