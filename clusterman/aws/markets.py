from collections import namedtuple

from clusterman.aws.client import ec2

InstanceResources = namedtuple('InstanceResources', ['cpu', 'mem', 'disk'])


class InstanceMarket(namedtuple('InstanceMarket', ['instance', 'az'])):
    __slots__ = ()

    def __new__(cls, instance, az):
        if instance not in EC2_INSTANCE_TYPES or az not in EC2_AZS:
            raise ValueError(f'Invalid AWS market specified: <{instance}, {az}>')
        return super().__new__(cls, instance, az)

    def __str__(self):
        return f'<{self.instance}, {self.az}>'


EC2_INSTANCE_TYPES = {
    't2.nano': InstanceResources(1.0, 0.5, None),
    't2.micro': InstanceResources(1.0, 1.0, None),
    't2.small': InstanceResources(1.0, 2.0, None),
    't2.medium': InstanceResources(2.0, 4.0, None),
    't2.large': InstanceResources(2.0, 8.0, None),
    't2.xlarge': InstanceResources(4.0, 16.0, None),
    't2.2xlarge': InstanceResources(8.0, 32.0, None),
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
    # No GPU instances in this list for now
}

EC2_AZS = [
    'us-west-1a',
    'us-west-1b',
    'us-west-1c',
    'us-west-2a',
    'us-west-2b',
    'us-west-2c',
]


def get_instance_resources(market):
    return EC2_INSTANCE_TYPES[market.instance]


def get_instance_market(aws_instance_object):
    az = ec2.describe_subnets(SubnetIds=[aws_instance_object['SubnetId']])['Subnets'][0]['AvailabilityZone']
    return InstanceMarket(aws_instance_object['InstanceType'], az)
