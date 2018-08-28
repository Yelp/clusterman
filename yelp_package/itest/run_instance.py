#!/usr/bin/python3.6
import boto3
import simplejson as json
import yaml

session = boto3.session.Session('foo', 'bar', region_name='us-west-2')
ec2 = session.client('ec2', endpoint_url='http://moto-ec2:5000')
s3 = session.client('s3', endpoint_url='http://moto-s3:5000')

vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
subnet_response = ec2.create_subnet(
    CidrBlock='10.0.0.0/24',
    VpcId=vpc_response['Vpc']['VpcId'],
    AvailabilityZone='us-west-2a'
)
subnet_id = subnet_response['Subnet']['SubnetId']
with open('/itest/autoscaler_config.tmpl') as config_template:
    simulated_config = yaml.safe_load(config_template)
for spec in simulated_config['configs'][0]['LaunchSpecifications']:
    spec['SubnetId'] = subnet_id
with open('/itest/autoscaler_config.yaml', 'w') as config:
    yaml.dump(simulated_config, config)

# Two dummy instances so that moto and the mesos agent container have the same IPs
ec2.run_instances(MinCount=2, MaxCount=2, SubnetId=subnet_id)
sfr_response = ec2.request_spot_fleet(
    SpotFleetRequestConfig={
        'AllocationStrategy': 'diversified',
        'TargetCapacity': 1,
        'LaunchSpecifications': [
            {
                'ImageId': 'ami-foo',
                'SubnetId': subnet_id,
                'WeightedCapacity': 1,
                'InstanceType': 'm3.large',
                'EbsOptimized': False,
                # note that this is not useful until we solve
                # https://github.com/spulec/moto/issues/1644
                'TagSpecifications': [{
                    'ResourceType': 'instance',
                    'Tags': [{
                        'Key': 'foo',
                        'Value': 'bar',
                    }],
                }],
            },
        ],
        'IamFleetRole': 'foo',
    },
)

s3.create_bucket(Bucket='clusterman-resource-groups')
s3.put_object(
    Bucket='clusterman-resource-groups',
    Key='acceptance/sfr.json',
    Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request.docker': {
                'id': sfr_response['SpotFleetRequestId'],
                'pool': 'default'
            }
        }
    }).encode()
)
