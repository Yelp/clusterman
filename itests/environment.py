import behave
import mock
import simplejson as json
import staticconf.testing
import yelp_meteorite
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS
from moto import mock_autoscaling
from moto import mock_ec2
from moto import mock_sqs

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.config import CREDENTIALS_NAMESPACE

_ttl_patch = mock.patch('clusterman.aws.CACHE_TTL_SECONDS', -1)
_ttl_patch.__enter__()
behave.use_step_matcher('re')
BEHAVE_DEBUG_ON_ERROR = False


@behave.fixture
def patch_meteorite(context):
    with yelp_meteorite.testcase():
        yield


@behave.fixture
def setup_configurations(context):
    boto_config = {
        'accessKeyId': 'foo',
        'secretAccessKey': 'bar',
    }

    main_clusterman_config = {
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
            'signals_bucket': 'the_bucket',
        },
        'autoscaling': {
            'setpoint': 0.7,
            'setpoint_margin': 0.1,
            'default_signal_role': 'foo',
        },
        'batches': {
            'spot_prices': {
                'run_interval_seconds': 120,
                'dedupe_interval_seconds': 60,
            },
            'cluster_metrics': {
                'run_interval_seconds': 120,
            },
        },
        'clusters': {
            'mesos-test': {
                'fqdn': 'the.mesos.leader',
                'cluster_manager': 'mesos',
                'aws_region': 'us-west-2',
                'max_weight_to_add': 200,
                'max_weight_to_remove': 10,
            },
        },
        'sensu_config': [
            {
                'team': 'my_team',
                'runbook': 'y/my-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'DefaultSignal',
            'branch_or_tag': 'master',
            'period_minutes': 10,
        }
    }

    pool_config = {
        'resource_groups': [
            {
                'sfr': {
                    's3': {
                        'bucket': 'fake-bucket',
                        'prefix': 'none',
                    }
                },
            },
            {'asg': {'tag': 'puppet:role::paasta'}},
        ],
        'scaling_limits': {
            'min_capacity': 3,
            'max_capacity': 100,
        },
        'sensu_config': [
            {
                'team': 'other-team',
                'runbook': 'y/their-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'BarSignal3',
            'branch_or_tag': 'v42',
            'period_minutes': 7,
            'required_metrics': [
                {'name': 'cpus_allocated', 'type': SYSTEM_METRICS, 'minute_range': 10},
                {'name': 'cost', 'type': APP_METRICS, 'minute_range': 30},
            ],
        }
    }
    with staticconf.testing.MockConfiguration(boto_config, namespace=CREDENTIALS_NAMESPACE), \
            staticconf.testing.MockConfiguration(main_clusterman_config), \
            staticconf.testing.MockConfiguration(pool_config, namespace='bar_config'):
        yield


def make_asg(asg_name, subnet_id):
    autoscaling.create_launch_configuration(
        LaunchConfigurationName='mock_launch_configuration',
        ImageId='ami-foo',
        InstanceType='t2.micro',
    )
    return autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName='mock_launch_configuration',
        MinSize=1,
        MaxSize=30,
        DesiredCapacity=1,
        AvailabilityZones=['us-west-2a'],
        VPCZoneIdentifier=subnet_id,
        NewInstancesProtectedFromScaleIn=False,
        Tags=[
            {
                'Key': 'puppet:role::paasta',
                'Value': json.dumps({
                    'paasta_cluster': 'mesos-test',
                    'pool': 'bar',
                }),
            }, {
                'Key': 'fake_tag_key',
                'Value': 'fake_tag_value',
            },
        ],
    )


def make_fleet(subnet_id):
    ec2.create_launch_template(
        LaunchTemplateName='mock_launch_template',
        LaunchTemplateData={
            'InstanceType': 'c3.4xlarge',
            'NetworkInterfaces': [{'SubnetId': subnet_id}],
        },
    )
    return ec2.create_fleet(
        ExcessCapacityTerminationPolicy='no-termination',
        LaunchTemplateConfigs={'LaunchTemplateSpecification': {'LaunchTemplateName': 'mock_launch_template'}},
        TargetCapacitySpecification={'TotalTargetCapacity': 1},
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{
                    'Key': 'puppet:role::paasta',
                    'Value': json.dumps({
                        'paasta_cluster': 'mesos-test',
                        'pool': 'bar',
                    }),
                }, {
                    'Key': 'fake_fleet_key',
                    'Value': 'fake_fleet_value',
                }],
            },
        ],
    )


def make_sfr(subnet_id):
    return ec2.request_spot_fleet(
        SpotFleetRequestConfig={
            'AllocationStrategy': 'diversified',
            'SpotPrice': '2.0',
            'TargetCapacity': 1,
            'LaunchSpecifications': [
                {
                    'ImageId': 'ami-foo',
                    'SubnetId': subnet_id,
                    'WeightedCapacity': 1,
                    'InstanceType': 'c3.8xlarge',
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


@behave.fixture
def boto_patches(context):
    mock_sqs_obj = mock_sqs()
    mock_sqs_obj.start()
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    mock_autoscaling_obj = mock_autoscaling()
    mock_autoscaling_obj.start()
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    subnet_response = ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
        AvailabilityZone='us-west-2a'
    )
    context.subnet_id = subnet_response['Subnet']['SubnetId']
    yield
    mock_sqs_obj.stop()
    mock_ec2_obj.stop()
    mock_autoscaling_obj.stop()


def before_all(context):
    global BEHAVE_DEBUG_ON_ERROR
    BEHAVE_DEBUG_ON_ERROR = context.config.userdata.getbool('BEHAVE_DEBUG_ON_ERROR')
    behave.use_fixture(setup_configurations, context)
    behave.use_fixture(patch_meteorite, context)


def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == 'failed':
        import ipdb
        ipdb.post_mortem(step.exc_traceback)
