import spotinst_sdk

from clusterman.aws.client import get_latest_ami
from clusterman.aws.markets import EC2_INSTANCE_TYPES
from clusterman.mesos.spotinst_resource_group import load_elastigroups
from clusterman.spotinst.client import get_spotinst_client


def update_ami(ami_id, cluster, pool):
    client = get_spotinst_client()
    groups = load_elastigroups(cluster, pool, None)
    if len(groups) == 0:
        print(f'No resource groups found matching the {cluster} and {pool}.')
        return

    for currgroup in groups:
        spec = spotinst_sdk.aws_elastigroup.LaunchSpecification(image_id=ami_id)
        compute = spotinst_sdk.aws_elastigroup.Compute(launch_specification=spec)
        group_update = spotinst_sdk.aws_elastigroup.Elastigroup(compute=compute)
        print(f'Updating the AMI id of the elastic group {currgroup.id()} to {ami_id}')
        client.update_elastigroup(group_update=group_update, group_id=currgroup.id())


def create_new_eg(name, config):
    client = get_spotinst_client()

    if 'amiType' in config['compute']['launchSpecification'] and 'imageId' in config['compute']['launchSpecification']:
        raise Exception('Both imageId and amiType provided in the config.'
                        'Please specify either.')

    if 'imageId' in config['compute']['launchSpecification']:
        ami_id = config['compute']['launchSpecification']['imageId']
    elif 'amiType' in config['compute']['launchSpecification']:
        ami_type = config['compute']['launchSpecification']['amiType']
        ami_id = get_latest_ami(ami_type)

        if ami_id is None:
            raise Exception(f'Could not find an AMI for {ami_type}')
    else:
        raise Exception('Neither imageId nor amiType provided!')

    strategy = spotinst_sdk.aws_elastigroup.Strategy(
        risk=config['strategy']['risk'],
        on_demand_count=config['strategy']['onDemandCount'],
        utilize_reserved_instances=config['strategy']['utilizeReservedInstances'],
        fallback_to_od=config['strategy']['fallbackToOd'],
        availability_vs_cost=config['strategy']['availabilityVsCost']
    )

    # Initialize group capacity
    capacity = spotinst_sdk.aws_elastigroup.Capacity(
        minimum=config['capacity']['target'],
        maximum=config['capacity']['maximum'],
        target=config['capacity']['target'],
        unit=config['capacity']['unit']
    )

    tags = []
    # Initialize group tags
    for tag in config['compute']['launchSpecification']['tags']:
        tags.append(
            spotinst_sdk.aws_elastigroup.Tag(
                tag_key=tag['tagKey'],
                tag_value=tag['tagValue']
            )
        )

    # Initialize Launch Specification
    launchSpec = spotinst_sdk.aws_elastigroup.LaunchSpecification(
        image_id=ami_id,
        # No need to use KeyPair
        tags=tags,
        security_group_ids=config['compute']['launchSpecification']['securityGroupIds'],
        monitoring=config['compute']['launchSpecification']['monitoring'],
        iam_role=config['compute']['launchSpecification']['iamRole'],
        network_interfaces=config['compute']['launchSpecification']['networkInterfaces'],
    )

    # Initialize Availability Zones
    az_list = []
    for az in config['compute']['availabilityZones']:
        az_list.append(
            spotinst_sdk.aws_elastigroup.AvailabilityZone(
                name=az['name'],
                subnet_ids=az['subnetIds']
            )
        )

    weights = []
    if config['capacity']['unit'] == 'weight':
        instance_types_to_use = config['compute']['instanceTypes']['spot']
        instance_types_to_use.append(config['compute']['instanceTypes']['onDemand'])
        for instance in instance_types_to_use:
            weights.append(
                {
                    'instanceType': instance,
                    'weightedCapacity': EC2_INSTANCE_TYPES[instance].cpus
                }

            )

    # Initialize spot and on demand instance types
    instance_types = spotinst_sdk.aws_elastigroup.InstanceTypes(
        ondemand=config['compute']['instanceTypes']['onDemand'],
        spot=config['compute']['instanceTypes']['spot'],
        preferred_spot=config['compute']['instanceTypes']['preferredSpot'],
        weights=weights if 'weight' == config['capacity']['unit'] else None
    )

    # Initialize Compute
    compute = spotinst_sdk.aws_elastigroup.Compute(
        product="Linux/UNIX",
        instance_types=instance_types,
        availability_zones=az_list,
        launch_specification=launchSpec
    )

    # Initialize Elastigroup
    group = spotinst_sdk.aws_elastigroup.Elastigroup(
        name=name,
        capacity=capacity,
        strategy=strategy,
        compute=compute
    )

    group = client.create_elastigroup(group)
    group_id = group['id']
    print(f'Created a new ElasticGroup with id {group_id}')
