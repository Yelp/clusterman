import spotinst_sdk

from clusterman.mesos.spotinst_resource_group import load_spotinsts
from clusterman.aws.client import get_latest_ami
from clusterman.spotinst.client import get_spotinst_client


def update_ami(ami_id, cluster, pool):
    client = get_spotinst_client()
    groups = load_spotinsts(client, cluster, pool)
    if len(groups) == 0:
        print(f'No resource groups found matching the {cluster} and {pool}.')
        return

    for currgroup in groups:
        spec = spotinst_sdk.aws_elastigroup.LaunchSpecification(image_id=ami_id)
        compute = spotinst_sdk.aws_elastigroup.Compute(launch_specification=spec)
        group_update = spotinst_sdk.aws_elastigroup.Elastigroup(compute=compute)
        print(f'Updating the AMI id of the elastic group {currgroup.id()} to {ami_id}')
        client.client.update_elastigroup(group_update=group_update, group_id=currgroup.id())


def create_new_eg(name, config):
    client = get_spotinst_client()

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

    # Initialize group tags
    tag1 = spotinst_sdk.aws_elastigroup.Tag(tag_key='Creator', tag_value='Spotinst-Python-SDK')
    tag2 = spotinst_sdk.aws_elastigroup.Tag(tag_key='Name', tag_value='Spotinst-Python-SDK')
    tags = [tag1, tag2]

    securityGroupIds = []
    # Initialize group security group id list
    for sg in config['compute']['launchSpecification']['securityGroupIds']:
        securityGroupIds.append(sg)

    if 'image_id' not in config['compute']['launchSpecification']:
        # TODO: Get the latest AMI
        pass

    # Initialize Launch Specification
    launchSpec = spotinst_sdk.aws_elastigroup.LaunchSpecification(
        image_id=config['compute']['launchSpecification']['imageId'],
        # key_pair=config['compute']['launchSpecification']['keyPair'],
        key_pair=None,
        tags=tags,
        security_group_ids=securityGroupIds,
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

    # Initialize spot and on demand instance types
    instance_types = spotinst_sdk.aws_elastigroup.InstanceTypes(
        ondemand="c3.large",
        spot=config['compute']['instanceTypes']['spot'],
        preferred_spot=config['compute']['instanceTypes']['preferredSpot']
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
        description="Created by the Python SDK",
        capacity=capacity,
        strategy=strategy,
        compute=compute
    )

    group = get_spotinst_client().client.create_elastigroup(group)
    group_id = group['id']
