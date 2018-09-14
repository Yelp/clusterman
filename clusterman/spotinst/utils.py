import colorlog
import spotinst_sdk

from clusterman.mesos.spotinst_resource_group import load_elastigroups
from clusterman.spotinst.client import get_spotinst_client


logger = colorlog.getLogger(__name__)


def update_ami(ami_id, cluster, pool):
    client = get_spotinst_client()
    groups = load_elastigroups(cluster, pool)
    if len(groups) == 0:
        logger.warning(f'No resource groups found matching the {cluster} and {pool}.')
        return

    for currgroup in groups:
        spec = spotinst_sdk.aws_elastigroup.LaunchSpecification(image_id=ami_id)
        compute = spotinst_sdk.aws_elastigroup.Compute(launch_specification=spec)
        group_update = spotinst_sdk.aws_elastigroup.Elastigroup(compute=compute)
        logger.info(f'Updating the AMI id of the elastic group {currgroup.id} to {ami_id}')
        client.update_elastigroup(group_update=group_update, group_id=currgroup.id)
