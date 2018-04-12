import docker

from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


class DockerResourceGroup(MesosPoolResourceGroup):

    def __init__(self, container_id):
        self.docker_client = docker.from_env()
        self.container_id = container_id
        self.container = self.docker_client.containers.get(container_id)

    def modify_target_capacity(self, target_capacity, *, terminate_excess_capacity, dry_run):
        logger.info(f'Requested change in target capacity to {target_capacity}')

    def terminate_instances_by_id(self, instance_ids):
        logger.info(f'Requested the following instances to be terminated: {instance_ids}')
        return instance_ids

    @property
    def id(self):
        return self.container_id

    @property
    def instance_ids(self):
        return [1]

    @property
    def market_capacities(self):
        return {}

    @property
    def target_capacity(self):
        return 1

    @property
    def fulfilled_capacity(self):
        return 1

    @property
    def status(self):
        return "running"
