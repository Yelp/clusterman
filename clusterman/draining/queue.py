import argparse
import datetime
import json
import logging
import time
from typing import Callable
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Type

import staticconf
from yelp_servlib.config_util import load_default_config

from clusterman.args import add_cluster_arg
from clusterman.args import subparser
from clusterman.aws.client import sqs
from clusterman.config import CREDENTIALS_NAMESPACE
from clusterman.draining.mesos import down
from clusterman.draining.mesos import drain
from clusterman.draining.mesos import operator_api
from clusterman.draining.mesos import up
from clusterman.mesos.mesos_pool_resource_group import MesosPoolResourceGroup
from clusterman.mesos.util import InstanceMetadata
from clusterman.mesos.util import RESOURCE_GROUPS
from clusterman.mesos.util import RESOURCE_GROUPS_REV
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


class Host(NamedTuple):
    instance_id: str
    hostname: str
    group_id: str
    ip: str
    sender: str
    receipt_handle: str


class DrainingClient():
    def __init__(self, cluster_name: str) -> None:
        self.client = sqs
        self.cluster = cluster_name
        self.drain_queue_url = staticconf.read_string(f'mesos_clusters.{cluster_name}.drain_queue_url')
        self.termination_queue_url = staticconf.read_string(f'mesos_clusters.{cluster_name}.termination_queue_url')

    def submit_host_for_draining(self, instance: InstanceMetadata, sender: Type[MesosPoolResourceGroup]) -> None:
        return self.client.send_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': RESOURCE_GROUPS_REV[sender],
                },
            },
            MessageBody=json.dumps(
                {
                    'instance_id': instance.instance_id,
                    'ip': instance.instance_ip,
                    'hostname': instance.hostname,
                    'group_id': instance.group_id,
                }
            ),
        )

    def submit_host_for_termination(self, host: Host, delay: Optional[int] = None) -> None:
        delay_seconds = delay if delay is not None else staticconf.read_int(
            f'drain_termination_timeout_seconds.{host.sender}', default=90
        )
        logger.info(f'Delaying terminating {host.instance_id} for {delay_seconds} seconds')
        return self.client.send_message(
            QueueUrl=self.termination_queue_url,
            DelaySeconds=delay_seconds,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': host.sender,
                },
            },
            MessageBody=json.dumps(
                {
                    'instance_id': host.instance_id,
                    'ip': host.ip,
                    'hostname': host.hostname,
                    'group_id': host.group_id,
                }
            ),
        )

    def get_host_to_drain(self) -> Optional[Host]:
        messages = self.client.receive_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1
        ).get('Messages', [])
        if messages:
            host_data = json.loads(messages[0]['Body'])
            return Host(
                sender=messages[0]['MessageAttributes']['Sender']['StringValue'],
                receipt_handle=messages[0]['ReceiptHandle'],
                **host_data,
            )
        return None

    def get_host_to_terminate(self) -> Optional[Host]:
        messages = self.client.receive_message(
            QueueUrl=self.termination_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        ).get('Messages', [])
        if messages:
            host_data = json.loads(messages[0]['Body'])
            return Host(
                sender=messages[0]['MessageAttributes']['Sender']['StringValue'],
                receipt_handle=messages[0]['ReceiptHandle'],
                **host_data,
            )
        return None

    def delete_drain_messages(self, hosts: Sequence[Host]) -> None:
        for host in hosts:
            self.client.delete_message(
                QueueUrl=self.drain_queue_url,
                ReceiptHandle=host.receipt_handle,
            )

    def delete_terminate_messages(self, hosts: Sequence[Host]) -> None:
        for host in hosts:
            self.client.delete_message(
                QueueUrl=self.termination_queue_url,
                ReceiptHandle=host.receipt_handle,
            )

    def process_termination_queue(
        self,
        mesos_operator_client: Callable[..., Callable[[str], Callable[..., None]]],
    ) -> None:
        host_to_terminate = self.get_host_to_terminate()
        if host_to_terminate:
            # as for draining if it has a hostname we should down + up around the termination
            if host_to_terminate.hostname:
                logger.info(f'Hosts to down+terminate+up: {host_to_terminate}')
                hostname_ip = f'{host_to_terminate.hostname}|{host_to_terminate.ip}'
                try:
                    down(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f'Failed to down {hostname_ip} continuing to terminate anyway: {e}')
                terminate_host(host_to_terminate)
                try:
                    up(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f'Failed to up {hostname_ip} continuing to terminate anyway: {e}')
            else:
                logger.info(f'Host to terminate: {host_to_terminate}')
                terminate_host(host_to_terminate)
            self.delete_terminate_messages([host_to_terminate])

    def process_drain_queue(
        self,
        mesos_operator_client: Callable[..., Callable[[str], Callable[..., None]]],
    ) -> None:
        host_to_process = self.get_host_to_drain()
        if host_to_process:
            # if hosts do not have hostname it means they are likely not in mesos and don't need draining
            # so instead we send them to terminate straight away
            if not host_to_process.hostname:
                logger.info(f'Host to submit for termination immediately: {host_to_process}')
                self.submit_host_for_termination(host_to_process, delay=0)
            else:
                logger.info(f'Host to drain and submit for termination: {host_to_process}')
                try:
                    drain(
                        mesos_operator_client,
                        [f'{host_to_process.hostname}|{host_to_process.ip}'],
                        int(datetime.datetime.now().strftime('%s')) * 1000000000,
                        staticconf.read_int('mesos_maintenance_timeout_seconds', default=600) * 1000000000
                    )
                except Exception as e:
                    logger.error(f'Failed to drain {host_to_process.hostname} continuing to terminate anyway: {e}')
                finally:
                    self.submit_host_for_termination(host_to_process)
            self.delete_drain_messages([host_to_process])


def process_queues(cluster_name: str) -> None:
    draining_client = DrainingClient(cluster_name)
    mesos_master_fqdn = staticconf.read_string(f'mesos_clusters.{cluster_name}.fqdn')
    mesos_secret_path = staticconf.read_string(f'mesos.mesos_agent_secret_path', default='/nail/etc/mesos-slave-secret')
    operator_client = operator_api(mesos_master_fqdn, mesos_secret_path)
    logger.info('Polling SQS for messages every 5s')
    while True:
        draining_client.process_drain_queue(
            mesos_operator_client=operator_client,
        )
        draining_client.process_termination_queue(
            mesos_operator_client=operator_client,
        )
        time.sleep(5)


def terminate_host(host: Host) -> None:
    logger.info(f'Terminating: {host.instance_id}')
    resource_group_class = RESOURCE_GROUPS[host.sender]
    resource_group = resource_group_class(host.group_id)
    resource_group.terminate_instances_by_id([host.instance_id])


def setup_config(cluster: str, env_config_path: str, log_level: str) -> None:
    logger.setLevel(getattr(logging, log_level.upper()))
    load_default_config(env_config_path, env_config_path)
    boto_creds_file = staticconf.read_string('aws.access_key_file')
    aws_region = staticconf.read_string(f'mesos_clusters.{cluster}.aws_region')
    staticconf.DictConfiguration({'aws': {'region': aws_region}})
    staticconf.JSONConfiguration(boto_creds_file, namespace=CREDENTIALS_NAMESPACE)


def main(args: argparse.Namespace) -> None:
    setup_config(
        cluster=args.cluster,
        env_config_path=args.env_config_path,
        log_level=args.log_level,
    )
    process_queues(args.cluster)


@subparser('drain', 'Drains and terminates instances submitted to SQS by clusterman', main)
def add_queue_parser(
    subparser: argparse.ArgumentParser,
    required_named_args: argparse.Namespace,
    optional_named_args: argparse.Namespace
) -> None:
    add_cluster_arg(required_named_args, required=True)
