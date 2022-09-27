# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import enum
import json
import socket
import time
from typing import Callable
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Type

import arrow
import colorlog
import staticconf
from botocore.exceptions import ClientError

from clusterman.args import add_cluster_arg
from clusterman.args import subparser
from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import sqs
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.aws.util import RESOURCE_GROUPS
from clusterman.aws.util import RESOURCE_GROUPS_REV
from clusterman.config import load_cluster_pool_config
from clusterman.config import POOL_NAMESPACE
from clusterman.config import setup_config
from clusterman.draining.kubernetes import drain as k8s_drain
from clusterman.draining.kubernetes import uncordon as k8s_uncordon
from clusterman.draining.mesos import down
from clusterman.draining.mesos import drain as mesos_drain
from clusterman.draining.mesos import operator_api
from clusterman.draining.mesos import up
from clusterman.interfaces.types import InstanceMetadata
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.util import get_pool_name_list


logger = colorlog.getLogger(__name__)
DRAIN_CACHE_SECONDS = 1800
DEFAULT_FORCE_TERMINATION = False
DEFAULT_DRAIN_REPROCESSING_DELAY_SECONDS = 15
DEFAULT_DRAINING_TIME_THRESHOLD_SECONDS = 1800
EC2_FLEET_KEYS = {
    "aws:ec2spot:fleet-request-id",
    "aws:ec2:fleet-id",
}


class TerminationReason(enum.Enum):
    SCALING_DOWN = "scaling down"
    SPOT_INTERRUPTION = "spot interruption"


class Host(NamedTuple):
    instance_id: str
    hostname: str
    group_id: str
    ip: str
    sender: str
    receipt_handle: str
    agent_id: str = ""
    pool: str = ""
    termination_reason: str = TerminationReason.SCALING_DOWN.value
    draining_start_time: str = arrow.now().for_json()
    scheduler: str = "mesos"


class DrainingClient:
    def __init__(self, cluster_name: str) -> None:
        self.client = sqs
        self.cluster = cluster_name
        self.drain_reprocessing_delay_seconds = staticconf.read_int(
            "drain_reprocessing_delay_seconds",
            default=DEFAULT_DRAIN_REPROCESSING_DELAY_SECONDS,
        )
        self.drain_queue_url = staticconf.read_string(f"clusters.{cluster_name}.drain_queue_url")
        self.termination_queue_url = staticconf.read_string(f"clusters.{cluster_name}.termination_queue_url")
        self.draining_host_ttl_cache: Dict[str, arrow.Arrow] = {}
        self.warning_queue_url = staticconf.read_string(
            f"clusters.{cluster_name}.warning_queue_url",
            default=None,
        )

    def submit_instance_for_draining(
        self,
        instance: InstanceMetadata,
        sender: Type[AWSResourceGroup],
        scheduler: str,
        pool: str,
        agent_id: str,
        draining_start_time: arrow.Arrow,
    ) -> None:
        return self.client.send_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributes={
                "Sender": {
                    "DataType": "String",
                    "StringValue": RESOURCE_GROUPS_REV[sender],
                },
            },
            MessageBody=json.dumps(
                {
                    "agent_id": agent_id,
                    "draining_start_time": draining_start_time.for_json(),
                    "group_id": instance.group_id,
                    "hostname": instance.hostname,
                    "instance_id": instance.instance_id,
                    "ip": instance.ip_address,
                    "pool": pool,
                    "termination_reason": TerminationReason.SCALING_DOWN.value,
                    "scheduler": scheduler,
                }
            ),
        )

    def submit_host_for_draining(self, host: Host, delay: Optional[int] = 0) -> None:
        return self.client.send_message(
            QueueUrl=self.drain_queue_url,
            DelaySeconds=delay,
            MessageAttributes={
                "Sender": {
                    "DataType": "String",
                    "StringValue": host.sender,
                },
            },
            MessageBody=json.dumps(
                {
                    "agent_id": host.agent_id,
                    "draining_start_time": host.draining_start_time,
                    "group_id": host.group_id,
                    "hostname": host.hostname,
                    "instance_id": host.instance_id,
                    "ip": host.ip,
                    "pool": host.pool,
                    "scheduler": host.scheduler,
                    "termination_reason": host.termination_reason,
                }
            ),
        )

    def submit_host_for_termination(self, host: Host, delay: Optional[int] = None) -> None:
        delay_seconds = (
            delay
            if delay is not None
            else staticconf.read_int(f"drain_termination_timeout_seconds.{host.sender}", default=90)
        )
        logger.info(f"Delaying terminating {host.instance_id} for {delay_seconds} seconds")
        return self.client.send_message(
            QueueUrl=self.termination_queue_url,
            DelaySeconds=delay_seconds,
            MessageAttributes={
                "Sender": {
                    "DataType": "String",
                    "StringValue": host.sender,
                },
            },
            MessageBody=json.dumps(
                {
                    "agent_id": host.agent_id,
                    "draining_start_time": host.draining_start_time,
                    "group_id": host.group_id,
                    "hostname": host.hostname,
                    "instance_id": host.instance_id,
                    "ip": host.ip,
                    "pool": host.pool,
                    "scheduler": host.scheduler,
                    "termination_reason": host.termination_reason,
                }
            ),
        )

    def get_host_to_drain(self) -> Optional[Host]:
        messages = self.client.receive_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributeNames=["Sender"],
            MaxNumberOfMessages=1,
        ).get("Messages", [])
        if messages:
            host_data = json.loads(messages[0]["Body"])
            return Host(
                sender=messages[0]["MessageAttributes"]["Sender"]["StringValue"],
                receipt_handle=messages[0]["ReceiptHandle"],
                **host_data,
            )
        return None

    def get_warned_host(self) -> Optional[Host]:
        if self.warning_queue_url is None:
            return None
        messages = self.client.receive_message(
            QueueUrl=self.warning_queue_url,
            MessageAttributeNames=["Sender"],
            MaxNumberOfMessages=1,
        ).get("Messages", [])
        if messages:
            event_data = json.loads(messages[0]["Body"])
            host = host_from_instance_id(
                sender=RESOURCE_GROUPS_REV[SpotFleetResourceGroup],
                receipt_handle=messages[0]["ReceiptHandle"],
                instance_id=event_data["detail"]["instance-id"],
            )
            # if we couldn't derive the host data from the instance id
            # then we just delete the message so we don't get stuck
            # worse case AWS will just terminate the box for us...
            if not host:
                logger.warning(
                    "Couldn't derive host data from instance id {} skipping".format(event_data["detail"]["instance-id"])
                )
                self.client.delete_message(
                    QueueUrl=self.warning_queue_url,
                    ReceiptHandle=messages[0]["ReceiptHandle"],
                )
            else:
                return host
        return None

    def get_host_to_terminate(self) -> Optional[Host]:
        messages = self.client.receive_message(
            QueueUrl=self.termination_queue_url,
            MessageAttributeNames=["Sender"],
            MaxNumberOfMessages=1,
        ).get("Messages", [])
        if messages:
            host_data = json.loads(messages[0]["Body"])
            return Host(
                sender=messages[0]["MessageAttributes"]["Sender"]["StringValue"],
                receipt_handle=messages[0]["ReceiptHandle"],
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

    def delete_warning_messages(self, hosts: Sequence[Host]) -> None:
        if self.warning_queue_url is None:
            return
        for host in hosts:
            self.client.delete_message(
                QueueUrl=self.warning_queue_url,
                ReceiptHandle=host.receipt_handle,
            )

    def process_termination_queue(
        self,
        mesos_operator_client: Optional[Callable[..., Callable[[str], Callable[..., None]]]],
        kube_operator_client: Optional[KubernetesClusterConnector],
    ) -> None:
        host_to_terminate = self.get_host_to_terminate()
        if host_to_terminate:
            # as for draining if it has a hostname we should down + up around the termination
            if host_to_terminate.scheduler == "mesos":
                logger.info(f"Mesos hosts to down+terminate+up: {host_to_terminate}")
                hostname_ip = f"{host_to_terminate.hostname}|{host_to_terminate.ip}"
                try:
                    down(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f"Failed to down {hostname_ip} continuing to terminate anyway: {e}")
                terminate_host(host_to_terminate)
                try:
                    up(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f"Failed to up {hostname_ip} continuing to terminate anyway: {e}")
            elif host_to_terminate.scheduler == "kubernetes":
                logger.info(f"Kubernetes hosts to delete k8s node and terminate: {host_to_terminate}")
                terminate_host(host_to_terminate)
            else:
                logger.info(f"Host to terminate immediately: {host_to_terminate}")
                terminate_host(host_to_terminate)
            self.delete_terminate_messages([host_to_terminate])

    def process_drain_queue(
        self,
        mesos_operator_client: Optional[Callable[..., Callable[[str], Callable[..., None]]]],
        kube_operator_client: Optional[KubernetesClusterConnector],
    ) -> None:
        host_to_process = self.get_host_to_drain()
        if host_to_process and host_to_process.instance_id not in self.draining_host_ttl_cache:
            # We should add instance_id to cache only if we submit host for termination
            should_add_to_cache = False
            if host_to_process.scheduler == "mesos":
                logger.info(f"Mesos host to drain and submit for termination: {host_to_process}")
                try:
                    mesos_drain(
                        mesos_operator_client,
                        [f"{host_to_process.hostname}|{host_to_process.ip}"],
                        arrow.now().timestamp * 1000000000,
                        staticconf.read_int("mesos_maintenance_timeout_seconds", default=600) * 1000000000,
                    )
                except Exception as e:
                    logger.error(f"Failed to drain {host_to_process.hostname} continuing to terminate anyway: {e}")
                finally:
                    self.submit_host_for_termination(host_to_process)
                    should_add_to_cache = True
            elif host_to_process.scheduler == "kubernetes":
                logger.info(f"Kubernetes host to drain and submit for termination: {host_to_process}")
                spent_time = arrow.now() - arrow.get(host_to_process.draining_start_time)
                pool_config = staticconf.NamespaceReaders(
                    POOL_NAMESPACE.format(pool=host_to_process.pool, scheduler="kubernetes")
                )
                force_terminate = pool_config.read_bool("draining.force_terminate", DEFAULT_FORCE_TERMINATION)
                draining_time_threshold_seconds = pool_config.read_int(
                    "draining.draining_time_threshold_seconds",
                    default=DEFAULT_DRAINING_TIME_THRESHOLD_SECONDS,
                )
                should_resend_to_queue = False
                disable_eviction = host_to_process.termination_reason == TerminationReason.SPOT_INTERRUPTION.value

                # Try to drain node; there are a few different possibilities:
                #  0) Instance is orphan, it should be terminated
                #  1) threshold expired, it should be terminated since force_terminate is true
                #  2) threshold expired, it should be uncordoned since force_terminate is false
                #  if it can't be uncordoned, then it should be returned to queue to try again
                #  3) threshold not expired, drain and terminate node
                #  4) threshold not expired, drain failed for any reason(api is unreachable, PDB doesn't allow eviction)
                #  then it should be returned to queue to try again

                if not host_to_process.agent_id:  # case 0
                    logger.info(f"Instance is Orphan: {host_to_process.instance_id}")
                    self.submit_host_for_termination(host_to_process, delay=0)
                    should_add_to_cache = True
                elif spent_time.total_seconds() > draining_time_threshold_seconds:
                    if force_terminate:  # case 1
                        logger.info(f"Draining expired for: {host_to_process.instance_id}")
                        self.submit_host_for_termination(host_to_process, delay=0)
                        should_add_to_cache = True
                    elif not k8s_uncordon(kube_operator_client, host_to_process.agent_id):  # case 2
                        # Todo Message can be stay in the queue up to SQS retention period, limit should be added
                        should_resend_to_queue = True
                else:
                    if k8s_drain(kube_operator_client, host_to_process.agent_id, disable_eviction):  # case 3
                        self.submit_host_for_termination(host_to_process, delay=0)
                        should_add_to_cache = True
                    else:  # case 4
                        should_resend_to_queue = True

                if should_resend_to_queue:
                    logger.info(
                        f"Delaying re-draining {host_to_process.instance_id} "
                        f"for {self.drain_reprocessing_delay_seconds} seconds"
                    )
                    self.submit_host_for_draining(host_to_process, self.drain_reprocessing_delay_seconds)
            else:
                logger.info(f"Host to submit for termination immediately: {host_to_process}")
                self.submit_host_for_termination(host_to_process, delay=0)
            self.delete_drain_messages([host_to_process])

            if should_add_to_cache:
                self.draining_host_ttl_cache[host_to_process.instance_id] = arrow.now().shift(
                    seconds=DRAIN_CACHE_SECONDS
                )

        elif host_to_process:
            logger.warning(f"Host: {host_to_process.hostname} already being processed, skipping...")
            self.delete_drain_messages([host_to_process])

    def clean_processing_hosts_cache(self) -> None:
        hosts_to_remove = []
        for instance_id, expiration_time in self.draining_host_ttl_cache.items():
            if arrow.now() > expiration_time:
                hosts_to_remove.append(instance_id)
        for host in hosts_to_remove:
            del self.draining_host_ttl_cache[host]

    def process_warning_queue(self) -> None:
        host_to_process = self.get_warned_host()
        if host_to_process:
            logger.info(f"Processing spot warning for {host_to_process.hostname}")
            spot_fleet_resource_groups: Set[str] = set()
            autoscaling_resource_groups: Set[str] = set()
            # we do this in two loops since we only use SFRs for Mesos, but we use Spot
            # ASGs for Kubernetes
            for pool in get_pool_name_list(self.cluster, "mesos"):
                pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=pool, scheduler="mesos"))
                for resource_group_conf in pool_config.read_list("resource_groups"):
                    spot_fleet_resource_groups |= set(
                        SpotFleetResourceGroup.load(
                            cluster=self.cluster,
                            pool=pool,
                            config=list(resource_group_conf.values())[0],
                        ).keys()
                    )

            for pool in get_pool_name_list(self.cluster, "kubernetes"):
                pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=pool, scheduler="kubernetes"))
                for resource_group_conf in pool_config.read_list("resource_groups"):
                    autoscaling_resource_groups |= set(
                        AutoScalingResourceGroup.load(
                            cluster=self.cluster,
                            pool=pool,
                            config=list(resource_group_conf.values())[0],
                        ).keys()
                    )

            # we should definitely ignore termination warnings that aren't from this
            # cluster or maybe not even paasta instances...
            if (
                host_to_process.group_id in spot_fleet_resource_groups
                or host_to_process.group_id in autoscaling_resource_groups
            ):
                logger.info(f"Sending warned host to drain: {host_to_process.hostname}")
                self.submit_host_for_draining(host_to_process)
            else:
                logger.info(f"Ignoring warned host because not in our target group: {host_to_process.hostname}")
            self.delete_warning_messages([host_to_process])


def host_from_instance_id(
    sender: str,
    receipt_handle: str,
    instance_id: str,
) -> Optional[Host]:
    try:
        instance_data = ec2_describe_instances(instance_ids=[instance_id])
    except ClientError as e:
        logger.exception(f"Couldn't describe instance: {e}")
        return None
    if not instance_data:
        logger.warning(f"No instance data found for {instance_id}")
        return None
    try:
        group_ids = [tag["Value"] for tag in instance_data[0]["Tags"] if tag["Key"] in EC2_FLEET_KEYS]
        scheduler = "mesos"
        for tag in instance_data[0]["Tags"]:
            if tag["Key"] == "KubernetesCluster":
                scheduler = "kubernetes"
                break
    except KeyError:
        logger.exception("Spot tag key not found - is this Spot Fleet/ASG correctly configured?")
        group_ids = []
    if not group_ids:
        logger.warning(f"Not draining {instance_id}: no Spot ID found - is this actually a Spot instance?")
        return None
    try:
        ip = instance_data[0]["PrivateIpAddress"]
    except KeyError:
        logger.warning(f"No primary IP found for {instance_id}")
        return None
    try:
        agent_id = instance_data[0]["PrivateDnsName"]
    except KeyError:
        logger.warning(f"No DNS name found for {instance_id} - continuing to proceed anyway")
    try:
        hostnames = socket.gethostbyaddr(ip)
    except socket.error:
        logger.warning(f"Couldn't derive hostname from IP via DNS for {ip}")
        return None
    return Host(
        agent_id=agent_id,
        sender=sender,
        receipt_handle=receipt_handle,
        instance_id=instance_id,
        hostname=hostnames[0],
        group_id=group_ids[0],
        ip=ip,
        termination_reason=TerminationReason.SPOT_INTERRUPTION.value,
        scheduler=scheduler,
        draining_start_time=arrow.now().for_json(),
    )


def process_queues(cluster_name: str) -> None:
    draining_client = DrainingClient(cluster_name)
    cluster_manager_name = staticconf.read_string(f"clusters.{cluster_name}.cluster_manager")
    mesos_operator_client = kube_operator_client = None
    try:
        kube_operator_client = KubernetesClusterConnector(cluster_name, None)
    except Exception:
        logger.error("Cluster specified is mesos specific. Skipping kubernetes operator")
    if cluster_manager_name == "mesos":
        try:
            mesos_master_url = staticconf.read_string(f"clusters.{cluster_name}.mesos_master_fqdn")
            mesos_secret_path = staticconf.read_string("mesos.mesos_agent_secret_path", default=None)
            mesos_operator_client = operator_api(mesos_master_url, mesos_secret_path)
        except Exception:
            logger.error("Cluster specified is kubernetes specific. Skipping mesos operator")

    logger.info("Polling SQS for messages every 5s")
    while True:
        if kube_operator_client:
            kube_operator_client.reload_client()
        draining_client.clean_processing_hosts_cache()
        draining_client.process_warning_queue()
        draining_client.process_drain_queue(
            mesos_operator_client=mesos_operator_client,
            kube_operator_client=kube_operator_client,
        )
        draining_client.process_termination_queue(
            mesos_operator_client=mesos_operator_client,
            kube_operator_client=kube_operator_client,
        )
        time.sleep(5)


def terminate_host(host: Host) -> None:
    logger.info(f"Terminating: {host.instance_id}")
    resource_group_class = RESOURCE_GROUPS[host.sender]
    resource_group = resource_group_class(host.group_id)
    resource_group.terminate_instances_by_id([host.instance_id])


def main(args: argparse.Namespace) -> None:
    setup_config(args)
    for pool in get_pool_name_list(args.cluster, "mesos"):
        load_cluster_pool_config(args.cluster, pool, "mesos", None)
    for pool in get_pool_name_list(args.cluster, "kubernetes"):
        load_cluster_pool_config(args.cluster, pool, "kubernetes", None)
    process_queues(args.cluster)


@subparser("drain", "Drains and terminates instances submitted to SQS by clusterman", main)
def add_queue_parser(
    subparser: argparse.ArgumentParser,
    required_named_args: argparse.Namespace,
    optional_named_args: argparse.Namespace,
) -> None:
    add_cluster_arg(required_named_args, required=True)
