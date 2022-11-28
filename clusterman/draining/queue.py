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
import enum
import json
import socket
from typing import Callable
from typing import Dict
from typing import Hashable
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Type

import arrow
import cachetools
import colorlog
import staticconf
from botocore.exceptions import ClientError

from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import sqs
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.aws.util import RESOURCE_GROUPS
from clusterman.aws.util import RESOURCE_GROUPS_REV
from clusterman.config import POOL_NAMESPACE
from clusterman.draining.kubernetes import drain as k8s_drain
from clusterman.draining.kubernetes import uncordon as k8s_uncordon
from clusterman.draining.mesos import down
from clusterman.draining.mesos import drain as mesos_drain
from clusterman.draining.mesos import up
from clusterman.interfaces.types import InstanceMetadata
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.monitoring_lib import get_monitoring_client
from clusterman.util import get_pool_name_list


logger = colorlog.getLogger(__name__)
DRAIN_CACHE_SECONDS = 1800
DEFAULT_RESOURCE_GROUPS_CACHE_SECONDS = 0
DEFAULT_FORCE_TERMINATION = False
DEFAULT_GLOBAL_REDRAINING_DELAY_SECONDS = 15
DEFAULT_DRAINING_TIME_THRESHOLD_SECONDS = 1800
EC2_ASG_TAG_KEY = "aws:autoscaling:groupName"
EC2_IDENTIFIER_TAG_KEY = "puppet:role::kube"
EC2_TAG_GROUP_KEYS = {
    "aws:ec2spot:fleet-request-id",
    "aws:autoscaling:groupName",
}
SFX_EXPIRATION_COUNT = "clusterman.drainer.expiration_count"
SFX_DRAINING_COUNT = "clusterman.drainer.draining_count"
SFX_DUPLICATE_COUNT = "clusterman.drainer.duplicate_count"
SFX_DRAINING_DURATION = "clusterman.drainer.draining_duration"
SFX_RECEIVING_DURATION = "clusterman.drainer.receiving_duration"
SFX_TERMINATING_DURATION = "clusterman.drainer.terminating_duration"


class TerminationReason(enum.Enum):
    SCALING_DOWN = "scaling down"
    SPOT_INTERRUPTION = "spot interruption"
    NODE_MIGRATION = "node migration"


class Host(NamedTuple):
    instance_id: str
    hostname: str
    group_id: str
    ip: str
    sender: str
    receipt_handle: str
    agent_id: str = ""
    pool: str = ""
    attempt: int = 1
    termination_reason: str = TerminationReason.SCALING_DOWN.value
    draining_start_time: str = arrow.now().for_json()
    scheduler: str = "mesos"


class DrainingClient:
    def __init__(self, cluster_name: str) -> None:
        self.client = sqs
        self.cluster = cluster_name
        self.global_redraining_delay_seconds = staticconf.read_int(
            "global_redraining_delay_seconds",
            default=DEFAULT_GLOBAL_REDRAINING_DELAY_SECONDS,
        )
        self.drain_queue_url = staticconf.read_string(f"clusters.{cluster_name}.drain_queue_url")
        self.termination_queue_url = staticconf.read_string(f"clusters.{cluster_name}.termination_queue_url")
        self.draining_host_ttl_cache: Dict[str, arrow.Arrow] = {}
        self.warning_queue_url = staticconf.read_string(
            f"clusters.{cluster_name}.warning_queue_url",
            default=None,
        )
        self.spot_fleet_resource_groups_cache: MutableMapping[Hashable, Set[str]] = cachetools.TTLCache(
            maxsize=1,
            ttl=staticconf.read_int(
                f"clusters.{cluster_name}.spot_fleet_resource_groups_cache_seconds",
                default=DEFAULT_RESOURCE_GROUPS_CACHE_SECONDS,
            ),
        )
        asg_groups_cache_ttl = staticconf.read_int(
            f"clusters.{cluster_name}.auto_scaling_resource_groups_cache_seconds",
            default=DEFAULT_RESOURCE_GROUPS_CACHE_SECONDS,
        )
        self.is_asg_cache_enabled = asg_groups_cache_ttl > 0
        self.auto_scaling_resource_groups_cache: MutableMapping[Hashable, Set[str]] = cachetools.TTLCache(
            maxsize=1,
            ttl=asg_groups_cache_ttl,
        )
        monitoring_info = {"cluster": cluster_name}
        self.expiration_counter = get_monitoring_client().create_counter(SFX_EXPIRATION_COUNT, monitoring_info)
        self.draining_counter = get_monitoring_client().create_counter(SFX_DRAINING_COUNT, monitoring_info)
        self.duplicate_counter = get_monitoring_client().create_counter(SFX_DUPLICATE_COUNT, monitoring_info)
        self.draining_timer = get_monitoring_client().create_timer(SFX_DRAINING_DURATION, monitoring_info)
        self.receiving_timer = get_monitoring_client().create_timer(SFX_RECEIVING_DURATION, monitoring_info)
        self.terminating_timer = get_monitoring_client().create_timer(SFX_TERMINATING_DURATION, monitoring_info)

    def submit_instance_for_draining(
        self,
        instance: InstanceMetadata,
        sender: Type[AWSResourceGroup],
        scheduler: str,
        pool: str,
        agent_id: str,
        termination_reason: TerminationReason,
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
                    "attempt": 1,
                    "draining_start_time": draining_start_time.for_json(),
                    "group_id": instance.group_id,
                    "hostname": instance.hostname,
                    "instance_id": instance.instance_id,
                    "ip": instance.ip_address,
                    "pool": pool,
                    "termination_reason": termination_reason.value,
                    "scheduler": scheduler,
                }
            ),
        )

    def submit_host_for_draining(self, host: Host, delay: Optional[int] = 0, attempt: Optional[int] = 1) -> None:
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
                    "attempt": attempt,
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
    ) -> bool:
        message_exist = False
        host_to_terminate = self.get_host_to_terminate()
        if host_to_terminate:
            message_exist = True
            # as for draining if it has a hostname we should down + up around the termination
            if host_to_terminate.scheduler == "mesos":
                logger.info(f"Mesos hosts to down+terminate+up: {host_to_terminate}")
                hostname_ip = f"{host_to_terminate.hostname}|{host_to_terminate.ip}"
                try:
                    down(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f"Failed to down {hostname_ip} continuing to terminate anyway: {e}")
                self.terminate_host(host_to_terminate)
                try:
                    up(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f"Failed to up {hostname_ip} continuing to terminate anyway: {e}")
            elif host_to_terminate.scheduler == "kubernetes":
                logger.info(f"Kubernetes host to delete k8s node and terminate: {host_to_terminate}")
                try:
                    self.terminate_host(host_to_terminate)
                    terminating_time_milliseconds = self._get_spent_time_milliseconds(host_to_terminate)
                    logger.info(
                        f"terminating took {terminating_time_milliseconds} "
                        f"milliseconds for {host_to_terminate.instance_id}"
                    )
                    self.terminating_timer.record(
                        terminating_time_milliseconds,
                        {
                            "pool": host_to_terminate.pool,
                            "reason": host_to_terminate.termination_reason,
                        },
                    )
                except Exception as e:
                    logger.exception(f"Failed to terminate {host_to_terminate.instance_id}: {e}")
                    # we should stop here so as not to delete message from queue
                    return message_exist
            else:
                logger.info(f"Host to terminate immediately: {host_to_terminate}")
                self.terminate_host(host_to_terminate)
            self.delete_terminate_messages([host_to_terminate])
        return message_exist

    def process_drain_queue(
        self,
        mesos_operator_client: Optional[Callable[..., Callable[[str], Callable[..., None]]]],
        kube_operator_client: Optional[KubernetesClusterConnector],
    ) -> bool:
        message_exist = False
        host_to_process = self.get_host_to_drain()
        if host_to_process and (
            host_to_process.instance_id not in self.draining_host_ttl_cache
            or host_to_process.attempt > 1  # re-draining shouldn't be avoided due to caching
            # We may have instance in the cache for different reasons. But we have to process force draining
            # if we receive spot interruption
            or host_to_process.termination_reason == TerminationReason.SPOT_INTERRUPTION.value
        ):
            self.draining_host_ttl_cache[host_to_process.instance_id] = arrow.now().shift(seconds=DRAIN_CACHE_SECONDS)
            message_exist = True
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
            elif host_to_process.scheduler == "kubernetes":
                self._emit_draining_metrics(host_to_process)
                logger.info(f"Kubernetes host to drain and submit for termination: {host_to_process}")
                spent_time_milliseconds = self._get_spent_time_milliseconds(host_to_process)
                pool_config = staticconf.NamespaceReaders(
                    POOL_NAMESPACE.format(pool=host_to_process.pool, scheduler="kubernetes")
                )
                force_terminate = pool_config.read_bool("draining.force_terminate", DEFAULT_FORCE_TERMINATION)
                draining_time_threshold_seconds = pool_config.read_int(
                    "draining.draining_time_threshold_seconds",
                    default=DEFAULT_DRAINING_TIME_THRESHOLD_SECONDS,
                )
                redraining_delay_seconds = pool_config.read_int(
                    "draining.redraining_delay_seconds",
                    default=self.global_redraining_delay_seconds,
                )
                disable_eviction = host_to_process.termination_reason == TerminationReason.SPOT_INTERRUPTION.value
                # Try to drain node; there are a few different possibilities:
                #  0) host is orphan, getting host information from AWS
                #       a) host doesn't exist, don't need any action
                #       b) host doesn't have agent_id (PrivateDnsName), submit it for termination
                #       c) host exists, submit for draining as non-orphan
                #  1) threshold expired, it should be terminated since force_terminate is true
                #  2) threshold expired, it should be uncordoned since force_terminate is false
                #  3) threshold not expired, drain and terminate node, if it can't submit it for re-draining.

                if not host_to_process.agent_id:  # case 0
                    logger.info(f"Host doesn't have agent_id, it may be orphan: {host_to_process.instance_id}")
                    host_to_process_fresh = host_from_instance_id(
                        host_to_process.receipt_handle,
                        host_to_process.instance_id,
                        host_to_process.pool,
                        host_to_process.termination_reason,
                    )
                    if not host_to_process_fresh:  # case 0a
                        logger.info(f"Host doesn't exist: {host_to_process.instance_id}")
                    elif not host_to_process_fresh.agent_id:  # case 0b
                        logger.info(f"Host doesn't have agent_id: {host_to_process.instance_id}")
                        self.submit_host_for_termination(host_to_process, delay=0)
                    else:  # case 0c
                        logger.info(f"Sending host to drain: {host_to_process.instance_id}")
                        self.submit_host_for_draining(host_to_process_fresh, attempt=host_to_process.attempt + 1)
                elif spent_time_milliseconds / 1000 > draining_time_threshold_seconds:
                    self.expiration_counter.count(
                        1,
                        {
                            "pool": host_to_process.pool,
                            "force_terminate": force_terminate,
                            "reason": host_to_process.termination_reason,
                        },
                    )
                    logger.info(f"Draining expired for: {host_to_process.instance_id}")
                    if force_terminate:  # case 1
                        self.submit_host_for_termination(host_to_process, delay=0)
                    else:  # case 2
                        k8s_uncordon(kube_operator_client, host_to_process.agent_id)
                        #  removing instance_id from cache to avoid unnecessary blocking by cache
                        self.draining_host_ttl_cache.pop(host_to_process.instance_id, None)
                elif not self._drain_k8s_host(kube_operator_client, host_to_process, disable_eviction):  # case 3
                    logger.info(
                        f"Delaying re-draining {host_to_process.instance_id} for {redraining_delay_seconds} seconds"
                    )
                    self.submit_host_for_draining(
                        host_to_process, redraining_delay_seconds, host_to_process.attempt + 1
                    )
            else:
                logger.info(f"Host to submit for termination immediately: {host_to_process}")
                self.submit_host_for_termination(host_to_process, delay=0)
            self.delete_drain_messages([host_to_process])

        elif host_to_process:
            logger.warning(f"Host: {host_to_process.hostname} already being processed, skipping...")
            self.delete_drain_messages([host_to_process])
            message_exist = True
            self.duplicate_counter.count(
                1,
                {
                    "pool": host_to_process.pool,
                    "reason": host_to_process.termination_reason,
                },
            )
        return message_exist

    def clean_processing_hosts_cache(self) -> None:
        hosts_to_remove = []
        for instance_id, expiration_time in self.draining_host_ttl_cache.items():
            if arrow.now() > expiration_time:
                hosts_to_remove.append(instance_id)
        for host in hosts_to_remove:
            del self.draining_host_ttl_cache[host]

    def process_warning_queue(self) -> bool:
        message_exist = False
        host_to_process = self.get_warned_host()
        if host_to_process:
            message_exist = True
            logger.info(f"Processing spot warning for {host_to_process.hostname}")

            # we should definitely ignore termination warnings that aren't from this
            # cluster or maybe not even paasta instances...
            if (
                host_to_process.group_id in self.spot_fleet_resource_groups
                or host_to_process.group_id in self.auto_scaling_resource_groups
            ):
                logger.info(f"Sending warned host to drain: {host_to_process.hostname}")
                self.submit_host_for_draining(host_to_process)
            else:
                logger.info(f"Ignoring warned host because not in our target group: {host_to_process.hostname}")
            self.delete_warning_messages([host_to_process])
        return message_exist

    def _drain_k8s_host(
        self, kube_operator_client: Optional[KubernetesClusterConnector], host_to_process: Host, disable_eviction: bool
    ) -> bool:
        if not k8s_drain(kube_operator_client, host_to_process.agent_id, disable_eviction):
            return False
        draining_time_milliseconds = self._get_spent_time_milliseconds(host_to_process)
        self.submit_host_for_termination(host_to_process, delay=0)
        logger.info(
            f"draining took {draining_time_milliseconds} milliseconds with "
            f"{host_to_process.attempt} attempt for {host_to_process.instance_id}"
        )
        self.draining_timer.record(
            draining_time_milliseconds,
            {
                "pool": host_to_process.pool,
                "reason": host_to_process.termination_reason,
            },
        )
        return True

    def _emit_draining_metrics(self, host: Host):
        self.draining_counter.count(
            1,
            {
                "pool": host.pool,
                "orphan": False if host.agent_id else True,
                "first_try": True if host.attempt == 1 else False,
                "reason": host.termination_reason,
            },
        )
        # We need to emit metrics only for first requests, because delay was added intentionally to other requests.
        if host.attempt == 1:
            self.receiving_timer.record(
                self._get_spent_time_milliseconds(host),
                {
                    "pool": host.pool,
                    "reason": host.termination_reason,
                },
            )

    def _get_spent_time_milliseconds(self, host: Host):
        return (arrow.now() - arrow.get(host.draining_start_time)).total_seconds() * 1000

    def terminate_host(self, host: Host) -> None:
        logger.info(f"Terminating: {host.instance_id}")
        if self.is_asg_cache_enabled and host.group_id in self.auto_scaling_resource_groups:
            # possibly take advantage of EC2 API caching for ASGs;
            # avoids re-listing all groups if the local ASG caching is disabled
            resource_group = self.auto_scaling_resource_groups[host.group_id]
            resource_group._reload_resource_group()
        else:
            resource_group_class = RESOURCE_GROUPS[host.sender]
            resource_group = resource_group_class(host.group_id)
        resource_group.terminate_instances_by_id([host.instance_id])

    def _list_resource_groups(
        self, scheduler: str, resource_group_class: Type[AWSResourceGroup]
    ) -> Dict[str, AWSResourceGroup]:
        result: Dict[str, AWSResourceGroup] = {}
        for pool in get_pool_name_list(self.cluster, scheduler):
            pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=pool, scheduler=scheduler))
            for resource_group_conf in pool_config.read_list("resource_groups"):
                if resource_group_class.FRIENDLY_NAME not in resource_group_conf:
                    continue
                result.update(
                    resource_group_class.load(
                        cluster=self.cluster,
                        pool=pool,
                        config=list(resource_group_conf.values())[0],
                    ),
                )
        return result

    @property
    @cachetools.cachedmethod(lambda self: self.spot_fleet_resource_groups_cache)
    def spot_fleet_resource_groups(self) -> Dict[str, AWSResourceGroup]:
        return self._list_resource_groups("mesos", SpotFleetResourceGroup)

    @property
    @cachetools.cachedmethod(lambda self: self.auto_scaling_resource_groups_cache)
    def auto_scaling_resource_groups(self) -> Dict[str, AWSResourceGroup]:
        return self._list_resource_groups("kubernetes", AutoScalingResourceGroup)


def host_from_instance_id(
    receipt_handle: str,
    instance_id: str,
    pool: Optional[str] = None,
    termination_reason: Optional[str] = None,
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
        group_ids = [tag["Value"] for tag in instance_data[0]["Tags"] if tag["Key"] in EC2_TAG_GROUP_KEYS]
        scheduler = "mesos"
        sender = RESOURCE_GROUPS_REV[SpotFleetResourceGroup]
        for tag in instance_data[0]["Tags"]:
            if tag["Key"] == "KubernetesCluster":
                scheduler = "kubernetes"
            if tag["Key"] == EC2_ASG_TAG_KEY:
                sender = RESOURCE_GROUPS_REV[AutoScalingResourceGroup]
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
    try:
        pool_from_ec2 = ""
        for tag in instance_data[0]["Tags"]:
            if tag["Key"] == EC2_IDENTIFIER_TAG_KEY:
                pool_from_ec2 = json.loads(tag["Value"]).get("pool", "")
    except Exception:
        logger.warning(f"Couldn't get pool name from {EC2_IDENTIFIER_TAG_KEY} tag for {instance_id}")

    return Host(
        agent_id=agent_id,
        sender=sender,
        receipt_handle=receipt_handle,
        instance_id=instance_id,
        hostname=hostnames[0],
        group_id=group_ids[0],
        ip=ip,
        pool=pool if pool else pool_from_ec2,  # getting pool from client and ec2 temporary, parameter will be deleted
        termination_reason=termination_reason if termination_reason else TerminationReason.SPOT_INTERRUPTION.value,
        scheduler=scheduler,
        draining_start_time=arrow.now().for_json(),
    )
