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
import os
import re
import socket
from functools import partial
from typing import Any
from typing import Hashable
from typing import List
from typing import MutableMapping
from typing import Type

import colorlog
import kubernetes
from cachetools import TTLCache
from cachetools.keys import hashkey
from humanfriendly import parse_size
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod
from kubernetes.config.config_exception import ConfigException

from clusterman.util import ClustermanResources


# If a container does not specify a resource request, Kubernetes makes up
# numbers for the purposes of scheduling.  I think it makes the most sense
# to use the same made-up numbers here.
#
# https://github.com/kubernetes/kubernetes/blob/1c11ff7a26c498dc623f060aa30c7c970f3d3eee/pkg/scheduler/util/non_zero.go#L34
DEFAULT_KUBERNETES_CPU_REQUEST = "100m"
DEFAULT_KUBERNETES_MEMORY_REQUEST = "200MB"
DEFAULT_KUBERNETES_DISK_REQUEST = "0"  # Kubernetes doesn't schedule based on disk allocation right now
KUBERNETES_API_CACHE_SIZE = 16
KUBERNETES_API_CACHE_TTL = 60
KUBERNETES_API_CACHE: MutableMapping[Hashable, Any] = TTLCache(
    maxsize=KUBERNETES_API_CACHE_SIZE, ttl=KUBERNETES_API_CACHE_TTL
)
VERSION_MATCH_EXPR = re.compile(r"(\W|^)(?P<release>\d+\.\d+(\.\d+)?)(\W|$)")
MILLIBYTE_MATCH_EXPR = re.compile(r"(\d+)m$")
logger = colorlog.getLogger(__name__)


class KubeApiClientWrapper:
    def __init__(self, kubeconfig_path: str, client_class: Type) -> None:
        """Init k8s API client

        :param str kubeconfig_path: k8s configuration path
        :param Type client_class: k8s client class to initialize
        """
        try:
            kubernetes.config.load_kube_config(kubeconfig_path, context=os.getenv("KUBECONTEXT"))
        except ConfigException:
            error_msg = "Could not load KUBECONFIG; is this running on Kubernetes master?"
            if "yelpcorp" in socket.getfqdn():
                error_msg += "\nHint: try using the clusterman-k8s-<clustername> wrapper script!"
            logger.error(error_msg)
            raise

        self._client = client_class()

    def __getattr__(self, attr):
        return getattr(self._client, attr)


class CachedCoreV1Api(KubeApiClientWrapper):
    CACHED_FUNCTION_CALLS = {"list_node", "list_pod_for_all_namespaces"}

    def __init__(self, kubeconfig_path: str):
        super().__init__(kubeconfig_path, kubernetes.client.CoreV1Api)

    def __getattr__(self, attr):
        global KUBERNETES_API_CACHE
        func = getattr(self._client, attr)

        if os.environ.get("KUBE_CACHE_ENABLED", "") and attr in self.CACHED_FUNCTION_CALLS:

            def decorator(f):
                def wrapper(*args, **kwargs):
                    k = hashkey(attr, *args, **kwargs)
                    try:
                        return KUBERNETES_API_CACHE[k]
                    except KeyError:
                        logger.debug(f"Cache miss for the {attr} Kubernetes function call")
                        pass  # the function call hasn't been cached recently
                    v = f(*args, **kwargs)
                    KUBERNETES_API_CACHE[k] = v
                    return v

                return wrapper

            func = decorator(func)

        return func


class ConciseCRDApi(KubeApiClientWrapper):
    def __init__(self, kubeconfig_path: str, group: str, version: str, plural: str) -> None:
        super().__init__(kubeconfig_path, kubernetes.client.CustomObjectsApi)
        self.group = group
        self.version = version
        self.plural = plural

    def __getattr__(self, attr):
        return partial(
            getattr(self._client, attr),
            group=self.group,
            version=self.version,
            plural=self.plural,
        )


class ResourceParser:
    @staticmethod
    def cpus(resources):
        resources = resources or {}
        cpu_str = resources.get("cpu", DEFAULT_KUBERNETES_CPU_REQUEST)
        if cpu_str[-1] == "m":
            return float(cpu_str[:-1]) / 1000
        else:
            return float(cpu_str)

    @staticmethod
    def mem(resources):
        resources = resources or {}
        # CLUSTERMAN-729 temporary fix while adding milli-byte support to humanfriendly
        memory = resources.get("memory", DEFAULT_KUBERNETES_MEMORY_REQUEST)
        result = MILLIBYTE_MATCH_EXPR.search(memory)
        if result:
            memory = result.group(1)
            memory = str(int(memory) // 1000)
        return parse_size(memory) / 1000000

    @staticmethod
    def disk(resources):
        resources = resources or {}
        return parse_size(resources.get("ephemeral-storage", DEFAULT_KUBERNETES_DISK_REQUEST)) / 1000000

    @staticmethod
    def gpus(resources):
        resources = resources or {}
        return int(resources.get("nvidia.com/gpu", 0))


def allocated_node_resources(pods: List[KubernetesPod]) -> ClustermanResources:
    cpus = mem = disk = gpus = 0
    for pod in pods:
        cpus += sum(ResourceParser.cpus(c.resources.requests) for c in pod.spec.containers)
        mem += sum(ResourceParser.mem(c.resources.requests) for c in pod.spec.containers)
        disk += sum(ResourceParser.disk(c.resources.requests) for c in pod.spec.containers)
        gpus += sum(ResourceParser.gpus(c.resources.requests) for c in pod.spec.containers)

    return ClustermanResources(
        cpus=cpus,
        mem=mem,
        disk=disk,
        gpus=gpus,
    )


def get_node_ip(node: KubernetesNode) -> str:
    for address in node.status.addresses:
        if address.type == "InternalIP":
            return address.address
    raise ValueError('Kubernetes node {node.metadata.name} has no "InternalIP" address')


def get_node_kernel_version(node: KubernetesNode) -> str:
    """Get kernel version from node info

    :param KubernetesNode node: k8s node object
    :return: kernel version if present
    """
    return getattr(node.status.node_info, "kernel_version", "")


def get_node_lsbrelease(node: KubernetesNode) -> str:
    """Get operating system release from node info

    :param KubernetesNode node: k8s node object
    :return: os release if present
    """
    os_image = getattr(node.status.node_info, "os_image", "")
    m = VERSION_MATCH_EXPR.search(os_image)
    return m.group("release") if m else ""


def total_node_resources(node: KubernetesNode, excluded_pods: List[KubernetesPod]) -> ClustermanResources:
    base_total = ClustermanResources(
        cpus=ResourceParser.cpus(node.status.allocatable),
        mem=ResourceParser.mem(node.status.allocatable),
        disk=ResourceParser.disk(node.status.allocatable),
        gpus=ResourceParser.gpus(node.status.allocatable),
    )
    excluded_resources = allocated_node_resources(excluded_pods)
    return base_total - excluded_resources


def total_pod_resources(pod: KubernetesPod) -> ClustermanResources:
    return ClustermanResources(
        cpus=sum(ResourceParser.cpus(c.resources.requests) for c in pod.spec.containers),
        mem=sum(ResourceParser.mem(c.resources.requests) for c in pod.spec.containers),
        disk=sum(ResourceParser.disk(c.resources.requests) for c in pod.spec.containers),
        gpus=sum(ResourceParser.gpus(c.resources.requests) for c in pod.spec.containers),
    )


def selector_term_matches_requirement(
    selector_terms: List[V1NodeSelectorTerm],
    selector_requirement: V1NodeSelectorRequirement,
) -> bool:
    for selector_term in selector_terms:
        if selector_term.match_expressions:
            for match_expression in selector_term.match_expressions:
                if match_expression == selector_requirement:
                    return True
    return False
