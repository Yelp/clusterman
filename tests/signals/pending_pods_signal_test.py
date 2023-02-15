from unittest import mock

import pytest
import staticconf
import staticconf.testing
from kubernetes.client import V1Container
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodCondition
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements

from clusterman.signals.pending_pods_signal import PendingPodsSignal
from clusterman.util import ClustermanResources
from clusterman.util import SignalResourceRequest


@pytest.fixture
def pending_pods_signal():
    with staticconf.testing.PatchConfiguration({"autoscale_signal.period_minutes": 5}, namespace="bar.kube_config"):
        return PendingPodsSignal(
            "foo",
            "bar",
            "kube",
            "app1",
            "bar.kube_config",
            mock.Mock(),
            mock.Mock(get_unschedulable_pods=mock.Mock(return_value=[])),
        )


@pytest.fixture
def allocated_resources():
    return ClustermanResources(cpus=150, mem=1000, disk=500, gpus=0)


@pytest.fixture
def pending_pods():
    return [
        V1Pod(
            metadata=V1ObjectMeta(name="pod1"),
            status=V1PodStatus(
                phase="Pending",
                conditions=[V1PodCondition(status="False", type="PodScheduled", reason="Unschedulable")],
            ),
            spec=V1PodSpec(
                containers=[
                    V1Container(
                        name="container1",
                        resources=V1ResourceRequirements(requests={"cpu": "1.5", "memory": "150MB"}),
                    ),
                    V1Container(
                        name="container1",
                        resources=V1ResourceRequirements(requests={"cpu": "1.5", "memory": "350MB"}),
                    ),
                ]
            ),
        ),
    ]


# Just the existing resources, no pending pods
def test_get_resource_request_no_pending_pods(allocated_resources, pending_pods_signal):
    assert pending_pods_signal._get_resource_request(allocated_resources) == SignalResourceRequest(
        cpus=150,
        mem=1000,
        disk=500,
        gpus=0,
    )


# Just the increase from pending pods (2 pods with 1.5 CPU = 3 x default multiplier 2), with no existing resources
def test_get_resource_request_only_pending_pods(pending_pods, pending_pods_signal):
    assert pending_pods_signal._get_resource_request(ClustermanResources(), pending_pods) == SignalResourceRequest(
        cpus=6,
        mem=1000,
        disk=0,
        gpus=0,
    )


# Existing resources AND pending pods, so sum of both
def test_get_resource_request_pending_pods_and_metrics(allocated_resources, pending_pods, pending_pods_signal):
    assert pending_pods_signal._get_resource_request(allocated_resources, pending_pods) == SignalResourceRequest(
        cpus=156,
        mem=2000,
        disk=500,
        gpus=0,
    )


# Increase from pending pods but higher multipler
def test_get_resource_request_only_pending_pods_custom_multipler(pending_pods, pending_pods_signal):
    pending_pods_signal.parameters["pending_pods_multiplier"] = 20
    assert pending_pods_signal._get_resource_request(ClustermanResources(), pending_pods) == SignalResourceRequest(
        cpus=60,
        mem=10000,
        disk=0,
        gpus=0,
    )
