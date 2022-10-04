import os
from argparse import Namespace
from unittest import mock

import pytest
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm

from clusterman.kubernetes.util import CachedCoreV1Api
from clusterman.kubernetes.util import ConciseCRDApi
from clusterman.kubernetes.util import get_node_kernel_version
from clusterman.kubernetes.util import get_node_lsbrelease
from clusterman.kubernetes.util import ResourceParser
from clusterman.kubernetes.util import selector_term_matches_requirement


@pytest.fixture
def mock_cached_core_v1_api():
    with mock.patch("clusterman.kubernetes.util.kubernetes"):
        yield CachedCoreV1Api("/foo/bar/admin.conf")


def test_cached_corev1_api_no_kubeconfig(caplog):
    with pytest.raises(TypeError):
        CachedCoreV1Api("/foo/bar/admin.conf")
        assert "Could not load KUBECONFIG" in caplog.text


def test_cached_corev1_api_caches_non_cached_function(mock_cached_core_v1_api):
    mock_cached_core_v1_api.list_namespace()
    assert mock_cached_core_v1_api._client.list_namespace.call_count == 1


def test_cached_corev1_api_caches_cached_function_no_env_var(mock_cached_core_v1_api):
    mock_cached_core_v1_api.list_node()
    mock_cached_core_v1_api.list_node()
    assert mock_cached_core_v1_api._client.list_node.call_count == 2


def test_cached_corev1_api_caches_cached_function(mock_cached_core_v1_api):
    with mock.patch.dict(os.environ, {"KUBE_CACHE_ENABLED": "true"}):
        mock_cached_core_v1_api.list_node()
        mock_cached_core_v1_api.list_node()
    assert mock_cached_core_v1_api._client.list_node.call_count == 1


def test_resource_parser_cpu():
    assert ResourceParser.cpus({"cpu": "2"}) == 2.0
    assert ResourceParser.cpus({"cpu": "500m"}) == 0.5


def test_resource_parser_mem():
    assert ResourceParser.mem({"memory": "1Gi"}) == 1000.0


def test_resource_parser_disk():
    assert ResourceParser.disk({"ephemeral-storage": "1Gi"}) == 1000.0


def test_resource_parser_gpus():
    assert ResourceParser.gpus({"nvidia.com/gpu": "3"}) == 3


def test_resource_parser_gpus_non_integer():
    with pytest.raises(ValueError):
        ResourceParser.gpus({"nvidia.com/gpu": "3.5"})


def test_selector_term_matches_requirement():
    selector_term = [
        V1NodeSelectorTerm(
            match_expressions=[
                V1NodeSelectorRequirement(key="clusterman.com/scheduler", operator="Exists"),
                V1NodeSelectorRequirement(key="clusterman.com/pool", operator="In", values=["bar"]),
            ]
        )
    ]
    selector_requirement = V1NodeSelectorRequirement(key="clusterman.com/pool", operator="In", values=["bar"])
    assert selector_term_matches_requirement(selector_term, selector_requirement)


@mock.patch("clusterman.kubernetes.util.kubernetes")
def test_concise_crd_api(mock_kube):
    api = ConciseCRDApi("/foo/bar/admin.conf", "group-x", "v-y", "plur-z")
    api.list_node_migration_resources(label_selector="foo=bar")
    mock_kube.client.CustomObjectsApi.return_value.list_node_migration_resources.assert_called_once_with(
        group="group-x",
        plural="plur-z",
        version="v-y",
        label_selector="foo=bar",
    )


@pytest.mark.parametrize(
    "node_info,expected",
    (
        # using an argparse namespace to simulate system info object
        (Namespace(), ""),
        (Namespace(kernel_version="1.2.3"), "1.2.3"),
    ),
)
def test_get_node_kernel_version(node_info, expected):
    node = mock.MagicMock()
    node.status.node_info = node_info
    assert get_node_kernel_version(node) == expected


@pytest.mark.parametrize(
    "node_info,expected",
    (
        # using an argparse namespace to simulate system info object
        (Namespace(), ""),
        (Namespace(os_image="Some 1.2.3 foobar"), "1.2.3"),
        (Namespace(os_image="Some 1.02 foobar"), "1.02"),
        (Namespace(os_image="1.2.3"), "1.2.3"),
    ),
)
def test_get_node_lsbrelease(node_info, expected):
    node = mock.MagicMock()
    node.status.node_info = node_info
    assert get_node_lsbrelease(node) == expected
