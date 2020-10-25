import pytest
from kubernetes.client.models import V1Taint
from kubernetes.client.models import V1Toleration

from clusterman.kubernetes.util import is_node_tolerable


def test_is_node_tolerable_no_taints(pod1):
    assert is_node_tolerable(pod1, [])


def test_is_node_tolerable_with_taint(pod1):
    taint = V1Taint(effect='NoExecute', key='clusterman.com/tainted', value='true')
    assert not is_node_tolerable(pod1, [taint])


@pytest.mark.parametrize('key', ['', 'clusterman.com/tainted'])
@pytest.mark.parametrize('operator,value,effect', [
    ('', 'true', ''),
    ('Equal', 'true', 'NoExecute'),
    ('Exists', '', ''),
    ('Exists', '', 'NoExecute'),
])
def test_is_node_tolerable_with_taint_and_matching_toleration(pod1, key, value, effect, operator):
    taint = V1Taint(effect='NoExecute', key='clusterman.com/tainted', value='true')
    pod1.spec.tolerations = [V1Toleration(key=key, value=value, effect=effect, operator=operator)]
    assert is_node_tolerable(pod1, [taint])


@pytest.mark.parametrize('key,operator,value,effect', [
    ('clusterman.com/other', '', 'true', ''),
    ('clusterman.com/other', 'Exists', '', ''),
    ('clusterman.com/tainted', '', 'false', ''),
    ('clusterman.com/tainted', 'Equal', 'false', 'NoExecute'),
    ('clusterman.com/tainted', '', 'false', 'NoSchedule'),
    ('clusterman.com/tainted', '', 'true', 'NoSchedule'),
])
def test_is_node_tolerable_with_taint_and_non_matching_toleration(pod1, key, value, effect, operator):
    taint = V1Taint(effect='NoExecute', key='clusterman.com/tainted', value='true')
    pod1.spec.tolerations = [V1Toleration(key=key, value=value, effect=effect, operator=operator)]
    assert not is_node_tolerable(pod1, [taint])


def test_is_node_tolerable_with_multiple_taints_and_non_matching_toleration(pod1):
    taint1 = V1Taint(effect='NoExecute', key='clusterman.com/tainted', value='true')
    taint2 = V1Taint(effect='NoExecute', key='clusterman.com/tainted2', value='true')
    pod1.spec.tolerations = [V1Toleration(key='clusterman.com/tainted', value='true')]
    assert not is_node_tolerable(pod1, [taint1, taint2])


def test_is_node_tolerable_prefer_no_schedule(pod1):
    taint = V1Taint(effect='PreferNoSchedule', key='clusterman.com/tainted', value='true')
    pod1.spec.tolerations = [V1Toleration(key='clusterman.com/tainted', value='false')]
    assert is_node_tolerable(pod1, [taint])
