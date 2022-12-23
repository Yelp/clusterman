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
import json
from unittest import mock

import arrow
import pytest

from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.auto_scaling_resource_group import CLUSTERMAN_STALE_TAG
from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.client import S3ObjectWrapper
from clusterman.exceptions import NoLaunchTemplateConfiguredError
from clusterman.simulator.simulate_aws_market import simulate_InstanceMarket
from clusterman.util import ClustermanResources
from clusterman.util import DEFAULT_VOLUME_SIZE_GB


@pytest.fixture
def mock_launch_template():
    launch_template = {
        "LaunchTemplateName": "fake_launch_template",
        "LaunchTemplateData": {
            "ImageId": "ami-785db401",  # this AMI is hard-coded into moto, represents ubuntu xenial
            "InstanceType": "t2.2xlarge",
        },
    }
    ec2.create_launch_template(**launch_template)
    return launch_template


@pytest.fixture
def mock_asg_name():
    return "fake_asg"


@pytest.fixture
def mock_cluster():
    return "fake_cluster"


@pytest.fixture
def mock_pool():
    return "fake_pool"


@pytest.fixture
def mock_asg_config(mock_subnet, mock_launch_template, mock_asg_name, mock_cluster, mock_pool):
    asg = {
        "AutoScalingGroupName": mock_asg_name,
        "LaunchTemplate": {
            "LaunchTemplateName": "fake_launch_template",
            "Version": "1",
        },
        "MinSize": 1,
        "MaxSize": 30,
        "DesiredCapacity": 10,
        "AvailabilityZones": ["us-west-2a"],
        "VPCZoneIdentifier": mock_subnet["Subnet"]["SubnetId"],
        "Tags": [
            {
                "Key": "puppet:role::paasta",
                "Value": json.dumps({"pool": mock_pool, "paasta_cluster": mock_cluster}),
            },
            {
                "Key": "fake_tag_key",
                "Value": "fake_tag_value",
            },
        ],
        "NewInstancesProtectedFromScaleIn": True,
    }
    autoscaling.create_auto_scaling_group(**asg)

    return asg


@pytest.fixture
def mock_asrg(mock_asg_config):
    return AutoScalingResourceGroup(mock_asg_config["AutoScalingGroupName"])


@pytest.mark.parametrize("instance_type", ["t2.2xlarge", "m5.large"])
def test_market_weight(mock_asrg, instance_type):
    market_weight = mock_asrg.market_weight(simulate_InstanceMarket(instance_type, "us-west-2a"))
    assert market_weight == 1.0


@pytest.mark.parametrize("dry_run", [True, False])
def test_mark_stale(mock_asrg, dry_run):
    mock_asrg.mark_stale(dry_run)
    for inst in mock_asrg.instance_ids:
        tags = ec2.describe_tags(
            Filters=[
                {
                    "Name": "resource-id",
                    "Values": [inst],
                }
            ],
        )
        stale_tags = [tag for tag in tags["Tags"] if tag["Key"] == CLUSTERMAN_STALE_TAG]
        if dry_run:
            assert not stale_tags
        else:
            assert len(stale_tags) == 1


@pytest.mark.parametrize("stale_instances", [0, 7])
def test_modify_target_capacity_up(mock_asrg, stale_instances):
    new_desired_capacity = mock_asrg.target_capacity + 5
    with mock.patch(
        "clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.stale_instance_ids",
        mock.PropertyMock(return_value=mock_asrg.instance_ids[:stale_instances]),
    ):

        mock_asrg.modify_target_capacity(
            new_desired_capacity,
            dry_run=False,
            honor_cooldown=False,
        )

        new_config = mock_asrg._get_auto_scaling_group_config()
        assert new_config["DesiredCapacity"] == new_desired_capacity + stale_instances


@pytest.mark.parametrize("stale_instances", [0, 7])
def test_modify_target_capacity_down(mock_asrg, stale_instances):
    old_target_capacity = mock_asrg.target_capacity
    new_target_capacity = old_target_capacity - 5

    with mock.patch(
        "clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.stale_instance_ids",
        mock.PropertyMock(return_value=mock_asrg.instance_ids[:stale_instances]),
    ):
        mock_asrg.modify_target_capacity(
            new_target_capacity,
            dry_run=False,
            honor_cooldown=False,
        )

        new_config = mock_asrg._get_auto_scaling_group_config()
        # because some instances are stale, we might have to _increase_ our "real" target capacity
        # even if we're decreasing our _requested_ target capacity
        assert new_config["DesiredCapacity"] == new_target_capacity + stale_instances


@pytest.mark.parametrize("new_desired_capacity", [0, 100])
def test_modify_target_capacity_min_max(
    mock_asrg,
    mock_asg_config,
    new_desired_capacity,
):
    mock_asrg.modify_target_capacity(
        new_desired_capacity,
        dry_run=False,
        honor_cooldown=False,
    )

    new_config = mock_asrg._get_auto_scaling_group_config()
    if new_desired_capacity < mock_asg_config["MinSize"]:
        assert new_config["DesiredCapacity"] == mock_asg_config["MinSize"]
    elif new_desired_capacity > mock_asg_config["MaxSize"]:
        assert new_config["DesiredCapacity"] == mock_asg_config["MaxSize"]


def test_get_scale_up_options_no_launch_template(mock_asrg):
    mock_asrg._launch_template_config = None
    with pytest.raises(NoLaunchTemplateConfiguredError):
        mock_asrg.scale_up_options()


@pytest.mark.parametrize("overrides", [[], [{"InstanceType": "m4.5xlarge", "WeightedCapacity": "4"}]])
def test_get_scale_up_options_no_override(mock_asrg, overrides):
    mock_asrg._launch_template_overrides = overrides
    mock_asrg._get_options_for_instance_type = mock.MagicMock(return_value=[mock.Mock()])
    mock_asrg.scale_up_options()
    assert mock_asrg._get_options_for_instance_type.call_args == (
        mock.call("t2.2xlarge") if not overrides else mock.call("m4.5xlarge", 4.0)
    )


def test_get_launch_template_and_overrides_no_overrides(mock_asrg):
    lt, overrides = mock_asrg._get_launch_template_and_overrides()
    assert lt["LaunchTemplateName"] == "fake_launch_template"
    assert overrides == []


def test_get_launch_template_and_overrides_with_overrides(mock_asrg):
    expected_overrides = [
        {
            "InstanceType": "t2.2xlarge",
            "WeightedCapacity": 400,
        },
        {
            "InstanceType": "m5.12xlarge",
            "WeightedCapacity": 12345,
        },
    ]
    mock_asrg._group_config["MixedInstancesPolicy"] = {
        "LaunchTemplate": {
            "LaunchTemplateSpecification": mock_asrg._group_config["LaunchTemplate"],
            "Overrides": expected_overrides,
        },
    }
    del mock_asrg._group_config["LaunchTemplate"]
    lt, overrides = mock_asrg._get_launch_template_and_overrides()
    assert lt["LaunchTemplateName"] == "fake_launch_template"
    assert overrides == expected_overrides


def test_get_launch_template_and_overrides_with_launch_config(mock_asrg):
    mock_asrg._group_config = {"LaunchConfigurationName": "fake-launch-config"}
    assert mock_asrg._get_launch_template_and_overrides() == (None, [])


def test_get_options_for_instance_type(mock_asrg):
    mock_asrg._group_config["AvailabilityZones"] = ["us-west-1a", "us-west-2a"]
    result = mock_asrg._get_options_for_instance_type("m5.4xlarge")
    assert len(result) == 2
    assert all(
        [
            r.agent.total_resources
            == ClustermanResources(
                cpus=16,
                mem=64 * 1024,
                disk=DEFAULT_VOLUME_SIZE_GB * 1024,
                gpus=0,
            )
            for r in result
        ]
    )
    assert result[0].instance.market == simulate_InstanceMarket("m5.4xlarge", "us-west-1a")
    assert result[1].instance.market == simulate_InstanceMarket("m5.4xlarge", "us-west-2a")


@pytest.mark.parametrize("stale_instances", [0, 1, 10])
def test_status(mock_asrg, stale_instances):
    is_stale = stale_instances == 10
    with mock.patch(
        "clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.is_stale",
        new_callable=mock.PropertyMock(return_value=is_stale),
    ), mock.patch(
        "clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.stale_instance_ids",
        new_callable=mock.PropertyMock(return_value=mock_asrg.instance_ids[:stale_instances]),
    ):
        status = mock_asrg.status
        if stale_instances == 0:
            assert status == "active"
        elif stale_instances > 0:
            assert status == "rolling"


def test_get_asg_tags(mock_asrg, mock_asg_config):
    asg_id_to_tags = mock_asrg._get_resource_group_tags()

    assert mock_asg_config["AutoScalingGroupName"] in asg_id_to_tags
    tags = asg_id_to_tags[mock_asg_config["AutoScalingGroupName"]]
    assert "fake_tag_key" in tags
    assert tags["fake_tag_key"] == "fake_tag_value"


@mock.patch("clusterman.aws.aws_resource_group.cached_s3_get_object")
def test_load_from_cache_data(mock_get_obj):
    mock_data = {"g1": {}, "g2": {}}
    mock_get_obj.return_value = S3ObjectWrapper(json.dumps(mock_data).encode(), arrow.utcnow())
    with mock.patch.object(AutoScalingResourceGroup, "_get_instances_by_market"), mock.patch.object(
        AutoScalingResourceGroup, "_reload_resource_group"
    ):
        groups = AutoScalingResourceGroup.load("foo", "bar", {"aws_api_cache_bucket": "some-bucket"})
    assert all(k in groups and k == groups[k].group_id for k in mock_data)
    mock_get_obj.assert_called_once_with("some-bucket", "asg/foo/bar.json")


@mock.patch("clusterman.aws.aws_resource_group.cached_s3_get_object")
def test_get_auto_scaling_group_config_cached(mock_get_obj):
    mock_data = {"some-name": {"foo": 123}}
    mock_get_obj.return_value = S3ObjectWrapper(json.dumps(mock_data).encode(), arrow.utcnow())
    with mock.patch.object(AutoScalingResourceGroup, "_get_instances_by_market"), mock.patch.object(
        AutoScalingResourceGroup, "_reload_resource_group"
    ):
        asg = AutoScalingResourceGroup(
            "some-name", aws_api_cache_bucket="some-bucket", aws_api_cache_key="some-key.json"
        )
        assert asg._get_auto_scaling_group_config() == {"foo": 123}
