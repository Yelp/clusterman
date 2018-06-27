import mock
import pytest
from spotinst_sdk import SpotinstClient

from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket
from clusterman.aws.markets import subnet_to_az
from clusterman.mesos.spotinst_resource_group import get_spotinst_tags
from clusterman.mesos.spotinst_resource_group import load_elastigroups
from clusterman.mesos.spotinst_resource_group import SpotInstResourceGroup
from tests.mesos.spot_fleet_resource_group_test import mock_sfr_response
from tests.mesos.spot_fleet_resource_group_test import mock_subnet


pytest.mark.usefixtures(mock_sfr_response, mock_subnet)


class SpotinstClientEmulator(object):

    def __init__(self, active_sfr, sfrs=None) -> None:
        if sfrs is None:
            sfrs = []
        self._sig_id_to_sfr_ids = {'sig-deadbeef': [active_sfr] + sfrs}

    def update_elastigroup(self, group_update, group_id):
        ec2.modify_spot_fleet_request(
            SpotFleetRequestId=self._sig_id_to_sfr_ids[group_id][0],
            TargetCapacity=int(group_update.capacity.target),
            ExcessCapacityTerminationPolicy='Default',
        )

    def detach_elastigroup_instances(self, group_id, detach_configuration):
        assert detach_configuration.should_terminate_instances
        assert detach_configuration.should_decrement_target_capacity
        # FIXME: decrease capacity before removing instances
        ec2.terminate_instances(InstanceIds=detach_configuration.instances_to_detach)

    def get_elastigroup_active_instances(self, group_id):
        sfr_ids = self._sig_id_to_sfr_ids[group_id]
        return [
            {
                "availability_zone": instance.get('Placement', {}).get('AvailabilityZone'),
                "created_at": instance.get("LaunchTime"),
                "group_id": group_id,
                "instance_id": instance["InstanceId"],
                "instance_type": instance["InstanceType"],
                "private_ip": instance.get("PrivateIpAddress"),
                "product": "Linux/UNIX (Amazon VPC)",  # FIXME?
                "public_ip": None,  # FIXME?
                "spot_instance_request_id": instance['SpotInstanceRequestId'],
                "status": "fulfilled",  # FIXME?
            }
            for sfr_id in sfr_ids
            for page in ec2.get_paginator('describe_spot_fleet_instances').paginate(SpotFleetRequestId=sfr_id)
            for instance in page['ActiveInstances']
        ]

    def get_elastigroup(self, group_id):
        sfr_ids = self._sig_id_to_sfr_ids[group_id]
        sfrs = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=sfr_ids)['SpotFleetRequestConfigs']
        for sfr in sfrs:
            if sfr['SpotFleetRequestId'] == sfr_ids[0]:
                active_sfr = sfr
                break
        return {
            "capacity": {
                "maximum": 5,  # FIXME?
                "minimum": 1,  # FIXME?
                "target": sum(
                    sfr['SpotFleetRequestConfig']["FulfilledCapacity"]
                    for sfr in sfrs
                ),
                "unit": "weight"
            },
            "compute": {
                "availability_zones": [
                    {
                        "name": subnet_to_az(spec['SubnetId']),
                        "subnet_id": spec['SubnetId'],
                        "subnet_ids": [
                            spec['SubnetId']
                        ]
                    }
                    for sfr in sfrs
                    for spec in sfr['SpotFleetRequestConfig']['LaunchSpecifications']
                ],
                "instance_types": {
                    "ondemand": None,  # FIXME?
                    "spot": list({
                        spec["InstanceType"]
                        for sfr in sfrs
                        for spec in sfr['SpotFleetRequestConfig']["LaunchSpecifications"]
                    }),
                    "weights": [
                        {
                            "instance_type": instance_type,
                            "weighted_capacity": capacity,
                        }
                        for instance_type, capacity in dict(
                            {
                                spec.get("InstanceType"): spec.get("WeightedCapacity")
                                for sfr in sfrs
                                for spec in sfr['SpotFleetRequestConfig']["LaunchSpecifications"]
                            },
                            **{
                                spec.get("InstanceType"): spec.get("WeightedCapacity")
                                for spec in active_sfr['SpotFleetRequestConfig']["LaunchSpecifications"]
                            },
                        ).items()
                    ]
                },
                "launch_specification": {
                    "iam_role": {
                        "arn": active_sfr['SpotFleetRequestConfig']["LaunchSpecifications"][0]["IamInstanceProfile"]["Arn"],
                        "name": active_sfr['SpotFleetRequestConfig'].get("IamFleetRole"),
                    },
                    "image_id": active_sfr['SpotFleetRequestConfig']["LaunchSpecifications"][0]["ImageId"],
                    "monitoring": False,
                    "security_group_ids": [
                        secgroup
                        for sfr in sfrs
                        for spec in sfr['SpotFleetRequestConfig']["LaunchSpecifications"]
                        for secgroup in spec["SecurityGroups"]
                    ],
                    "tags": [
                        {
                            "tag_key": tag['Key'],
                            "tag_value": tag['Value'],
                        }
                        for tag in active_sfr['SpotFleetRequestConfig']["LaunchSpecifications"][0].get(
                            "TagSpecifications", [{}])[0].get("Tags", [])
                    ]
                },
                "product": "Linux/UNIX (Amazon VPC)"  # FIXME?
            },
            "created_at": None,  # FIXME? "2018-06-08T18:04:36.000Z",
            "id": group_id,
            "name": "MVP",  # FIXME?
            "strategy": {
                "availability_vs_cost": active_sfr['SpotFleetRequestConfig']["AllocationStrategy"],
                "fallbackToOd": False,  # FIXME?
                "risk": 100,  # FIXME?
                "utilize_reserved_instances": False,  # FIXME?
            },
            "updated_at": None,  # FIXME? "2018-06-19T18:04:58.000Z"
        }

    def get_elastigroups(self):
        return [
            self.get_elastigroup(sig_id)
            for sig_id in self._sig_id_to_sfr_ids.keys()
        ]


@pytest.fixture
def mock_spotinst_client():
    yield mock.Mock(spec_set=SpotinstClient)


@pytest.fixture
def mock_get_spotinst_client(mock_spotinst_client):
    with mock.patch(
        'clusterman.mesos.spotinst_resource_group.get_spotinst_client',
    ) as mock_get_spotinst_client:
        mock_get_spotinst_client.return_value = mock_spotinst_client
        yield mock_get_spotinst_client


@pytest.fixture
def mock_spotinst_resource_group(mock_sfr_response, mock_spotinst_client):
    client = SpotinstClientEmulator(mock_sfr_response['SpotFleetRequestId'])
    return SpotInstResourceGroup(list(client._sig_id_to_sfr_ids.keys())[0], client)


def test_load_elastigroups(mock_spotinst_client, mock_get_spotinst_client):
    with mock.patch(
        'clusterman.mesos.spotinst_resource_group.SpotInstResourceGroup.__init__',
    ) as mock_init, mock.patch(
        'clusterman.mesos.spotinst_resource_group.get_spotinst_tags',
    ) as mock_get_spotinst_tags:
        mock_init.return_value = None
        mock_get_spotinst_tags.return_value = {
            'sig-123': {
                'some': 'tag',
                'puppet:role::paasta': '{"pool": "default", "paasta_cluster": "westeros-prod"}'
            },
            'sig-456': {
                'some': 'tag',
                'puppet:role::paasta': '{"pool": "another", "paasta_cluster": "westeros-prod"}'
            },
            'sig-789': {
                'some': 'tag',
                'puppet:role::paasta': '{"paasta_cluster": "westeros-prod"}'
            },
        }
        sigs = load_elastigroups(cluster='westeros-prod', pool='default')
        assert len(sigs) == 1
        mock_init.assert_called_with('sig-123', mock_spotinst_client)


def test_get_spotinst_tags(mock_spotinst_client):
    mock_spotinst_client.get_elastigroups.return_value = [
        {
            "id": "sig-12",
            "compute": {
                "launch_specification": {
                    "tags": [
                        {
                            "tag_key": "foo",
                            "tag_value": "bar",
                        },
                    ]
                },
            },
        },
        {
            "id": "sig-34",
            "compute": {
                "launch_specification": {},
            },
        },
        {
            "id": "sig-56",
            "compute": {
                "launch_specification": {
                    "tags": [],
                },
            },
        },
        {
            "id": "sig-78",
            "compute": {
                "launch_specification": {
                    "tags": [
                        {
                            "tag_key": "foo",
                            "tag_value": "bar",
                        },
                        {
                            "tag_key": "spam",
                            "tag_value": "baz",
                        },
                    ]
                },
            },
        },
    ]
    tags = get_spotinst_tags(mock_spotinst_client)
    expected = {
        'sig-12': {
            'foo': 'bar'
        },
        'sig-34': {},
        'sig-56': {},
        'sig-78': {
            'foo': 'bar',
            'spam': 'baz'
        }
    }
    assert tags == expected


def test_fulfilled_capacity(mock_spotinst_resource_group):
    assert mock_spotinst_resource_group.fulfilled_capacity == 11


def test_modify_target_capacity_up(mock_spotinst_resource_group):
    mock_spotinst_resource_group.modify_target_capacity(20)
    assert mock_spotinst_resource_group.target_capacity == 20
    assert len(mock_spotinst_resource_group.instance_ids) == 13


def test_modify_target_capacity_down_no_terminate(mock_spotinst_resource_group):
    mock_spotinst_resource_group.modify_target_capacity(5)
    assert mock_spotinst_resource_group.target_capacity == 5
    assert mock_spotinst_resource_group.fulfilled_capacity == 11
    assert len(mock_spotinst_resource_group.instance_ids) == 7


def test_modify_target_capacity_down_terminate(mock_spotinst_resource_group):
    mock_spotinst_resource_group.modify_target_capacity(5, terminate_excess_capacity=True)
    assert mock_spotinst_resource_group.target_capacity == 5
    assert mock_spotinst_resource_group.fulfilled_capacity == 5
    assert len(mock_spotinst_resource_group.instance_ids) == 4


def test_modify_target_capacity_dry_run(mock_spotinst_resource_group):
    mock_spotinst_resource_group.modify_target_capacity(5, dry_run=True)
    assert mock_spotinst_resource_group.target_capacity == 11
    assert mock_spotinst_resource_group.fulfilled_capacity == 11


def test_terminate_all_instances_by_id(mock_spotinst_resource_group):
    mock_spotinst_resource_group.terminate_instances_by_id(mock_spotinst_resource_group.instance_ids)
    assert mock_spotinst_resource_group.instance_ids == []


@mock.patch('clusterman.mesos.spotinst_resource_group.logger')
def test_terminate_no_instances_by_id(mock_logger, mock_spotinst_resource_group):
    mock_spotinst_resource_group.terminate_instances_by_id([])
    assert len(mock_spotinst_resource_group.instance_ids) == 7
    assert mock_spotinst_resource_group.target_capacity == 11
    assert mock_spotinst_resource_group.fulfilled_capacity == 11
    assert mock_logger.warn.call_count == 1


def test_instances(mock_spotinst_resource_group):
    assert len(mock_spotinst_resource_group.instance_ids) == 7


def test_market_capacities(mock_spotinst_resource_group):
    assert mock_spotinst_resource_group.market_capacities == {
        InstanceMarket('c3.8xlarge', None): 8,
        InstanceMarket('i2.4xlarge', None): 3,
    }
