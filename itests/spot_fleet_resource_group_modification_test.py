from collections import defaultdict

import mock
import pytest
import yaml

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.mesos.mesos_role_manager import DEFAULT_ROLE_CONFIG
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.mesos_role_manager import SERVICES_FILE
from tests.conftest import mock_open
from tests.mesos.conftest import mock_aws_client_setup
from tests.mesos.conftest import mock_service_config
from tests.mesos.conftest import setup_ec2
from tests.mesos.mesos_role_manager_test import mock_role_config
from tests.mesos.spot_fleet_resource_group_test import mock_spot_fleet_resource_group
from tests.mesos.spot_fleet_resource_group_test import mock_subnet


pytest.mark.usefixtures(mock_aws_client_setup, mock_service_config, setup_ec2)


@pytest.fixture
def mock_sfrs(setup_ec2):
    sfrgs = [mock_spot_fleet_resource_group(mock_subnet()) for i in range(5)]
    for sfrg in sfrgs:
        ec2.modify_spot_fleet_request(SpotFleetRequestId=sfrg.id, TargetCapacity=1)
    return sfrgs


@pytest.fixture
def mock_manager(mock_service_config, mock_aws_client_setup, mock_sfrs):
    role_config_file = DEFAULT_ROLE_CONFIG.format(name='my-role')
    with mock_open(role_config_file, yaml.dump(mock_role_config())), \
            mock_open(SERVICES_FILE, 'the.mesos.leader:\n  host: foo\n  port: 1234'), \
            mock.patch('clusterman.mesos.mesos_role_manager.load_spot_fleets_from_s3') as mock_load:
        mock_load.return_value = mock_sfrs
        return MesosRoleManager('my-role', 'mesos-test')


def test_target_capacity(mock_manager):
    assert mock_manager.target_capacity == 5


def test_scale_up(mock_manager, mock_sfrs):
    mock_manager.max_capacity = 101
    mock_manager.modify_target_capacity(53)
    assert {rg.target_capacity for rg in mock_manager.resource_groups} == {11, 11, 11, 10, 10}

    ec2.modify_spot_fleet_request(SpotFleetRequestId=mock_sfrs[0].id, TargetCapacity=13)
    mock_manager.modify_target_capacity(76)
    assert {rg.target_capacity for rg in mock_manager.resource_groups} == {16, 15, 15, 15, 15}

    ec2.modify_spot_fleet_request(SpotFleetRequestId=mock_sfrs[3].id, TargetCapacity=30)
    mock_manager.modify_target_capacity(1000)
    assert {rg.target_capacity for rg in mock_manager.resource_groups} == {18, 18, 18, 30, 17}


# TODO (CLUSTERMAN-97) the scale_down itests need some efficiency improvements in moto before it's feasible
# to run them, so I'm going to delay implementing this further for right now
def TODO_scale_down(mock_manager):
    mock_manager.max_capacity = 101
    mock_manager.modify_target_capacity(1000)
    print(mock_manager.target_capacity, mock_manager.fulfilled_capacity)
    with mock.patch('clusterman.mesos.mesos_role_manager.MesosRoleManager._idle_agents_by_market') as mock_idle:
        idle_agents = defaultdict(list)
        for rg in mock_manager.resource_groups:
            for instance in ec2_describe_instances(instance_ids=rg.instances):
                idle_agents[get_instance_market(instance)].append(instance['InstanceId'])

        print(idle_agents)
        mock_idle.return_value = idle_agents
        mock_manager.modify_target_capacity(80)
        print(mock_manager.target_capacity, mock_manager.fulfilled_capacity)