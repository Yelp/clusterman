import socket
import sys

import arrow
import humanize

from clusterman.args import add_cluster_arg
from clusterman.args import add_role_arg
from clusterman.args import subparser
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.mesos.util import allocated_cpu_resources
from clusterman.util import colored_status


class MesosAgentState:
    IDLE = 'no tasks'
    ORPHANED = 'orphan'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


def _write_resource_group_line(group):
    # TODO (CLUSTERMAN-100) These are just the status responses for spot fleets; this probably won't
    # extend to other types of resource groups, so we should figure out what to do about that.
    status_str = colored_status(
        group.status,
        green=('active',),
        blue=('modifying', 'submitted'),
        red=('cancelled', 'failed', 'cancelled_running', 'cancelled_terminating'),
    )
    print(f'\t{group.id}: {status_str} ({group.fulfilled_capacity} / {group.target_capacity})')


def _write_instance_line(instance, postfix=None):
    postfix = postfix or ''
    instance_status_str = colored_status(
        instance['State']['Name'],
        green=('running',),
        blue=('pending',),
        red=('shutting-down', 'terminated', 'stopping', 'stopped'),
    )
    instance_id = instance['InstanceId']
    market = get_instance_market(instance)
    try:
        instance_ip = instance['PrivateIpAddress']
    except KeyError:
        instance_ip = 'unknown'
    print(f'\t - {instance_id} {market} ({instance_ip}): {instance_status_str} {postfix}')


def _write_summary(manager):
    print('Mesos statistics:')
    total_cpus = manager.get_resource_total('cpus')
    total_mem = humanize.naturalsize(manager.get_resource_total('mem') * 1000000)
    total_disk = humanize.naturalsize(manager.get_resource_total('disk') * 1000000)
    allocated_cpus = manager.get_resource_allocation('cpus')
    allocated_mem = humanize.naturalsize(manager.get_resource_allocation('mem') * 1000000)
    allocated_disk = humanize.naturalsize(manager.get_resource_allocation('disk') * 1000000)
    print(f'\tCPU allocation: {allocated_cpus} CPUs allocated to tasks, {total_cpus} total')
    print(f'\tMemory allocation: {allocated_mem} memory allocated to tasks, {total_mem} total')
    print(f'\tDisk allocation: {allocated_disk} disk space allocated to tasks, {total_disk} total')


def _get_mesos_status_string(instance, agents):
    try:
        instance_ip = instance['PrivateIpAddress']
        launch_time = instance['LaunchTime']
    except KeyError:
        mesos_state = MesosAgentState.UNKNOWN
        postfix_str = ''
    else:
        uptime = humanize.naturaldelta(arrow.now() - arrow.get(launch_time))
        if instance_ip not in agents:
            mesos_state = MesosAgentState.ORPHANED
            postfix_str = f', up for {uptime}'
        elif allocated_cpu_resources(agents[instance_ip]) == 0:
            mesos_state = MesosAgentState.IDLE
            postfix_str = f', up for {uptime}'
        else:
            mesos_state = MesosAgentState.RUNNING
            postfix_str = str(allocated_cpu_resources(agents[instance_ip])) + ' CPUs allocated'

    return mesos_state, colored_status(
        mesos_state,
        blue=(MesosAgentState.IDLE,),
        red=(MesosAgentState.ORPHANED, MesosAgentState.UNKNOWN),
        prefix='[',
        postfix=postfix_str + ']',
    )


def print_status(manager, args):
    sys.stdout.write('\n')
    print(f'Current status for the {manager.role} role in the {manager.cluster} cluster:\n')
    print(f'Resource groups ({manager.fulfilled_capacity} units out of {manager.target_capacity}):')
    if args.verbose:
        agents = {
            socket.gethostbyname(agent['hostname']): agent
            for agent in manager.agents
        }

    for group in manager.resource_groups:
        _write_resource_group_line(group)
        if args.verbose:
            for instance in ec2_describe_instances(instance_ids=group.instance_ids):
                mesos_state, postfix = _get_mesos_status_string(instance, agents)
                if ((args.only_orphans and mesos_state != MesosAgentState.ORPHANED) or
                        (args.only_idle and mesos_state != MesosAgentState.IDLE)):
                    continue
                _write_instance_line(instance, postfix)
        sys.stdout.write('\n')

    _write_summary(manager)
    sys.stdout.write('\n')


def main(args):  # pragma: no cover
    manager = MesosRoleManager(args.cluster, args.role)
    print_status(manager, args)


@subparser('status', 'check the status of a Mesos cluster', main)
def add_mesos_status_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_role_arg(required_named_args, required=True)

    optional_named_args.add_argument(
        '--only-idle',
        action='store_true',
        help='Only show information about idle agents'
    )
    optional_named_args.add_argument(
        '--only-orphans',
        action='store_true',
        help='Only show information about orphaned instances (instances that are not in the Mesos cluster)'
    )
    optional_named_args.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show more detailed status information',
    )
