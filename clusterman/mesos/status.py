from clusterman.args import add_cluster_arg
from clusterman.args import subparser
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import colored_status


def print_status(manager, verbose):
    print('\n')
    print(f'Current status for the {manager.name} role in the {manager.cluster} cluster:\n')
    print('Resource groups:')
    for group in manager.resource_groups:
        # TODO (CLUSTERMAN-100) These are just the status responses for spot fleets; this probably won't
        # extend to other types of resource groups, so we should figure out what to do about that.
        status_str = colored_status(
            group.status,
            active=('active',),
            changing=('modifying', 'submitted'),
            inactive=('cancelled', 'failed', 'cancelled_running', 'cancelled_terminating'),
        )
        print(f'\t{group.id}: {status_str} ({group.fulfilled_capacity} / {group.target_capacity})')
        if verbose:
            for instance in ec2_describe_instances(instance_ids=group.instances):
                instance_status_str = colored_status(
                    instance['State']['Name'],
                    active=('running',),
                    changing=('pending',),
                    inactive=('shutting-down', 'terminated', 'stopping', 'stopped'),
                )
                instance_id = instance['InstanceId']
                market = get_instance_market(instance)
                try:
                    instance_ip = instance['PrivateIpAddress']
                except KeyError:
                    instance_ip = None
                print(f'\t - {instance_id} {market} ({instance_ip}): {instance_status_str}')
    print('\n')
    print(f'Total capacity: {manager.fulfilled_capacity} units out of {manager.target_capacity}')
    print('\n')


def main(args):
    manager = MesosRoleManager(args.role, args.cluster)
    print_status(manager, args.verbose)


@subparser('status', 'check the status of a Mesos cluster', main)
def add_mesos_status_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    required_named_args.add_argument(
        '--role',
        required=True,
        help='Mesos role to query the status for',
    )
    add_cluster_arg(required_named_args, required=True)
    optional_named_args.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show more detailed status information',
    )
