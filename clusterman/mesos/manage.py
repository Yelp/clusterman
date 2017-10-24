from clusterman.args import subparser
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import ask_for_confirmation


def main(args):
    if args.recycle:
        raise NotImplementedError('Cluster recycling is not yet supported')
    manager = MesosRoleManager(args.role, args.role_config_path)
    ask_for_confirmation(
        f'Modifying target capacity from {manager.target_capacity} to {args.target_capacity}.  Proceed? '
    )
    new_target_capacity = manager.modify_target_capacity(args.target_capacity)
    print(f'Operation complete.  New target capacity set to {new_target_capacity}')


@subparser('manage', 'check the status of a Mesos cluster', main)
def add_mesos_manager_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    required_named_args.add_argument(
        '--role',
        required=True,
        help='Mesos role to query the status for',
    )
    required_named_args.add_argument(
        '--target-capacity',
        type=int,
        required=True,
        help='New target capacity for the cluster',
    )
    optional_named_args.add_argument(
        '--recycle',
        help='Tear down the existing cluster and create a new one',
    )
    optional_named_args.add_argument(
        '--role-config-path',
        help='Location of role-specific configuration files',
    )
