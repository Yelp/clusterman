from clusterman.args import subparser
from clusterman.mesos.mesos_role_manager import MesosRoleManager


def main(args):
    manager = MesosRoleManager(args.role, args.role_config_path)
    manager.status(args.verbose)


@subparser('status', 'check the status of a Mesos cluster', main)
def add_mesos_status_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    required_named_args.add_argument(
        '--role',
        required=True,
        help='Mesos role to query the status for',
    )
    optional_named_args.add_argument(
        '--role-config-path',
        help='Location of role-specific configuration files',
    )
    optional_named_args.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show more detailed status information',
    )
