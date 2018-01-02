from clusterman.args import add_cluster_arg
from clusterman.args import add_role_arg
from clusterman.args import subparser
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import ask_for_confirmation
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


def main(args):
    if args.recycle:
        raise NotImplementedError('Cluster recycling is not yet supported')
    manager = MesosRoleManager(args.cluster, args.role)
    if not args.dry_run:
        ask_for_confirmation(
            f'Modifying target capacity from {manager.target_capacity} to {args.target_capacity}.  Proceed? '
        )
    new_target_capacity = manager.modify_target_capacity(args.target_capacity, args.dry_run)
    print(f'Operation complete.  New target capacity set to {new_target_capacity}')


@subparser('manage', 'check the status of a Mesos cluster', main)
def add_mesos_manager_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_role_arg(required_named_args, required=True)
    required_named_args.add_argument(
        '--target-capacity',
        type=int,
        metavar='X',
        required=True,
        help='New target capacity for the cluster',
    )
    optional_named_args.add_argument(
        '--dry-run',
        action='store_true',
        help='Just print what would happen, don\'t actually add or remove instances'
    )
    optional_named_args.add_argument(
        '--recycle',
        action='store_true',
        help='Tear down the existing cluster and create a new one',
    )
