from clusterman.args import add_cluster_arg
from clusterman.args import add_role_arg
from clusterman.args import subparser
from clusterman.batch.autoscaler import LOG_STREAM_NAME
from clusterman.mesos.mesos_role_manager import MesosRoleManager
from clusterman.util import ask_for_confirmation
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


def main(args):
    if args.recycle:
        raise NotImplementedError('Cluster recycling is not yet supported')
    manager = MesosRoleManager(args.cluster, args.role)
    old_target = manager.target_capacity
    if not args.dry_run:
        if not ask_for_confirmation(
            f'Modifying target capacity from {manager.target_capacity} to {args.target_capacity}.  Proceed? '
        ):
            print('Aborting operation.')
            return

    new_target = manager.modify_target_capacity(args.target_capacity, args.dry_run)
    log_message = f'Target capacity for {args.role} manually changed from {old_target} to {new_target}'
    print(log_message)
    try:
        import clog
        clog.log_line(LOG_STREAM_NAME, log_message)  # manual modifications show up in the scribe history
    except ModuleNotFoundError:
        logger.warn('clog not found, are you running on a Yelp host?')


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
