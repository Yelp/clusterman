from getpass import getuser
from socket import gethostname

import arrow
import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import add_pool_arg
from clusterman.args import subparser
from clusterman.autoscaler.config import LOG_STREAM_NAME
from clusterman.config import POOL_NAMESPACE
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.util import ask_for_confirmation
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


def get_target_capacity_value(target_capacity, pool):
    target_capacity = target_capacity.lower()
    pool_namespace = POOL_NAMESPACE.format(pool=pool)
    if target_capacity == 'min':
        return staticconf.read_int('scaling_limits.min_capacity', namespace=pool_namespace)
    elif target_capacity == 'max':
        return staticconf.read_int('scaling_limits.max_capacity', namespace=pool_namespace)
    else:
        return int(target_capacity)


def main(args):
    if args.recycle:
        raise NotImplementedError('Cluster recycling is not yet supported')
    manager = MesosPoolManager(args.cluster, args.pool)
    old_target = manager.target_capacity
    requested_target = get_target_capacity_value(args.target_capacity, args.pool)
    if not args.dry_run:
        if not ask_for_confirmation(
            f'Modifying target capacity from {manager.target_capacity} to {requested_target}.  Proceed? '
        ):
            print('Aborting operation.')
            return

    new_target = manager.modify_target_capacity(requested_target, args.dry_run)
    log_message = (f'Target capacity for {args.pool} on {args.cluster} manually changed '
                   f'from {old_target} to {new_target} by {getuser()}')
    print(log_message)
    if not args.dry_run:
        try:
            import clog
            # manual modifications show up in the scribe history
            clog.log_line(LOG_STREAM_NAME, f'{arrow.now()} {gethostname()} {__name__} {log_message}')
        except ModuleNotFoundError:
            logger.warn('clog not found, are you running on a Yelp host?')


@subparser('manage', 'check the status of a Mesos cluster', main)
def add_mesos_manager_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args, required=True)
    required_named_args.add_argument(
        '--target-capacity',
        metavar='X',
        required=True,
        help='New target capacity for the cluster (valid options: min, max, positive integer)',
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
