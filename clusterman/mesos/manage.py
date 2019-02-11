from getpass import getuser
from socket import gethostname

import arrow
import colorlog
import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import subparser
from clusterman.config import POOL_NAMESPACE
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.util import ask_for_confirmation
from clusterman.util import get_autoscaler_scribe_stream
from clusterman.util import log_to_scribe

LOG_TEMPLATE = f'{arrow.now()} {gethostname()} {__name__}'
logger = colorlog.getLogger(__name__)


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
        scribe_stream = get_autoscaler_scribe_stream(args.cluster, args.pool)
        log_to_scribe(scribe_stream, f'{LOG_TEMPLATE} {log_message}')


@subparser('manage', 'check the status of a Mesos cluster', main)
def add_mesos_manager_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
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
    add_cluster_config_directory_arg(optional_named_args)
