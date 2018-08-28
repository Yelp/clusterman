from getpass import getuser
from socket import gethostname

import arrow
import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import add_pool_arg
from clusterman.args import subparser
from clusterman.aws.client import get_latest_ami
from clusterman.config import POOL_NAMESPACE
from clusterman.mesos.mesos_pool_manager import MesosPoolManager
from clusterman.spotinst.utils import update_ami
from clusterman.util import ask_for_confirmation
from clusterman.util import get_autoscaler_scribe_stream
from clusterman.util import get_clusterman_logger
from clusterman.util import log_to_scribe


LOG_TEMPLATE = f'{arrow.now()} {gethostname()} {__name__}'

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


def _update_ami_to_latest(args):
    update_ami(get_latest_ami(args.update_ami_to_latest), args.cluster, args.pool)


def _update_ami_to(args):
    update_ami(args.update_ami_to, args.cluster, args.pool)


def main(args):
    log_message = (f'')
    if args.update_ami_to_latest is not None:
        if args.dry_run is True:
            log_message = (f'Would have updated the AMI of {args.cluster} {args.pool} to the latest')
        else:
            _update_ami_to_latest(args)
            log_message = (f'Updated the AMI of the ElastiGroups in {args.cluster} and {args.pool}'
                           f' to latest.')

    elif args.update_ami_to is not None:
        if args.dry_run is True:
            log_message = (f'Would have updated the AMI of the ElasticGroups in {args.cluster} cluster'
                           f'and {args.pool} pool to the specified AMI id.')
        else:
            _update_ami_to(args)
            log_message = (f'Updated the AMI of the ElastiGroups in {args.cluster} and {args.pool}'
                           f' to {args.update_ami_to}.')

    elif args.target_capacity is not None:
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

        if args.dry_run is True:
            log_message = (f'Would have modified the target capacity for the {args.pool} on {args.cluster}'
                           f'to {args.target_capacity}')
        else:
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
    optional_named_args.add_argument(
        '--target-capacity',
        metavar='X',
        required=False,
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
    optional_named_args.add_argument(
        '--update-ami-to-latest',
        metavar='PAASTA_HVM',
        required=False,
        help='Update the AMI of the ElastiGroups in the specified cluster and pool to latest.'
    )
    optional_named_args.add_argument(
        '--update-ami-to',
        metavar='AMI-12345',
        required=False,
        help='Update the AMI of the ElastiGroups in the specified cluster and pool.'
    )
