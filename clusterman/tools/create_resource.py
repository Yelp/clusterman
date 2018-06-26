from getpass import getuser
from socket import gethostname
import yaml

from clusterman.args import subparser
from clusterman.autoscaler.config import LOG_STREAM_NAME
from clusterman.spotinst.utils import create_new_eg
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)


def main(args):
    # read the config and generate a dict
    config = None
    with open(args.config) as fd:
        config = yaml.load(fd)

    print(f'{config}')
    create_new_eg(args.name, config)


@subparser('create_resource', 'create new spotinst ElasticGroup', main)
def add_create_resource_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    required_named_args.add_argument(
        '--type',
        metavar='X',
        required=True,
        help='Type of the resource.',
    )
    required_named_args.add_argument(
        '--config',
        metavar='conf/config_file.yaml',
        required=True,
        help='Path of the config file.',
    )
    required_named_args.add_argument(
        '--name',
        metavar='Y',
        required=True,
        help='Name of the resource.',
    )
