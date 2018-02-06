import os
from argparse import ArgumentError

import staticconf
import yaml
from yelp_servlib.config_util import load_default_config

from clusterman.aws.client import CREDENTIALS_NAMESPACE
from clusterman.mesos.constants import DEFAULT_ROLE_DIRECTORY
from clusterman.mesos.constants import ROLE_CONFIG_FILENAME
from clusterman.mesos.constants import ROLE_NAMESPACE


def setup_config(args, include_roles=True):
    # load_default_config merges the 'module_config' key from the first file
    # and the 'module_env_config' key from the second file to configure packages.
    # This allows us to configure packages differently in different hiera envs by
    # changing 'module_env_config'. We use the same file for both keys.
    load_default_config(args.env_config_path, args.env_config_path)

    signals_branch_or_tag = getattr(args, 'signals_branch_or_tag', None)

    aws_region = getattr(args, 'aws_region', None)
    cluster = getattr(args, 'cluster', None)
    if aws_region and cluster:
        raise ArgumentError(None, 'Cannot specify both cluster and aws_region')

    # If there is a cluster specified via --cluster, load cluster-specific attributes
    # into staticconf.  These values are not specified using hiera in srv-configs because
    # we might want to be operating on a cluster in one region while running from a
    # different region.
    elif cluster:
        aws_region = staticconf.read_string(f'mesos_clusters.{cluster}.aws_region')

        if include_roles:
            load_role_configs_for_cluster(args.cluster, signals_branch_or_tag)

    if aws_region:
        staticconf.DictConfiguration({'aws': {'region': aws_region}})

    boto_creds_file = staticconf.read_string('aws.access_key_file')
    staticconf.JSONConfiguration(boto_creds_file, namespace=CREDENTIALS_NAMESPACE)

    if signals_branch_or_tag:
        staticconf.DictConfiguration({'autoscale_signal': {'branch_or_tag': signals_branch_or_tag}})


def load_role_configs_for_cluster(cluster, signals_branch_or_tag):
    role_config_dir = get_role_config_dir()
    all_roles = os.listdir(role_config_dir)
    cluster_roles = []

    # Loop through all of the roles that we find in the role_config_dir;
    # each role specifies a list of clusters that it operates on, so this function
    # computes the reverse mapping and loads only the roles that are present on the
    # cluster and which clusterman knows how to manage
    for role in all_roles:
        role_file = get_role_config_path(role)
        with open(role_file) as f:
            config = yaml.load(f)
            if cluster in config['mesos']:
                cluster_roles.append(role)
                role_namespace = ROLE_NAMESPACE.format(role=role)

                # Only include Mesos configuration for this cluster; since a role could run
                # on several clusters with different configurations, we don't want to load the
                # configs for the other clusters
                staticconf.DictConfiguration({'mesos': config['mesos'][cluster]}, namespace=role_namespace)
                del config['mesos']

                # Load all other config values.
                staticconf.DictConfiguration(config, namespace=role_namespace)

                if signals_branch_or_tag:
                    staticconf.DictConfiguration(
                        {'autoscale_signal': {'branch_or_tag': signals_branch_or_tag}},
                        namespace=role_namespace,
                    )

    staticconf.DictConfiguration({'cluster_roles': cluster_roles})


def get_role_config_dir():
    return staticconf.read_string('role_config_directory', default=DEFAULT_ROLE_DIRECTORY)


def get_role_config_path(role):
    return os.path.join(get_role_config_dir(), role, ROLE_CONFIG_FILENAME)
