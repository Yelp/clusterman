import os
from argparse import ArgumentError

import staticconf
import yaml
from yelp_servlib.config_util import load_default_config

CREDENTIALS_NAMESPACE = 'boto_cfg'
DEFAULT_CLUSTER_DIRECTORY = '/nail/srv/configs/clusterman-clusters'
ROLE_NAMESPACE = '{role}_config'


def setup_config(args, include_roles=True):
    # load_default_config merges the 'module_config' key from the first file
    # and the 'module_env_config' key from the second file to configure packages.
    # This allows us to configure packages differently in different hiera envs by
    # changing 'module_env_config'. We use the same file for both keys.
    load_default_config(args.env_config_path, args.env_config_path)

    signals_branch_or_tag = getattr(args, 'signals_branch_or_tag', None)
    cluster_config_directory = getattr(args, 'cluster_config_directory', None) or DEFAULT_CLUSTER_DIRECTORY
    staticconf.DictConfiguration({'cluster_config_directory': cluster_config_directory})

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
            load_cluster_role_configs(args.cluster, signals_branch_or_tag)

    staticconf.DictConfiguration({'aws': {'region': aws_region}})

    boto_creds_file = staticconf.read_string('aws.access_key_file')
    staticconf.JSONConfiguration(boto_creds_file, namespace=CREDENTIALS_NAMESPACE)

    if signals_branch_or_tag:
        staticconf.DictConfiguration({'autoscale_signal': {'branch_or_tag': signals_branch_or_tag}})


def load_cluster_role_configs(cluster, signals_branch_or_tag):
    cluster_config_directory = get_cluster_config_directory(cluster)
    role_config_files = [f for f in os.listdir(cluster_config_directory) if f[0] != '.']  # skip dotfiles
    cluster_roles = []

    for role_file in role_config_files:
        role = os.path.splitext(role_file)[0]
        cluster_roles.append(role)
        with open(os.path.join(cluster_config_directory, role_file)) as f:
            config = yaml.load(f)
            role_namespace = ROLE_NAMESPACE.format(role=role)
            staticconf.DictConfiguration(config, namespace=role_namespace)

            if signals_branch_or_tag:
                staticconf.DictConfiguration(
                    {'autoscale_signal': {'branch_or_tag': signals_branch_or_tag}},
                    namespace=role_namespace,
                )

    staticconf.DictConfiguration({'cluster_roles': cluster_roles})


def get_cluster_config_directory(cluster):
    return os.path.join(staticconf.read_string('cluster_config_directory'), cluster)


def get_role_config_path(cluster, role):
    return os.path.join(get_cluster_config_directory(cluster), f'{role}.yaml')
