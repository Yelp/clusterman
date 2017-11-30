import os
from functools import partial

import staticconf
import yaml
from yelp_servlib.config_util import load_default_config

from clusterman.mesos.constants import DEFAULT_ROLE_DIRECTORY
from clusterman.mesos.constants import ROLE_CONFIG_FILENAME
from clusterman.mesos.constants import ROLE_NAMESPACE


def setup_config(args, include_roles=True):
    load_default_config(args.env_config_path)

    # If a cluster is specified, any AWS calls should go to the corresponding region.
    # We can also load the role configs in that cluster, if include_roles is True.
    if getattr(args, 'cluster', None):
        cluster_region = staticconf.read_string('mesos_clusters.{cluster}.aws_region'.format(cluster=args.cluster))
        staticconf.DictConfiguration({'aws': {'region': cluster_region}})

        if include_roles:
            role_directory = staticconf.read_string(
                'role_config_directory',
                default=DEFAULT_ROLE_DIRECTORY,
            )
            load_role_configs_for_cluster(role_directory, args.cluster)

    # Required to indicate that configs have been loaded, to the watcher.
    return True


def load_role_configs_for_cluster(role_config_dir, cluster):
    all_roles = os.listdir(role_config_dir)
    cluster_roles = []
    for role in all_roles:
        role_file = get_role_config_path(role_config_dir, role)
        with open(role_file) as f:
            config = yaml.load(f)
            if cluster in config['mesos']:
                cluster_roles.append(role)
                role_namespace = ROLE_NAMESPACE.format(role=role)
                # Only include Mesos configuration for this cluster.
                staticconf.DictConfiguration({'mesos': config['mesos'][cluster]}, namespace=role_namespace)
                del config['mesos']
                # Load all other config values.
                staticconf.DictConfiguration(config, namespace=role_namespace)

    staticconf.DictConfiguration({'cluster_roles': cluster_roles})

    # Required to indicate that configs have been loaded, to the watcher.
    return True


def get_role_config_path(role_config_dir, role):
    return os.path.join(role_config_dir, role, ROLE_CONFIG_FILENAME)


def get_service_watcher(args, using_roles=True):
    config_loader = partial(setup_config, args, using_roles)
    return staticconf.config.ConfigurationWatcher(config_loader, args.env_config_path)


def get_roles_watcher(cluster):
    role_directory = staticconf.read_string('role_config_directory', default=DEFAULT_ROLE_DIRECTORY)
    role_files = [get_role_config_path(role_directory, role) for role in staticconf.read_list('cluster_roles')]
    config_loader = partial(load_role_configs_for_cluster, role_directory, cluster)
    return staticconf.config.ConfigurationWatcher(config_loader, role_files)
