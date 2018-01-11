import os
import subprocess
import sys
from importlib import import_module

from clusterman.exceptions import GitError
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'


def _get_cache_location():
    return os.path.join(os.path.expanduser("~"), '.cache', 'clusterman')


def _sha_from_branch_or_tag(branch_or_tag):
    result = subprocess.run(['git', 'ls-remote', '--exit-code', SIGNALS_REPO, branch_or_tag], stdout=subprocess.PIPE)
    if result.returncode == 0:
        output = result.stdout.decode()
        sha = output.split('\t')[0]
        return sha
    else:
        raise GitError(f'No such branch_or_tag ({branch_or_tag}) found!')


def _add_clusterman_signals_to_path(branch_or_tag):
    local_repo_cache = _get_cache_location()
    sha = _sha_from_branch_or_tag(branch_or_tag)
    local_path = os.path.join(local_repo_cache, f'clusterman_signals_{sha}')
    if local_path in sys.path:
        return

    if not os.path.exists(local_path):
        subprocess.run(['git', 'clone', '--depth', '1', '--branch', branch_or_tag, SIGNALS_REPO, local_path])
    else:
        logger.debug(f'signal version {sha} exists in cache, not re-cloning')

    # TODO (CLUSTERMAN-126) if there are multiple roles on a cluster that each specify different versions of
    # the signal, this will break; we should have some mechanism to load a different signal version for each
    # role.
    sys.path.append(local_path)


def load_signal_class(branch_or_tag, role, signal_name):
    _add_clusterman_signals_to_path(branch_or_tag)
    signal_module = import_module(f'clusterman_signals.{role}')
    return getattr(signal_module, signal_name)


def load_signal_metric_config(branch_or_tag):
    _add_clusterman_signals_to_path(branch_or_tag)
    base_signal_module = import_module(f'clusterman_signals.base_signal')
    return base_signal_module.MetricConfig
