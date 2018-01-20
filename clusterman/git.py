import os
import socket
import subprocess

from clusterman.exceptions import GitError
from clusterman.util import get_clusterman_logger

logger = get_clusterman_logger(__name__)
SIGNALS_REPO = 'git@git.yelpcorp.com:clusterman_signals'
SIGNAL_SOCK_NAME = 'clusterman-signal-socket'


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

    if not os.path.exists(local_path):
        subprocess.run(['git', 'clone', '--depth', '1', '--branch', branch_or_tag, SIGNALS_REPO, local_path])
        subprocess.run(['make', 'venv'], cwd=local_path)
        # TODO should check returncodes
    else:
        logger.debug(f'signal version {sha} exists in cache, not re-cloning')

    return local_path


def load_signal_connection(branch_or_tag, role, signal_name):
    signal_dir = _add_clusterman_signals_to_path(branch_or_tag)
    s = socket.socket(socket.AF_UNIX)
    s.bind(f'\0{SIGNAL_SOCK_NAME}')  # this creates an abstract namespace socket which is auto-cleaned on program exit
    s.listen(1)
    subprocess.Popen([
        os.path.join(signal_dir, 'venv', 'bin', 'python'),
        '-m',
        'clusterman_signals.run',
        role,
        signal_name,
        'clusterman-signal-socket',
    ])
    conn, __ = s.accept()
    return conn
