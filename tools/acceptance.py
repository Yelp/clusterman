import argparse
import os
import shlex
import shutil
import subprocess
from contextlib import contextmanager
from getpass import getuser

import yaml
from colorama import Fore
from colorama import Style

from clusterman.args import help_formatter

TEST_SUITES = ['unit', 'paasta', 'debian']
TMP_SRV_CONFIGS = os.path.join('/', 'tmp', f'srv-configs-{getuser()}')


def message(msg):
    print(Fore.RED + '\n**** ' + msg + '\n' + Style.RESET_ALL)


@contextmanager
def git_stash(repo_dir, conditional):
    """ Context manager to stash and restore changes to a git repository """
    # Do nothing if the conditional is not satisfied
    if not conditional:
        yield
        return

    # Check if there are any changes to the working directory that need to be stashed
    status = subprocess.run(['git', 'status', '--porcelain', '-uno'], cwd=repo_dir, check=True, stdout=subprocess.PIPE)

    if status.stdout:
        message(f'There are uncommitted changes in {repo_dir}; stashing until testing is done')
        subprocess.run(['git', 'stash', 'save'], cwd=repo_dir, check=True)

    # There's a race condition here if someone makes some other changes while we're in here, and then
    # stashes them -- once the context manager exits, it will pop the other stashed thing, not the
    # original set of changes.  Fixing this is annoying, so, just, don't do that.
    try:
        yield
    finally:
        if status.stdout:
            message(f'Restoring stash for {repo_dir}')
            subprocess.run(['git', 'stash', 'pop'], cwd=repo_dir, check=True)


@contextmanager
def hiera_merge(args, service, cluster, instance):
    """ Context manager to merge the local srv-configs directory into a temporary directory """
    if not args.srv_configs_repo:
        yield
        return

    try:
        srv_configs_args = args.srv_configs_args.split()
    except AttributeError:
        srv_configs_args = []
    message(f'Merging {args.srv_configs_repo} with the following arguments: {srv_configs_args}')
    subprocess.run(
        [
            '/opt/yelp_service_deployment/bin/hiera-merge',
            args.srv_configs_repo,
            TMP_SRV_CONFIGS,
        ] + srv_configs_args,
        check=True,
    )

    config_path = os.path.join(args.yelpsoa_configs_root, service, f'marathon-{cluster}.yaml')
    with open(config_path) as f:
        orig_contents = f.read()
        config = yaml.load(orig_contents)
        # Bind mounts don't play well with symlinks so we have to add our custom branch to the actual
        # srv-configs folder, which is .main-<timestamp>.  PaaSTA performs its volume mounts in a "specific
        # trumps general" fashion, so this is always guaranteed to work.
        main_name = next(path for path in os.listdir('/nail/etc/srv-configs') if path.startswith('.main'))
        config[instance].setdefault('extra_volumes', list()).append({
            'containerPath': os.path.join('/nail/etc/srv-configs', main_name),
            'hostPath': TMP_SRV_CONFIGS,
            'mode': 'RO',
        })
    with open(config_path, 'w') as f:
        f.write(yaml.dump(config))

    try:
        yield
    finally:
        message('Cleaning up merged srv-configs')
        with open(config_path, 'w') as f:
            f.write(orig_contents)
        shutil.rmtree(TMP_SRV_CONFIGS)


def parse_makefile_paasta_itests():
    """ Look for commands in the service Makefile 'itest' section matching 'paasta local-run' """
    itest_section = False
    local_run_cmds = []
    with open('Makefile') as f:
        for line in f:
            if line.startswith('itest: cook-image'):
                itest_section = True
            elif itest_section:
                # If we're in the itest section and we see a line that doesn't start with a tab, we know that
                # the section is over and we can stop
                if not line[0] == '\t':
                    break
                elif 'paasta local-run' in line:
                    local_run_cmds.append(shlex.split(line))

    return local_run_cmds


def run_acceptance_tests(args):
    suites = TEST_SUITES if args.test_suites == 'all' else args.test_suites

    if 'unit' in suites:
        message('Running unit tests')
        subprocess.run(['make', 'test'], check=True)

    if 'paasta' in suites:
        message('Running PaaSTA itests')
        yelpsoa_stash = git_stash(args.yelpsoa_configs_root, (not args.no_stash and args.yelpsoa_configs_root))
        srvconf_stash = git_stash(args.srv_configs_repo, (not args.no_stash and args.srv_configs_repo))

        with yelpsoa_stash, srvconf_stash:
            local_run_cmds = parse_makefile_paasta_itests()
            for cmd in local_run_cmds:
                if args.yelpsoa_configs_root:
                    cmd.extend(['-y', args.yelpsoa_configs_root])

                service_i, cluster_i, instance_i = cmd.index('-s') + 1, cmd.index('-c') + 1, cmd.index('-i') + 1
                with hiera_merge(args, cmd[service_i], cmd[cluster_i], cmd[instance_i]):
                    subprocess.run(cmd, check=True)

    if 'debian' in suites:
        message('Running debian itests')
        subprocess.run(['make', 'package'], check=True)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Run acceptance tests for clusterman', formatter_class=help_formatter)
    parser.add_argument(
        '--test-suites',
        choices=['all'] + TEST_SUITES,
        nargs='*',
        default='all',
        help='which test suites to run',
    )
    parser.add_argument(
        '-y', '--yelpsoa-configs-root',
        metavar='directory',
        default=None,
        help='location of the yelpsoa-configs to use',
    )
    parser.add_argument(
        '-s', '--srv-configs-repo',
        metavar='directory',
        default=None,
        help='location of the srv-configs to use',
    )
    parser.add_argument(
        '--srv-configs-args',
        type=str,
        help='arguments to pass in to srv-configs hiera-merge (use double-quotes; example "--test-ecosystem testopia")',
    )
    parser.add_argument(
        '--no-stash',
        action='store_true',
        help='do not stash working tree changes',
    )

    args = parser.parse_args()
    if args.srv_configs_repo and not args.yelpsoa_configs_root:
        raise argparse.ArgumentError(None, 'Using a custom srv-configs repo requires --yelpsoa-configs-root')
    return args


if __name__ == '__main__':
    args = parse_arguments()
    with git_stash(os.getcwd(), conditional=not args.no_stash):
        try:
            run_acceptance_tests(args)
        except KeyboardInterrupt:
            pass  # no stack trace on ctrl-c
