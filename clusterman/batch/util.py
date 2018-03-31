import inspect
import os

import pysensu_yelp
from pysensu_yelp import Status
from yelp_batch.batch import batch_context


class BatchLoggingMixin:
    @batch_context
    def setup_watchers(self):
        self.logger.info('Starting batch {name}; watching {watched_files} for changes'.format(
            name=type(self).__name__,
            watched_files=[watcher.filenames for watcher in self.version_checker.watchers],
        ))
        yield
        self.logger.info('Batch {name} complete'.format(name=type(self).__name__))


class BatchRunningSentinelMixin:
    @batch_context
    def make_running_sentinel(self):
        batch_name, ext = os.path.splitext(os.path.basename(inspect.getfile(self.__class__)))
        sentinel_file = f'/tmp/{batch_name}.running'
        with open(sentinel_file, 'w') as f:
            f.write(str(os.getpid()))
        yield


def sensu_checkin(check_name, output, check_every, ttl, source, owner='distsys-compute', status=Status.OK,
                  runbook='http://y/rb-clusterman', page=True, alert_after='0m', noop=False):
    if noop:
        return

    pysensu_yelp.send_event(
        name=check_name,
        runbook=runbook,
        status=status,
        output=output,
        team=owner,
        page=page,
        check_every=check_every,
        ttl=ttl,
        alert_after=alert_after,
        source=source,
    )
