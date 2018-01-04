import pysensu_yelp


def log_run_info(logger):
    def decorator(func):
        def wrapper(self):
            logger.info('Starting batch {name}; watching {watched_files} for changes'.format(
                name=type(self).__name__,
                watched_files=[watcher.filenames for watcher in self.version_checker.watchers],
            ))
            func(self)
        return wrapper
    return decorator


def sensu_checkin(check_name, output, check_every, ttl, source, page=True, alert_after='0m', noop=False):
    if noop:
        return

    pysensu_yelp.send_event(
        name=check_name,
        runbook='http://y/rb-clusterman',
        status=pysensu_yelp.Status.OK,
        output=output,
        team='distsys_compute',
        page=page,
        check_every=check_every,
        ttl=ttl,
        alert_after=alert_after,
        source=source,
    )
