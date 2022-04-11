# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time

import colorlog
from botocore.exceptions import EndpointConnectionError
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_env_config_path_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.batch.util import suppress_request_limit_exceeded
from clusterman.config import setup_config
from clusterman.exceptions import AutoscalerError
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import PoolConnectionError
from clusterman.util import get_autoscaler_scribe_stream
from clusterman.util import sensu_checkin
from clusterman.util import setup_logging
from clusterman.util import splay_event_time
from clusterman.util import Status

logger = colorlog.getLogger(__name__)
colorlog.getLogger("clusterman_metrics")  # This just adds a handler to the clusterman_metrics logger
SERVICE_CHECK_NAME = "check_clusterman_autoscaler_service"
DEFAULT_TTL = "25m"
DEFAULT_CHECK_EVERY = "10m"


def sensu_alert_triage(fail=False):
    def decorator(fn):
        def wrapper(self):
            msg = ""
            service_failed = False
            error = None
            try:
                fn(self)
            except ClustermanSignalError as e:
                msg = str(e)
                logger.exception(f"Autoscaler signal failed: {msg}")
                error = e
            except Exception as e:
                msg = str(e)
                logger.exception(f"Autoscaler service failed: {msg}")
                error = e
                service_failed = True
            self._do_sensu_checkins(service_failed, msg)
            if fail and error:
                raise AutoscalerError from error

        return wrapper

    return decorator


class AutoscalerBatch(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ["compute-infra@yelp.com"]

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group("AutoscalerBatch options")
        add_cluster_arg(arg_group, required=True)
        add_pool_arg(arg_group)
        add_scheduler_arg(arg_group)
        add_cluster_config_directory_arg(arg_group)
        add_env_config_path_arg(arg_group)
        arg_group.add_argument(
            "--dry-run",
            default=False,
            action="store_true",
            help="If true, will only log autoscaling decisions instead of modifying capacities",
        )

    @batch_configure
    def configure_initial(self):
        setup_config(self.options)
        self.autoscaler = None
        self.logger = logger

        self.apps = [self.options.pool]  # TODO (CLUSTERMAN-126) someday these should not be the same thing
        pool_manager = PoolManager(
            self.options.cluster,
            self.options.pool,
            self.options.scheduler,
        )
        self.autoscaler = Autoscaler(
            self.options.cluster,
            self.options.pool,
            self.options.scheduler,
            self.apps,
            monitoring_enabled=(not self.options.dry_run),
            pool_manager=pool_manager,
        )

        # We don't want to watch anything here because the autoscaler bootstrap script takes care of that for us
        self.clear_watchers()

    def _get_local_log_stream(self, clog_prefix=None):
        # Overrides the yelp_batch default, which is tmp_batch_<filename> (autoscaler in this case)

        # This controls the name of the scribe log for this batch. Without this, the log
        # conflicts with other batches (like the Kew autoscaler).  We create a separate log for each
        # cluster and (non-default) pool, for easy distinguishmentability
        return get_autoscaler_scribe_stream(self.options.cluster, self.options.pool, self.options.scheduler)

    @sensu_alert_triage()
    def _autoscale(self):
        time.sleep(
            splay_event_time(
                self.autoscaler.run_frequency,
                self.get_name() + self.options.cluster + self.options.pool,
            )
        )
        with suppress_request_limit_exceeded():
            self.autoscaler.run(dry_run=self.options.dry_run)

    def run(self):
        # self.running is a property from yelp_batch which checks version_checker if a watcher config has changed.
        # If so, the entire batch restarts and configs for the service are reloaded.
        while self.running:
            try:
                self._autoscale()
            except (PoolConnectionError, EndpointConnectionError) as e:
                logger.exception(f"Encountered a connection error: {e}")

    def _do_sensu_checkins(self, service_failed, msg):
        check_every = (
            "{minutes}m".format(minutes=int(self.autoscaler.run_frequency // 60))
            if self.autoscaler
            else DEFAULT_CHECK_EVERY
        )
        # magic-y numbers here; an alert will time out after two autoscaler run periods plus a five minute buffer
        alert_delay = (
            "{minutes}m".format(minutes=int(self.autoscaler.run_frequency // 60) * 2 + 5)
            if self.autoscaler
            else DEFAULT_TTL
        )

        sensu_args = dict(
            check_name=SERVICE_CHECK_NAME,
            scheduler=self.options.scheduler,
            app=self.apps[0],  # TODO (CLUSTERMAN-126)
            check_every=check_every,
            source=f"{self.options.cluster}_{self.options.pool}",
            ttl=alert_delay,
            alert_after=alert_delay,
            noop=self.options.dry_run,
            pool=self.options.pool,
        )

        if service_failed:
            sensu_args["output"] = f"FAILED: clusterman autoscaler failed ({msg})"
            sensu_args["status"] = Status.CRITICAL
        else:
            sensu_args["output"] = "OK: clusterman autoscaler is fine"
        sensu_checkin(**sensu_args)


if __name__ == "__main__":
    setup_logging()
    AutoscalerBatch().start()
