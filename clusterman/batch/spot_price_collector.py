import time

import arrow
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import METADATA
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch import batch_context
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_env_config_path_arg
from clusterman.args import add_region_arg
from clusterman.aws.spot_prices import spot_price_generator
from clusterman.aws.spot_prices import write_prices_with_dedupe
from clusterman.util import setup_config


class SpotPriceCollector(BatchDaemon):
    notify_emails = ['distsys-processing@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('SpotPriceCollector options')
        add_region_arg(arg_group)
        add_env_config_path_arg(arg_group)
        arg_group.add_argument(
            '--start-time',
            default=arrow.utcnow(),
            help=(
                'Start of period to collect prices for. Default is now. '
                'Suggested format: 2017-01-13T21:10:34-08:00 (if no timezone, will be parsed as UTC).'
            ),
            type=lambda d: arrow.get(d).to('utc'),
        )

    @batch_configure
    def configure_initial(self):
        # Any keys in the env_config will override defaults in config.yaml.
        setup_config(self.options)

        self.region = self.options.aws_region
        self.last_time_called = self.options.start_time
        self.run_interval = staticconf.read_int('spot_prices.run_interval_seconds')
        self.dedupe_interval = staticconf.read_int('spot_prices.dedupe_interval_seconds')

    @batch_context
    def get_writer(self):
        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)
        with self.metrics_client.get_writer(METADATA) as writer:
            self.writer = writer
            yield

    def write_prices(self, end_time):
        prices = spot_price_generator(self.last_time_called, end_time)
        write_prices_with_dedupe(prices, self.writer, self.dedupe_interval)
        self.last_time_called = end_time

    def run(self):
        while self.running:
            time.sleep(self.run_interval - time.time() % self.run_interval)
            now = arrow.utcnow()
            self.write_prices(now)


if __name__ == '__main__':
    SpotPriceCollector().start()
