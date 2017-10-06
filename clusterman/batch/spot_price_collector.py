import json
import time

import arrow
import boto3
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import METADATA
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch import batch_context
from yelp_batch.batch_daemon import BatchDaemon
from yelp_servlib import config_util

from clusterman.aws.spot_prices import spot_price_generator
from clusterman.aws.spot_prices import write_prices_with_dedupe


class SpotPriceCollector(BatchDaemon):
    notify_emails = ['distsys-processing@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('SpotPriceCollector options')
        arg_group.add_argument(
            'aws_region_name',
            help='AWS region to collect prices for.',
        )
        arg_group.add_argument(
            '--env-config-path',
            default='/nail/srv/configs/clusterman.yaml',
            help='Path to custom app configuration. Default is %(default)s',
        )
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
        config_util.load_default_config('config.yaml', self.options.env_config_path)

        self.region = self.options.aws_region_name
        self.last_time_called = self.options.start_time
        self.run_interval = staticconf.read_int('spot_prices.run_interval_seconds')
        self.dedupe_interval = staticconf.read_int('spot_prices.dedupe_interval_seconds')

    @batch_configure
    def set_up_clients(self):
        with open(staticconf.read_string('aws.access_key_file')) as boto_cfg_file:
            boto_cfg = json.load(boto_cfg_file)
        self.ec2_client = boto3.client(
            'ec2',
            aws_access_key_id=boto_cfg['accessKeyId'],
            aws_secret_access_key=boto_cfg['secretAccessKey'],
            region_name=self.region,
        )

    @batch_context
    def get_writer(self):
        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)
        with self.metrics_client.get_writer(METADATA) as writer:
            self.writer = writer
            yield

    def write_prices(self, end_time):
        prices = spot_price_generator(self.ec2_client, self.last_time_called, end_time)
        write_prices_with_dedupe(prices, self.writer, self.dedupe_interval)
        self.last_time_called = end_time

    def run(self):
        while self.running:
            time.sleep(self.run_interval - time.time() % self.run_interval)
            now = arrow.utcnow()
            self.write_prices(now)


if __name__ == '__main__':
    SpotPriceCollector().start()
