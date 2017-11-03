import argparse
import datetime

import arrow
import mock
import pytest
from clusterman_metrics import METADATA

from clusterman.batch.spot_price_collector import SpotPriceCollector


@pytest.fixture
def batch():
    return SpotPriceCollector()


@pytest.fixture
def batch_arg_parser(batch, args=None):
    args = args or []
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    return parser.parse_args(args)


@pytest.fixture
def mock_client_class():
    with mock.patch('clusterman.batch.spot_price_collector.ClustermanMetricsBotoClient', autospec=True) as client:
        yield client


def test_start_time_parsing(batch):
    args = batch_arg_parser(batch, ['--start-time', '2017-09-12T12:11:23'])
    assert args.start_time == datetime.datetime(2017, 9, 12, 12, 11, 23, tzinfo=datetime.timezone.utc)


@mock.patch('arrow.utcnow')
def test_start_time_default(mock_now, batch):
    args = batch_arg_parser(batch)
    assert args.start_time == mock_now.return_value


def test_configure_initial_default(batch, mock_client_class):
    batch.options = batch_arg_parser(batch, ['--aws-region', 'us-test-2'])
    with mock.patch('clusterman.batch.spot_price_collector.setup_config'):
        batch.configure_initial()

    assert batch.region == 'us-test-2'
    assert batch.last_time_called == batch.options.start_time
    assert batch.run_interval == 120
    assert batch.dedupe_interval == 60
    assert mock_client_class.call_args_list == [mock.call(region_name='us-test-2')]
    assert batch.metrics_client == mock_client_class.return_value


def test_configure_initial_with_options(batch, batch_arg_parser, mock_client_class):
    batch.options = batch_arg_parser  # just to set up options object, will override
    batch.options.env_config_path = 'custom.yaml'
    batch.options.start_time = arrow.get(2017, 9, 1, 1, 1, 0)
    batch.options.aws_region = 'us-other-1'
    with mock.patch('clusterman.batch.spot_price_collector.setup_config'):
        batch.configure_initial()

    assert batch.region == 'us-other-1'
    assert batch.last_time_called == batch.options.start_time
    assert batch.run_interval == 120
    assert batch.dedupe_interval == 60
    assert mock_client_class.call_args_list == [mock.call(region_name='us-other-1')]
    assert batch.metrics_client == mock_client_class.return_value


@mock.patch('clusterman.batch.spot_price_collector.spot_price_generator', autospec=True)
@mock.patch('clusterman.batch.spot_price_collector.write_prices_with_dedupe', autospec=True)
def test_write_prices(mock_write, mock_price_gen, batch):
    batch.dedupe_interval = 60

    start = arrow.get(2017, 4, 10, 0, 3, 1)
    batch.last_time_called = start
    now = arrow.get(2017, 4, 10, 1, 0, 0)
    writer = mock.Mock()
    batch.write_prices(now, writer)

    assert mock_price_gen.call_args_list == [mock.call(start, now)]
    assert mock_write.call_args_list == [mock.call(mock_price_gen.return_value, writer, 60)]
    assert batch.last_time_called == now  # updated after the write_prices call


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('arrow.utcnow')
@mock.patch('clusterman.batch.spot_price_collector.SpotPriceCollector.running', new_callable=mock.PropertyMock)
def test_run(mock_running, mock_now, mock_time, mock_sleep, batch):
    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 113, 148]

    batch.run_interval = 10
    batch.metrics_client = mock.MagicMock()

    writer_context = batch.metrics_client.get_writer.return_value
    writer = writer_context.__enter__.return_value

    with mock.patch.object(batch, 'write_prices', autospec=True) as write_prices:
        batch.run()

        # Writing should have happened 3 times.
        # Each time, we create a new writer context and call write_metrics.
        assert batch.metrics_client.get_writer.call_args_list == [mock.call(METADATA) for i in range(3)]
        assert write_prices.call_args_list == [mock.call(mock_now.return_value, writer) for i in range(3)]
        assert writer_context.__exit__.call_count == 3

    assert mock_sleep.call_args_list == [mock.call(9), mock.call(7), mock.call(2)]
