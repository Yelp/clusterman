import argparse
import datetime
import io
from contextlib import contextmanager

import arrow
import mock
import pytest
import simplejson as json
import staticconf.testing
from clusterman_metrics import METADATA

from clusterman.batch.spot_price_collector import SpotPriceCollector


@pytest.fixture
def batch():
    return SpotPriceCollector()


@pytest.fixture
def mock_config():
    mock_config = {
        'aws': {
            'access_key_file': '/etc/secrets',
        },
        'dynamodb': {
            'region_name': 'us-test-2',
        },
        'spot_prices': {
            'run_interval_seconds': 120,
            'dedupe_interval_seconds': 60,
        }
    }
    with staticconf.testing.MockConfiguration(mock_config):
        yield


@pytest.fixture
def mock_boto_cfg():
    cfg = {
        'accessKeyId': 'id',
        'secretAccessKey': 'key',
    }
    # file-like object
    return io.StringIO(json.dumps(cfg))


@pytest.fixture
def default_options(batch):
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    return parser.parse_args([])


def test_start_time_parsing(batch):
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    args = parser.parse_args(['--start-time', '2017-09-12T12:11:23'])
    assert args.start_time == datetime.datetime(2017, 9, 12, 12, 11, 23, tzinfo=datetime.timezone.utc)


@mock.patch('arrow.utcnow')
def test_start_time_default(mock_now, batch):
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    args = parser.parse_args([])
    assert args.start_time == mock_now.return_value


@mock.patch('clusterman.batch.spot_price_collector.config_util', autospec=True)
def test_configure_initial_default(mock_config_util, batch, mock_config, default_options):
    batch.options = default_options
    batch.configure_initial()

    assert mock_config_util.load_default_config.call_args_list == [
        mock.call('config.yaml', batch.options.env_config_path),
    ]
    assert batch.region == 'us-test-2'
    assert batch.last_time_called == batch.options.start_time
    assert batch.run_interval == 120
    assert batch.dedupe_interval == 60


@mock.patch('clusterman.batch.spot_price_collector.config_util', autospec=True)
def test_configure_initial_with_options(mock_config_util, batch, mock_config, default_options):
    batch.options = default_options  # just to set up options object, will override
    batch.options.env_config_path = 'custom.yaml'
    batch.options.start_time = arrow.get(2017, 9, 1, 1, 1, 0)
    batch.options.aws_region_name = 'us-other-1'
    batch.configure_initial()

    assert mock_config_util.load_default_config.call_args_list == [
        mock.call('config.yaml', 'custom.yaml'),
    ]
    assert batch.region == 'us-other-1'
    assert batch.last_time_called == batch.options.start_time
    assert batch.run_interval == 120
    assert batch.dedupe_interval == 60


@mock.patch('clusterman.batch.spot_price_collector.boto3')
@mock.patch('clusterman.batch.spot_price_collector.open')
def test_set_up_clients(mock_open, mock_boto, batch, mock_config, mock_boto_cfg):
    mock_open.return_value = mock_boto_cfg
    batch.region = 'us-test-1'
    batch.set_up_clients()

    assert mock_open.call_args_list == [mock.call('/etc/secrets')]
    assert mock_boto.client.call_args_list == [
        mock.call('ec2', aws_access_key_id='id', aws_secret_access_key='key', region_name='us-test-1'),
    ]
    assert batch.ec2_client == mock_boto.client.return_value


@mock.patch('clusterman.batch.spot_price_collector.ClustermanMetricsBotoClient', autospec=True)
def test_get_writer(mock_client_class, batch):
    batch.region = 'us-test-2'
    # yelp_batch will create the context manager because of @batch_context when running
    # but do it ourselves for the unit test
    context = contextmanager(batch.get_writer)()
    with context:
        assert mock_client_class.call_args_list == [mock.call(region_name='us-test-2')]
        mock_client = mock_client_class.return_value
        assert mock_client.get_writer.call_args_list == [mock.call(METADATA)]
        writer_context = mock_client.get_writer.return_value
        assert batch.writer == writer_context.__enter__.return_value
    assert writer_context.__exit__.call_count == 1


@mock.patch('clusterman.batch.spot_price_collector.spot_price_generator', autospec=True)
@mock.patch('clusterman.batch.spot_price_collector.write_prices_with_dedupe', autospec=True)
def test_write_prices(mock_write, mock_price_gen, batch):
    batch.ec2_client = mock.Mock()
    batch.writer = mock.Mock()
    batch.dedupe_interval = 60

    start = arrow.get(2017, 4, 10, 0, 3, 1)
    batch.last_time_called = start
    now = arrow.get(2017, 4, 10, 1, 0, 0)
    batch.write_prices(now)

    assert mock_price_gen.call_args_list == [mock.call(batch.ec2_client, start, now)]
    assert mock_write.call_args_list == [mock.call(mock_price_gen.return_value, batch.writer, 60)]
    assert batch.last_time_called == now  # updated after the write_prices call


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('arrow.utcnow')
@mock.patch('clusterman.batch.spot_price_collector.SpotPriceCollector.running', new_callable=mock.PropertyMock)
def test_run(mock_running, mock_now, mock_time, mock_sleep, batch):
    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 113, 148]

    batch.run_interval = 10
    with mock.patch.object(batch, 'write_prices', autospec=True) as write_prices:
        batch.run()
        assert write_prices.call_args_list == [mock.call(mock_now.return_value) for _ in range(3)]
    assert mock_sleep.call_args_list == [mock.call(9), mock.call(7), mock.call(2)]
