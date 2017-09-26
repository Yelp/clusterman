import json

import arrow
import mock
import pytest
from clusterman_metrics.util.constants import APP_METRICS
from clusterman_metrics.util.constants import SYSTEM_METRICS

from clusterman.simulator.metrics import TimeSeriesDecoder
from clusterman.simulator.metrics import write_metrics_to_compressed_json


@pytest.fixture
def mock_ts_1():
    return {SYSTEM_METRICS: {'metric_1': [(arrow.get(1), 1.0), (arrow.get(2), 2.0), (arrow.get(3), 3.0)]}}


@pytest.fixture
def mock_ts_2():
    return {APP_METRICS: {'metric_2': [(arrow.get(1), 4.0), (arrow.get(2), 5.0), (arrow.get(4), 6.0)]}}


@pytest.yield_fixture
def mock_open():
    with mock.patch('clusterman.simulator.metrics.gzip') as mgz:
        mock_open_obj = mock.Mock()
        mgz.open.return_value.__enter__ = mock.Mock(return_value=mock_open_obj)
        yield mock_open_obj


@mock.patch('clusterman.simulator.metrics.ask_for_confirmation')
@mock.patch('clusterman.simulator.metrics.read_metrics_from_compressed_json')
class TestMetricsWriter:
    def test_write_new_metrics(self, mock_reader, mock_confirm, mock_ts_1, mock_open):
        mock_reader.side_effect = OSError('No such file or directory')
        write_metrics_to_compressed_json(mock_ts_1, 'foo')
        assert mock_confirm.call_count == 0
        assert json.loads(mock_open.write.call_args[0][0], object_hook=TimeSeriesDecoder()) == mock_ts_1

    def test_write_append_to_existing_metrics(self, mock_reader, mock_confirm, mock_ts_1, mock_ts_2, mock_open):
        mock_reader.return_value = mock_ts_1
        write_metrics_to_compressed_json(mock_ts_2, 'foo')
        assert mock_confirm.call_count == 0
        assert json.loads(mock_open.write.call_args[0][0], object_hook=TimeSeriesDecoder()) == \
            {**mock_ts_1, **mock_ts_2}

    def test_overwrite_existing_metrics(self, mock_reader, mock_confirm, mock_ts_1, mock_ts_2, mock_open):
        mock_reader.return_value = {**mock_ts_1, **mock_ts_2}

        to_write = {SYSTEM_METRICS: {'metric_1': [(arrow.get(42), 9), (arrow.get(4245), 77)]}}
        write_metrics_to_compressed_json(to_write, 'foo')

        assert mock_confirm.call_count == 1
        assert json.loads(mock_open.write.call_args[0][0], object_hook=TimeSeriesDecoder()) == \
            {**to_write, **mock_ts_2}
