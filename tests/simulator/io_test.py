import arrow
import jsonpickle
import mock
import pytest
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS

from clusterman.simulator.io import write_object_to_compressed_json


@pytest.fixture
def mock_ts_1():
    return {SYSTEM_METRICS: {'metric_1': [(arrow.get(1), 1.0), (arrow.get(2), 2.0), (arrow.get(3), 3.0)]}}


@pytest.fixture
def mock_ts_2():
    return {APP_METRICS: {'metric_2': [(arrow.get(1), 4.0), (arrow.get(2), 5.0), (arrow.get(4), 6.0)]}}


@pytest.yield_fixture
def mock_open():
    with mock.patch('clusterman.simulator.io.gzip') as mgz:
        mock_open_obj = mock.Mock()
        mgz.open.return_value.__enter__ = mock.Mock(return_value=mock_open_obj)
        yield mock_open_obj


def test_write_new_object(mock_ts_1, mock_open):
    write_object_to_compressed_json(mock_ts_1, 'foo')
    assert jsonpickle.decode(mock_open.write.call_args[0][0]) == mock_ts_1
