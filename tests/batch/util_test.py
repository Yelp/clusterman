import mock
import pytest
from botocore.exceptions import ClientError

from clusterman.batch.util import suppress_request_limit_exceeded


@mock.patch('clusterman.batch.util.yelp_meteorite')
@mock.patch('clusterman.batch.util.logger')
def test_suppress_rle(mock_logger, mock_meteorite):
    mock_counter = mock_meteorite.create_counter.return_value
    with suppress_request_limit_exceeded():
        raise ClientError({'Error': {'Code': 'RequestLimitExceeded'}}, 'foo')
    assert mock_logger.warning.call_count == 1
    assert mock_meteorite.create_counter.call_count == 1
    assert mock_counter.count.call_count == 1


@mock.patch('clusterman.batch.util.yelp_meteorite')
@mock.patch('clusterman.batch.util.logger')
def test_ignore_other_exceptions(mock_logger, mock_meteorite):
    mock_counter = mock_meteorite.create_counter.return_value
    with suppress_request_limit_exceeded(), pytest.raises(Exception):
        raise Exception('foo')
    assert mock_logger.warning.call_count == 0
    assert mock_meteorite.create_counter.call_count == 0
    assert mock_counter.count.call_count == 0
