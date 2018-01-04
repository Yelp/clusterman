import mock
import pysensu_yelp
import pytest

from clusterman.batch.util import sensu_checkin


@pytest.mark.parametrize('noop', [True, False])
@mock.patch('pysensu_yelp.send_event', autospec=True)
def test_sensu_checkin(mock_sensu, noop):
    sensu_checkin(
        'my_check',
        'output',
        '10m',
        '20m',
        'my_source',
        noop=noop,
    )

    if noop:
        assert mock_sensu.call_count == 0
    else:
        assert mock_sensu.call_args_list == [mock.call(
            name='my_check',
            output='output',
            check_every='10m',
            ttl='20m',
            source='my_source',
            status=pysensu_yelp.Status.OK,
            runbook=mock.ANY,
            team=mock.ANY,
            alert_after=mock.ANY,
            page=mock.ANY,
        )]
