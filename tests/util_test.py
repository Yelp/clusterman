import arrow
import mock
import pysensu_yelp
import pytest
from colorama import Fore
from colorama import Style

from clusterman.util import ask_for_choice
from clusterman.util import ask_for_confirmation
from clusterman.util import colored_status
from clusterman.util import parse_time_interval_seconds
from clusterman.util import parse_time_string
from clusterman.util import sensu_checkin


@pytest.mark.parametrize('inp,response', [('\n', True), ('\n', False), ('yE', True), ('n', False)])
def test_ask_for_confirmation(inp, response):
    with mock.patch('builtins.input', side_effect=inp):
        assert ask_for_confirmation(default=response) == response


def test_ask_for_confirmation_invalid_input():
    with mock.patch('builtins.input', side_effect=['asdf', 'yes']) as mock_input:
        assert ask_for_confirmation() is True
        assert mock_input.call_count == 2


def test_ask_for_choice():
    with mock.patch('builtins.input', side_effect=['asdf', '-1', '20', '2']) as mock_input:
        assert ask_for_choice('Pick something', ['a', 'b', 'c']) == 'c'
        assert mock_input.call_count == 4


def test_colored_status():
    assert colored_status('foo', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == Fore.GREEN + 'foo' + Style.RESET_ALL
    assert colored_status('baz', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == Fore.BLUE + 'baz' + Style.RESET_ALL
    assert colored_status('hjkl', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == Fore.RED + 'hjkl' + Style.RESET_ALL
    assert colored_status('qwerty', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == \
        Fore.WHITE + 'qwerty' + Style.RESET_ALL


def test_parse_time_string_without_tz():
    t = parse_time_string('2017-08-01 00:00', tz='US/Eastern')
    assert t.timestamp == 1501560000


def test_parse_time_string_with_tz():
    # Ignore the tz argument here and use the '+04:00' in the string
    t = parse_time_string('2017-08-01T00:00:00+04:00', tz='US/Eastern')
    assert t.timestamp == 1501531200


def test_parse_time_string_non_arrow():
    t = parse_time_string('one hour ago', tz='US/Eastern')

    # This has potential to be a little flaky so there's a little wiggle room here
    actual_timestamp = arrow.now().replace(tzinfo='US/Eastern').shift(hours=-1).timestamp
    assert abs(actual_timestamp - t.timestamp) <= 1


def test_parse_time_interval_seconds():
    assert parse_time_interval_seconds('5m') == 60 * 5


def test_parse_time_interval_seconds_invalid():
    with pytest.raises(ValueError):
        parse_time_interval_seconds('asdf')


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
