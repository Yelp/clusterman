import sys

import arrow
import mock
import pysensu_yelp
import pytest
from colorama import Fore
from colorama import Style

from clusterman.util import ask_for_choice
from clusterman.util import ask_for_confirmation
from clusterman.util import color_conditions
from clusterman.util import parse_time_interval_seconds
from clusterman.util import parse_time_string
from clusterman.util import sensu_checkin


@pytest.fixture(autouse=True)
def mock_clog():
    sys.modules['clog'] = mock.Mock()  # clog is imported in the main function so this is how we mock it
    yield


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


@pytest.mark.parametrize('input_str,color', [
    ('foo', Fore.GREEN),
    ('baz', Fore.BLUE),
    ('hjkl', Fore.RED),
    ('qwerty', '')
])
def test_color_conditions(input_str, color):
    assert color_conditions(
        input_str,
        green=('foo', 'bar'),
        blue=('baz', 'asdf'),
        red=('hjkl',)
    ) == color + input_str + Style.RESET_ALL


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


@mock.patch('clusterman.util.pysensu_yelp', autospec=True)
class TestSensu:
    @pytest.mark.parametrize('noop', [True, False])
    def test_sensu_checkin(self, mock_sensu, noop):
        sensu_checkin(
            check_name='my_check',
            output='output',
            source='my_source',
            noop=noop,
        )

        if noop:
            assert mock_sensu.send_event.call_count == 0
        else:
            assert mock_sensu.send_event.call_args == mock.call(
                name='my_check',
                output='output',
                source='my_source',
                status=pysensu_yelp.Status.OK,
                runbook='y/my-runbook',
                team='my_team',
                page=True,
            )

    @pytest.mark.parametrize('app', [None, 'bar'])
    def test_args_overrides_config(self, mock_sensu, app):
        sensu_checkin(
            check_name='my_check',
            output='output',
            source='my_source',
            team='a_different_team',
            app=app,
        )
        expected_runbook = 'y/my-runbook' if not app else 'y/their-runbook'
        assert mock_sensu.send_event.call_args == mock.call(
            name='my_check',
            output='output',
            source='my_source',
            status=pysensu_yelp.Status.OK,
            runbook=expected_runbook,
            team='a_different_team',
            page=True,
        )

    def test_fallback(self, mock_sensu):
        sensu_checkin(
            check_name='my_check',
            output='output',
            source='my_source',
            app='non-existent',
        )
        assert mock_sensu.send_event.call_args == mock.call(
            name='my_check',
            output='output',
            source='my_source',
            status=pysensu_yelp.Status.OK,
            runbook='y/my-runbook',
            team='my_team',
            page=True,
        )
