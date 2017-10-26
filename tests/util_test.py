import argparse

import arrow
import mock
import pytest
import staticconf
import staticconf.testing
from colorama import Fore
from colorama import Style

from clusterman.util import ask_for_confirmation
from clusterman.util import colored_status
from clusterman.util import parse_time_interval_seconds
from clusterman.util import parse_time_string
from clusterman.util import setup_config
from tests.mesos.conftest import cluster_configs


@pytest.mark.parametrize('inp,response', [('\n', True), ('\n', False), ('yE', True), ('n', False)])
def test_ask_for_confirmation(inp, response):
    with mock.patch('builtins.input', side_effect=inp):
        assert ask_for_confirmation(default=response) == response


def test_ask_for_confirmation_invalid_input():
    with mock.patch('builtins.input', side_effect=['asdf', 'yes']) as mock_input:
        assert ask_for_confirmation() is True
        assert mock_input.call_count == 2


def test_colored_status():
    assert colored_status('foo', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == Fore.GREEN + 'foo' + Style.RESET_ALL
    assert colored_status('baz', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == Fore.BLUE + 'baz' + Style.RESET_ALL
    assert colored_status('hjkl', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == Fore.RED + 'hjkl' + Style.RESET_ALL
    assert colored_status('qwerty', ('foo', 'bar'), ('baz', 'asdf'), ('hjkl',)) == 'qwerty'


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


@mock.patch('clusterman.util.load_default_config')
def test_setup_config_no_cluster(mock_load_config):
    args = argparse.Namespace(env_config_path='/nail/etc/config.yaml')
    setup_config(args)
    assert mock_load_config.call_args_list == [mock.call('/nail/etc/config.yaml')]


@mock.patch('clusterman.util.load_default_config')
def test_setup_config_cluster(mock_load_config):
    args = argparse.Namespace(env_config_path='/nail/etc/config.yaml', cluster='mesos-test')
    with staticconf.testing.MockConfiguration(cluster_configs()):
        setup_config(args)
        assert mock_load_config.call_args_list == [mock.call('/nail/etc/config.yaml')]
        assert staticconf.read_string('aws.region') == 'us-test-3'
