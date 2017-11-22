from datetime import datetime
from functools import partial

import arrow
import colorlog
import parsedatetime
import staticconf
from colorama import Fore
from colorama import Style
from yelp_servlib.config_util import load_default_config


def ask_for_confirmation(prompt='Are you sure? ', default=True):
    """ Display a prompt asking for confirmation from the user before continuing; accepts any form of "yes"/"no"

    :param prompt: the prompt to display before accepting input
    :param default: the default value if CR pressed with no input
    :returns: True if "yes", False if "no"
    """
    yes, no = ('Y', 'n') if default else ('y', 'N')
    prompt += f'[{yes}/{no}] '

    while True:
        ans = input(prompt).lower().strip()
        if not ans:
            return default
        elif not ('yes'.startswith(ans) or 'no'.startswith(ans)):
            print('Please enter yes or no.')
            continue
        else:
            return 'yes'.startswith(ans)


def colored_status(status, active, changing, inactive):
    color_str = ''
    if status in active:
        color_str = Fore.GREEN
    elif status in changing:
        color_str = Fore.BLUE
    elif status in inactive:
        color_str = Fore.RED
    return (color_str + status + Style.RESET_ALL if color_str else status)


def get_clusterman_logger(name):
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger = colorlog.getLogger(name)
    logger.addHandler(handler)
    return logger


def parse_time_string(time_str, tz='US/Pacific'):
    """ Convert a date or time string into an arrow object in UTC

    :param time_str: the string to convert
    :param tz: what timezone to interpret the time_str as *if no tz is specified*
    :returns: an arrow object representing the time_str in UTC
    :raises ValueError: if the time_str could not be parsed
    """

    # parsedatetime doesn't handle ISO-8601 time strings (YYYY-MM-DDThh:mm:ss+zz) so
    # try to parse it with arrow first and then use parsedatetime as a fallback (grumble)
    t = None
    try:
        t = arrow.get(time_str)
        # If the input string didn't specify a timezone, fill in the default
        if len(time_str.split('+')) == 1:
            t = t.replace(tzinfo=tz)
    except arrow.parser.ParserError:
        cal = parsedatetime.Calendar()
        parse_result = cal.parse(time_str)
        if parse_result[1] == 0:
            raise ValueError('Could not understand time {time}'.format(time=time_str))
        t = arrow.get(parse_result[0]).replace(tzinfo=tz)
    return t.to('utc')


def parse_time_interval_seconds(time_str):
    """ Convert a given time interval (e.g. '5m') into the number of seconds in that interval

    :param time_str: the string to parse
    :returns: the number of seconds in the interval
    :raises ValueError: if the string could not be parsed
    """
    cal = parsedatetime.Calendar()
    parse_result = cal.parseDT(time_str, sourceTime=datetime.min)
    if parse_result[1] == 0:
        raise ValueError('Could not understand time {time}'.format(time=time_str))
    return (parse_result[0] - datetime.min).total_seconds()


def setup_config(args):
    load_default_config(args.env_config_path)
    # If a cluster is specified, the CLI should operate on that AWS region.
    if getattr(args, 'cluster', None):
        cluster_region = staticconf.read_string('mesos_clusters.{cluster}.aws_region'.format(cluster=args.cluster))
        staticconf.DictConfiguration({'aws': {'region': cluster_region}})


def build_watcher(filename, namespace):
    config_loader = partial(staticconf.YamlConfiguration, filename, namespace=namespace)
    reloader = staticconf.config.ReloadCallbackChain(namespace)
    return staticconf.config.ConfigurationWatcher(config_loader, filename, reloader=reloader)
