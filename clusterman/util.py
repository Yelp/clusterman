import time
from datetime import datetime

import arrow
import colorlog
import parsedatetime
from colorama import Fore
from colorama import Style


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


def ask_for_choice(prompt, choices):
    enumerated_choices = [f'- {choice}: {num}\n' for num, choice in enumerate(choices)]
    full_prompt = prompt + '\n' + ''.join(enumerated_choices)

    current_prompt = full_prompt
    while True:
        ans = input(current_prompt).strip()
        if (not ans.isnumeric()) or int(ans) > len(choices):
            print('Please enter one of the numbers corresponding to a choice above.')
            current_prompt = prompt + ' '
            continue
        else:
            return choices[int(ans)]


def colored_status(status, green=None, blue=None, red=None, prefix=None, postfix=None):
    prefix = prefix or ''
    postfix = postfix or ''
    color_str = Fore.WHITE
    if green and status in green:
        color_str = Fore.GREEN
    elif blue and status in blue:
        color_str = Fore.BLUE
    elif red and status in red:
        color_str = Fore.RED
    combined_str = prefix + status + postfix
    return color_str + combined_str + Style.RESET_ALL


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


def splay_time_start(frequency, batch_name, region, timestamp=None):
    timestamp = timestamp or time.time()
    random_wait_time = hash(batch_name + region) % 60
    return frequency - timestamp % frequency + random_wait_time
