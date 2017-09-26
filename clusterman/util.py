import json
from datetime import datetime

import arrow
import colorlog
import parsedatetime


class DataPoint:
    def __init__(self, timestamp, value):
        self.timestamp = timestamp
        self.value = value


class DataPointEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DataPoint):
            return [str(obj.timestamp), obj.value]


def timeseries_decoder(obj):
    if '__data__' in obj:
        return [(parse_time_string(pt[0]), float(pt[1])) for pt in obj['__data__']]
    return obj


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
            t.replace(tzinfo=tz)
    except arrow.parser.ParserError:
        cal = parsedatetime.Calendar()
        parse_result = cal.parse(time_str)
        if parse_result[1] == 0:
            raise ValueError('Could not understand time {time}'.format(time=time_str))
        t = arrow.get(parse_result[0]).replace(tzinfo=tz)
    return t.to('utc')


def parse_time_interval_seconds(time_str):
    cal = parsedatetime.Calendar()
    parse_result = cal.parseDT(time_str, sourceTime=datetime.min)
    if parse_result[1] == 0:
        raise ValueError('Could not understand time {time}'.format(time=time_str))
    return (parse_result[0] - datetime.min).total_seconds()
