import arrow
import parsedatetime


def parse_time_string(time_str, tz='US/Pacific'):
    """ Convert a date or time string into an arrow object in UTC

    :param time_str: the string to convert
    :param tz: what timezone to interpret the time_str as
    :returns: an arrow object representing the time_str in UTC
    :raises ValueError: if the time_str could not be parsed
    """
    cal = parsedatetime.Calendar()
    parse_result = cal.parse(time_str)
    if parse_result[1] == 0:
        raise ValueError('Could not understand time {time}'.format(time=time_str))
    return arrow.get(parse_result[0]).replace(tzinfo=tz).to('utc')
