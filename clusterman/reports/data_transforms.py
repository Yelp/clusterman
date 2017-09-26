import arrow
import numpy as np


def transform_heatmap_data(data, months, tz):
    """ Transform input data into positions and values for heatmap plotting

    :param data: a SortedDict mapping from timestamp -> value
    :param months: a list of (mstart, mend) tuples for grouping the output data
    :param tz: what timezone the output data should be interpreted as
    :returns: a dict of month -> [<x-data>, <y-data>, <values>] lists
    """
    data_by_month = {}
    for mstart, mend in months:
        mstart_index = data.bisect_left(mstart)
        mend_index = data.bisect_right(mend)

        # We want the y-axis to just be a date (year-month-day) and the x-axis to just be a time (hour-minute-second)
        # However, the matplotlib DatetimeFormatter won't take just date or time objects; so to get a canonical
        # datetime object we use the beginning UNIX epoch time and then replace the date/time with the correct values
        aggregated_monthly_data = [(
            data.keys()[i].to(tz).replace(year=1970, month=1, day=1).datetime,
            data.keys()[i].to(tz).replace(hour=0, minute=0, second=0, microsecond=0).datetime,
            data.values()[i],
        ) for i in range(mstart_index, mend_index)]
        data_by_month[mstart] = zip(*aggregated_monthly_data)
    return data_by_month


def transform_trend_data(data, months, trend_type):
    """ Transform input data into (x,y) values aggregated over each day of the month

    :param data: a SortedDict mapping from timestamp -> value
    :param months: a list of (mstart, mend) tuples for grouping the output data
    :param trend_type: how to aggregate the data; supported values are
        * mean: take the average and interquartile range of the daily data
        * sum: sum the daily data (no range values)
    :returns: a dict of month -> [<day-of-month>, <lower-range>, <aggregated-value>, <upper-range>] lists,
        as well as the min/max aggregated values
    """
    data_by_month = {}
    min_val = 0
    max_val = 0
    for mstart, mend in months:
        aggregated_daily_data = []

        # For each day in the month aggregate the data according to the chosen method
        for dstart, dend in arrow.Arrow.span_range('day', mstart, mend):
            dstart_index = data.bisect_left(dstart)
            dend_index = data.bisect_right(dend)
            day_slice = data.values()[dstart_index:dend_index]
            if not day_slice:  # if there's no data for a given day, np.percentile will fail
                continue

            if trend_type == 'mean':
                q1, q2, q3 = np.percentile(day_slice, (25, 50, 75))
                min_val = min(min_val, q1)
                max_val = max(max_val, q3)
            elif trend_type == 'sum':
                q1, q2, q3 = None, np.sum(day_slice), None
                min_val = min(min_val, q2)
                max_val = max(max_val, q2)

            aggregated_daily_data.append((dstart.day, q1, q2, q3))

        data_by_month[mstart] = list(zip(*aggregated_daily_data))
    return data_by_month, min_val, max_val
