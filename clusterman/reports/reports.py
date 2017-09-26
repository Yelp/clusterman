from datetime import timedelta
from datetime import tzinfo

import arrow
from matplotlib.figure import Figure

from clusterman.reports.constants import AXIS_DIMENSION_INCHES
from clusterman.reports.data_transforms import transform_heatmap_data
from clusterman.reports.data_transforms import transform_trend_data
from clusterman.reports.plots import generate_heatmap_trend_grid
from clusterman.reports.plots import PlotStruct
from clusterman.reports.report_types import REPORT_TYPES


def _make_report_title(fig, report, sim_metadata, months):
    report_title = f'{sim_metadata.cluster} {report.title} data '
    if len(months) > 1:
        report_title += 'from {start} to {end}'.format(
            start=months[0][0].format('MMMM YYYY'),
            end=months[-1][0].format('MMMM YYYY'),
        )
    else:
        report_title += 'for {month}'.format(month=months[0][0].format('MMMM YYYY'))
    fig.suptitle(report_title, fontsize=14)


def _make_axis_titles(report, report_data, months):
    titles = {}
    for mstart, mend in months:
        mstart_ind = report_data.bisect(mstart)
        mend_ind = report_data.bisect(mend)
        titles[mstart] = report.plot_title_formatter(report_data.values()[mstart_ind:mend_ind])
    return titles


def make_report(name, simulator, start_time, end_time, tz='US/Pacific'):
    """ Create a report for a clusterman simulation run

    The layout for this report is a set of rows (one for each month in the time range)
    with [heatmap, trend_line] charts in each row.  The overall cost for each month is printed
    for each row, and the overall cost is given in the title

    :param name: the name of the report to generate
    :param simulator: a Simulator object
    :param start_time: the earliest time for the report
    :param end_time: the latest time for the report
    :param tz: a timezone string or object to interpret the chart data in
    """
    begin = arrow.now()
    print(f'Generting {name} report...')
    report = REPORT_TYPES[name]
    report_data = report.get_data(simulator, start_time, end_time, timedelta(seconds=60))

    if not isinstance(tz, tzinfo):
        tz = arrow.parser.TzinfoParser.parse(tz)
    local_start = start_time.to(tz)
    local_end = end_time.to(tz)

    months = arrow.Arrow.span_range('month', local_start, local_end)
    fig = Figure(figsize=(AXIS_DIMENSION_INCHES[0], AXIS_DIMENSION_INCHES[1] * len(months)))
    _make_report_title(fig, report, simulator.metadata, months)

    heatmap_data, *heatmap_range = transform_heatmap_data(report_data, months, tz)
    trend_data, *trend_range = transform_trend_data(report_data, months, report.trend_rollup)

    heatmap = PlotStruct(heatmap_data, heatmap_range, _make_axis_titles(report, report_data, months))
    trend = PlotStruct(trend_data, trend_range, report.trend_label, report.trend_axis_formatter)

    generate_heatmap_trend_grid(fig, heatmap, trend, months, tz)
    fig.savefig(f'{name}.png', dpi=300)
    print('Done!  Report saved as {name}.png ({time}s)'.format(
        name=name,
        time=(arrow.now() - begin).total_seconds(),
    ))
