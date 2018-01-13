from datetime import timedelta
from datetime import tzinfo

import arrow
from matplotlib.cm import get_cmap
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from clusterman.reports.constants import AXIS_DIMENSION_INCHES
from clusterman.reports.constants import COLORMAP
from clusterman.reports.constants import FIGURE_DPI
from clusterman.reports.constants import SUBTITLE_SPACING
from clusterman.reports.constants import TREND_LINE_COLOR
from clusterman.reports.constants import TREND_RANGE_ALPHA
from clusterman.reports.constants import TREND_RANGE_COLOR
from clusterman.reports.data_transforms import transform_heatmap_data
from clusterman.reports.data_transforms import transform_trend_data
from clusterman.reports.plots import generate_heatmap_trend_grid
from clusterman.reports.plots import PlotStruct
from clusterman.reports.report_types import REPORT_TYPES


def _make_report_title(fig, report, sim_metadata, months):
    report_title = f'{report.title} '
    if len(months) > 1:
        report_title += 'from {start} to {end}'.format(
            start=months[0][0].format('MMMM YYYY'),
            end=months[-1][0].format('MMMM YYYY'),
        )
    else:
        report_title += 'for {month}'.format(month=months[0][0].format('MMMM YYYY'))
    title = fig.suptitle(report_title, fontsize=14)
    y_axis_points = AXIS_DIMENSION_INCHES[1] * FIGURE_DPI * len(months)
    subtitle_abs_y = y_axis_points * title.get_position()[1] - SUBTITLE_SPACING
    subtitle_rel_y = subtitle_abs_y / y_axis_points
    fig.text(
        0.5, subtitle_rel_y,
        f'{sim_metadata.name}\nCluster: {sim_metadata.cluster}; Role: {sim_metadata.role}',
        va='top', ha='center',
        fontsize=6,
    )


def _make_heatmap_legend_marker(color, label):
    return Line2D(
        [0, 1], [0, 0],  # These don't matter
        marker='o', markerfacecolor=color,
        color='white',  # The line is white so it doesn't appear in the legend box, just the marker
        label=label,
    )


def _make_legend(fig, heatmap_range, legend_formatter):
    cmap = get_cmap(COLORMAP)
    low, high = cmap(0.0), cmap(1.0)
    low_marker = _make_heatmap_legend_marker(low, 'min (p5): ' + legend_formatter(heatmap_range[0]))
    high_marker = _make_heatmap_legend_marker(high, 'max (p95): ' + legend_formatter(heatmap_range[1]))
    fig.legend(handles=[low_marker, high_marker], loc='upper left', fontsize=6)

    trend_line = Line2D([0, 1], [0, 0], color=TREND_LINE_COLOR, label='average')
    bestfit_line = Line2D([0, 1], [0, 0], color='black', dashes=(1, 1), linewidth=0.75, label='best fit line')
    trend_patch = Patch(color=TREND_RANGE_COLOR, alpha=TREND_RANGE_ALPHA, label='interquartile range', linewidth=0)
    fig.legend(handles=[trend_line, bestfit_line, trend_patch], loc='upper right', fontsize=6)


def _make_axis_titles(report, report_data, months):
    titles = {}
    for mstart, mend in months:
        mstart_ind = report_data.bisect(mstart)
        mend_ind = report_data.bisect(mend)
        titles[mstart] = report.plot_title_formatter(report_data.values()[mstart_ind:mend_ind])
    return titles


def make_report(name, simulator, start_time, end_time, output_prefix='', tz='US/Pacific'):
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
    print(f'Generating {name} report...')
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
    _make_legend(fig, heatmap_range, report.legend_formatter)

    if output_prefix:
        output_prefix += '_'
    output_file = f'{output_prefix}{name}.png'
    processing_time = (arrow.now() - begin).total_seconds()
    fig.savefig(output_file, dpi=FIGURE_DPI)
    print(f'Done!  Report saved as {output_file} ({processing_time}s)')
