from datetime import tzinfo

import arrow
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec

from clusterman.reports.data_transforms import transform_heatmap_data
from clusterman.reports.data_transforms import transform_trend_data
from clusterman.reports.plots import plot_heatmap
from clusterman.reports.plots import plot_trend


def _report_title(fig, sim_metadata, total_cost, months):
    report_title = '{cluster} cost data '.format(cluster=sim_metadata.cluster)
    if len(months) > 1:
        report_title += 'from {start} to {end}'.format(
            start=months[0][0].format('MMMM YYYY'),
            end=months[-1][0].format('MMMM YYYY'),
        )
    else:
        report_title += 'for {month}'.format(month=months[0][0].format('MMMM YYYY'))
    title = fig.suptitle(report_title, fontsize=14)
    title_x, title_y = title.get_position()

    report_subtitle = 'Total cost: ${total_cost:.2f}'.format(total_cost=total_cost)
    fig.text(title_x, title_y - 0.15, report_subtitle, ha='center', fontsize=10)


def _generate_grid(fig, heatmap_data, trend_data, trend_ylim, months, tz):
    grid = GridSpec(len(months), 2, width_ratios=[3, 1])
    for i, (mstart, mend) in enumerate(reversed(months)):
        ax = fig.add_subplot(grid[2 * i])
        plot_heatmap(ax, *heatmap_data[mstart], mstart, mend, tz, vmin=0, vmax=1)
        if i != len(months) - 1:
            ax.xaxis.set_visible(False)

        ax = fig.add_subplot(grid[2 * i + 1])
        plot_trend(ax, *trend_data[mstart], xlim=[mstart.day, mend.day], ylim=trend_ylim, ylabel='Average cost')

    fig.tight_layout(rect=[0, 0, 1, 0.85])


def make_cost_report(sim_metadata, sim_data, start_time, end_time, tz='US/Pacific'):
    """ Create a report of cluster cost based on simulation data

    The layout for this report is a set of rows (one for each month in the time range)
    with [heatmap, trend_line] charts in each row.  The overall cost for each month is printed
    for each row, and the overall cost is given in the title

    :param sim_metadata: a SimulationMetadata object
    :param sim_data: a SortedDict mapping from timestamp -> cost value
    :param start_time: the earliest time for the report
    :param end_time: the latest time for the report
    :param tz: a timezone string or object to interpret the chart data in
    """
    if not isinstance(tz, tzinfo):
        tz = arrow.parser.TzinfoParser.parse(tz)
    local_start = start_time.to(tz)
    local_end = end_time.to(tz)
    months = arrow.Arrow.span_range('month', local_start, local_end)

    fig = Figure(figsize=(8, 2.5))
    total_cost = sum(sim_data.values())
    _report_title(fig, sim_metadata, total_cost, months),

    heatmap_data = transform_heatmap_data(sim_data, months, tz)
    trend_data, min_val, max_val = transform_trend_data(sim_data, months, 'mean')

    _generate_grid(fig, heatmap_data, trend_data, [min_val, max_val * 1.05], months, tz)
    return fig
