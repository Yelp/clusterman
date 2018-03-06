from collections import namedtuple

import numpy as np

from clusterman.simulator.simulator import Simulator

ReportProperties = namedtuple('ReportProperties', [
    'title',
    'trend_rollup',
    'plot_title_formatter',
    'trend_axis_formatter',
    'legend_formatter',
    'trend_label',
    'get_data',
])


def DEFAULT_TREND_ROLLUP(data): return np.percentile(data, [25, 50, 75])


REPORT_TYPES = {
    'cost': ReportProperties(
        title='Cost',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Total cost: ${sum(data):,.2f}',
        trend_axis_formatter=lambda val: f'${val:,.2f}',
        legend_formatter=lambda val: f'${val:,.2f}/minute',
        trend_label='cost/minute',
        get_data=Simulator.cost_data,
    ),
    'capacity': ReportProperties(
        title='vCPU capacity',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average capacity: {int(np.mean(data))} vCPUs',
        trend_axis_formatter=int,
        legend_formatter=lambda val: f'{val} vCPUs',
        trend_label='vCPUs/day',
        get_data=Simulator.capacity_data,
    ),
    'cost_per_cpu': ReportProperties(
        title='Cost per vCPU',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average cost per vCPU: ${np.mean(data):,.4f}',
        trend_axis_formatter=lambda val: f'${val:,.4f}',
        legend_formatter=lambda val: f'${val:,.4f}/vCPU-minute',
        trend_label='cost/vCPU-minute',
        get_data=Simulator.cost_per_cpu_data,
    ),
}
