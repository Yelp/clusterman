import arrow
import numpy as np
from matplotlib.dates import DateFormatter


def plot_heatmap(ax, x, y, z, mstart, mend, tz, **kwargs):
    ax.scatter(
        x, y,
        c=z,
        alpha=0.5,
        linewidths=0,
        s=3,
        cmap='plasma',
        **kwargs,
    )
    # Global plot settings
    ax.patch.set_facecolor('black')

    # x-axis settings
    ax.set_xlim(arrow.get(0).replace(tzinfo=tz).datetime, arrow.get(0).replace(tzinfo=tz).ceil('day').datetime)
    ax.xaxis.set_major_formatter(DateFormatter('%H:%M', tz=tz))
    ax.xaxis.set_tick_params(labelsize=8)

    # y-axis settings
    ax.set_ylim(mstart.shift(days=-1).datetime, mend.datetime)
    ax.yaxis.set_major_formatter(DateFormatter('%m-%d', tz=tz))
    ax.yaxis.set_tick_params(direction='out', labelsize=8)
    ax.yaxis.set_ticks([r.datetime for r in arrow.Arrow.range('week', mstart, mend)])
    ax.set_ylabel(mstart.format('MMMM'), fontsize=12, labelpad=5)


def plot_trend(ax, x, q1, y, q3, xlim, ylim, ylabel):
    # compute the best-fit (linear) trend line
    fit = np.polyfit(x, y, 1)
    fit_fn = np.poly1d(fit)

    # plot the data, the trendlines, and the interquartile range (if given)
    ax.plot(x, y, '-b')
    ax.plot(x, fit_fn(x), '--k', dashes=(1, 1))
    if all(q1) and all(q3):
        ax.fill_between(x, q1, q3, facecolor='lightblue', alpha=0.5, linewidths=0)

    # x-axis settings
    ax.set_xlim(*xlim)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.xaxis.set_tick_params(labelsize=8)
    ax.set_xlabel('Day of month', fontsize=10)

    # y-axis settings
    ax.set_ylim(*ylim)
    ax.spines['right'].set_visible(False)
    ax.yaxis.set_ticks_position('left')
    ax.yaxis.set_tick_params(labelsize=8)
    ax.set_ylabel(ylabel, fontsize=10)
