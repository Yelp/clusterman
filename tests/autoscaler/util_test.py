import mock
import pytest

from clusterman.autoscaler.util import get_average_cpu_util
from clusterman.exceptions import MetricsError


@mock.patch('clusterman.autoscaler.util.ClustermanMetricsBotoClient', autospec=True)
def test_get_average_cpu_util(mock_metrics_client):
    mock_metrics_client.return_value.get_metric_values.return_value = 'asdf', [
        [0, 0.8],
        [2, 0.1],
        [3, 0.3],
        [4, 0.6],
        [7, 0.4],
        [9, 0.4],
    ]

    assert pytest.approx(get_average_cpu_util('fake', 'baz', 95),  0.433333)


@mock.patch('clusterman.autoscaler.util.ClustermanMetricsBotoClient', autospec=True)
def test_get_average_cpu_util_no_data(mock_metrics_client):
    mock_metrics_client.return_value.get_metric_values.return_value = 'asdf', []
    with pytest.raises(MetricsError):
        get_average_cpu_util('fake', 'baz', 10)
