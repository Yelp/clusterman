from argparse import ArgumentError
from argparse import Namespace

import mock
import pytest

from clusterman.simulator.run import main


@pytest.fixture
def args():
    return Namespace(
        start_time='2018-01-01 00:00:00',
        end_time='2018-01-01 00:00:00',
        cluster='foo',
        role='bar',
        cluster_config_dir='baz',
        metrics_data_files=None,
        simulation_result_file=None,
        reports=None,
        comparison_operator='div',
    )


def test_main_too_many_compares(args):
    args.compare = ['sim1', 'sim2', 'sim3']
    with pytest.raises(ArgumentError):
        main(args)


@pytest.mark.parametrize('compare', [[], ['sim1'], ['sim1', 'sim2']])
def test_main_compare_param(compare, args):
    args.compare = compare
    with mock.patch('clusterman.simulator.run.read_object_from_compressed_json') as mock_read, \
            mock.patch('clusterman.simulator.run.write_object_to_compressed_json') as mock_write, \
            mock.patch('clusterman.simulator.run.setup_config') as mock_config, \
            mock.patch('clusterman.simulator.run._load_metrics') as mock_load_metrics, \
            mock.patch('clusterman.simulator.run._run_simulation') as mock_run_simulation, \
            mock.patch('clusterman.simulator.run.operator') as mock_operator:
        main(args)
        expected_call_count = 1 if len(compare) < 2 else 0
        assert mock_config.call_count == expected_call_count
        assert mock_load_metrics.call_count == expected_call_count
        assert mock_run_simulation.call_count == expected_call_count
        assert mock_read.call_count == len(compare)
        assert mock_write.call_count == 0
        assert mock_operator.div.call_count == (len(compare) > 0)
