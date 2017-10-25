import pytest
import staticconf.testing


@pytest.fixture
def mock_autoscaler_config_dict():
    return {
        'defaults': {
            'max_capacity': 5000,
            'min_capacity': 24,
            'max_weight_to_add': 200,
            'max_weight_to_remove': 10,
        },
        'autoscale_signals': [
            {
                'name': 'FakeSignalOne',
                'priority': 1,
                'param1': 42,
                'param2': 'asdf',
            },
            {
                'name': 'FakeSignalTwo',
                'paramA': 24,
                'paramB': 'fdsa',
            },
            {
                'name': 'FakeSignalThree',
                'priority': 1,
            },
            {
                'name': 'FakeSignalFour',
                'priority': 7,
            },
            {
                'name': 'MissingParamSignal',
            },
        ]
    }


@pytest.fixture
def mock_autoscaler_config(mock_autoscaler_config_dict):
    with staticconf.testing.MockConfiguration(mock_autoscaler_config_dict, namespace='bar_config'):
        yield
