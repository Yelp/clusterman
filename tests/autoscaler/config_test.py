import staticconf.testing

from clusterman.autoscaler.config import get_autoscaling_config


def test_get_autoscaling_config():
    default_autoscaling_values = {
        'setpoint': 0.7,
        'setpoint_margin': 0.1,
    }
    pool_autoscaling_values = {
        'setpoint': 0.8,
    }
    with staticconf.testing.MockConfiguration({'autoscaling': default_autoscaling_values}), \
            staticconf.testing.MockConfiguration({'autoscaling': pool_autoscaling_values}, namespace='pool_namespace'):
        autoscaling_config = get_autoscaling_config('pool_namespace')

        assert autoscaling_config.setpoint == 0.8
        assert autoscaling_config.setpoint_margin == 0.1
