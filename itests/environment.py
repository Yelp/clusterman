import behave
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS

behave.use_step_matcher('re')
BEHAVE_DEBUG_ON_ERROR = False


@behave.fixture
def setup_configurations(context):
    main_clusterman_config = {
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
            'signals_bucket': 'the_bucket',
        },
        'autoscaling': {
            'setpoint': 0.7,
            'setpoint_margin': 0.1,
            'default_signal_role': 'foo',
        },
        'batches': {
            'spot_prices': {
                'run_interval_seconds': 120,
                'dedupe_interval_seconds': 60,
            },
            'cluster_metrics': {
                'run_interval_seconds': 120,
            },
        },
        'mesos_clusters': {
            'mesos-test': {
                'fqdn': 'the.mesos.leader',
                'aws_region': 'us-west-2',
                'max_weight_to_add': 200,
                'max_weight_to_remove': 10,
            },
        },
        'sensu_config': [
            {
                'team': 'my_team',
                'runbook': 'y/my-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'DefaultSignal',
            'branch_or_tag': 'master',
            'period_minutes': 10,
        }
    }

    pool_config = {
        'resource_groups': [
            {
                'sfr': {
                    's3': {
                        'bucket': 'fake-bucket',
                        'prefix': 'none',
                    }
                },
            },
        ],
        'scaling_limits': {
            'min_capacity': 3,
            'max_capacity': 345,
        },
        'sensu_config': [
            {
                'team': 'other-team',
                'runbook': 'y/their-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'BarSignal3',
            'branch_or_tag': 'v42',
            'period_minutes': 7,
            'required_metrics': [
                {'name': 'cpus_allocated', 'type': SYSTEM_METRICS, 'minute_range': 10},
                {'name': 'cost', 'type': APP_METRICS, 'minute_range': 30},
            ],
        }
    }
    with staticconf.testing.MockConfiguration(main_clusterman_config), \
            staticconf.testing.MockConfiguration(pool_config, namespace='bar_config'):
        yield


def before_all(context):
    global BEHAVE_DEBUG_ON_ERROR
    BEHAVE_DEBUG_ON_ERROR = context.config.userdata.getbool('BEHAVE_DEBUG_ON_ERROR')
    behave.use_fixture(setup_configurations, context)


def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == 'failed':
        import ipdb
        ipdb.post_mortem(step.exc_traceback)
