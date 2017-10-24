from clusterman.autoscaler.signals.default.cpu_util import CpuUtilizationSignal


def load_default_signals(role, signal_list):
    signal_list.append(CpuUtilizationSignal())


def load_client_signals(role, signal_list):
    pass


def constrain_cluster_delta(fleet_config, fleet_delta, target_capacity):
    """Restrict the (arbitrarily-large) scaling request returned by a signal

    :param fleet_config: fleet configuration information
    :param fleet_delta: The requested amount to change the cluster size by
    :param target_capacity: The current capacity of the cluster
    :returns: The delta value constrained by capacity and scaling limits

    """
    if fleet_delta > 0:
        return min(
            fleet_config['max_unit_capacity'] - target_capacity,
            fleet_config['max_units_to_add'],
            fleet_delta,
        )
    elif fleet_delta < 0:
        return max(
            fleet_config['min_unit_capacity'] - target_capacity,
            -fleet_config['max_units_to_remove'],
            fleet_delta,
        )
    else:
        return 0
