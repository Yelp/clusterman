Configuration
=============

There are two levels of configuration for Clusterman.
The first configures the Clusterman application or service itself, for operators of the service.
The second provides per-role configuration, for client applications to customize scaling behavior.

.. _service_configuration:

Service Configuration
-------------------

The following is an example configuration file for the core Clusterman service and application::

    aws:
        access_key_file: /etc/boto_cfg/clusterman.json
        region: us-west-1

    autoscale_signal:
        name: MostRecentCPU
        period_minutes: 10  # How frequently the signal will be evaluated.
        required_metrics:
            - name: cpus_allocated
              type: system_metrics
              minute_range: 10  # The metric will be queried for the most recent data in this range.

    autoscaling:
        cpus_per_weight: 8  # Conversion from CPUs to capacity units.
        default_signal_role: clusterman  # Module where the default signal is defined in clusterman_signals.
        setpoint: 0.7  # Percentage utilization that Clusterman will try to maintain.
        setpoint_margin: 0.1  # Clusterman will only scale if utilization is beyond this margin from the setpoint.

    batches:
        cluster_metrics:
            run_interval_seconds: 60  # How frequently the batch should run to collect metrics.

        spot_prices:
            dedupe_interval_seconds: 60  # # Max one price change for each (instance type, AZ) in this interval.
            run_interval_seconds: 60  # How frequently the batch should run to collect metrics.

    mesos_clusters:
        cluster-name:
            aws_region: us-west-2
            fqdn: <Mesos cluster FQDN>

    role_config_directory: /nail/srv/configs/clusterman-roles/

    module_config:
    - config:
         log_stream_name: clusterman
      file: /nail/srv/configs/clog.yaml
      initialize: yelp_servlib.clog_util.initialize
      namespace: clog

    - file: /nail/srv/configs/clusterman_metrics.yaml
      namespace: clusterman_metrics

The ``aws`` section provides the location of access credentials for the AWS API, as well as the region in which
Clusterman should operate.

The ``autoscale_signal`` section defines the default signal for autoscaling. This signal will be used for a role, if
that role does not define its own ``autoscale_signal`` section in its role configuration. See :ref:`default_signal`.

The ``autoscaling`` section defines settings for the autoscaling behavior of Clusterman.

The ``batches`` section configures specific Clusterman batches, such as the autoscaler and metrics collection batches.

The ``mesos_clusters`` section provides the location of the Mesos clusters which Clusterman knows about.

The ``module_config`` section loads additional configuration values for Clusterman modules, such as
``clusterman_metrics``.

.. _role_configuration:

Role Configuration
------------------

To configure a role, a directory with that role's name should be created in the ``role_config_directory``
defined in the service configuration. Within that directory, there should be a file named ``config.yaml``.
The following is an example configuration file for a particular Clusterman role::

    mesos:
        everywhere-testopia:
            resource_groups:
                s3:
                    bucket: clusterman-s3-bucket
                    prefix: cluster-name

    scaling_limits:
        min_capacity: 1
        max_capacity: 800
        max_weight_to_add: 100
        max_weight_to_remove: 100


    autoscale_signal:
        name: CustomSignal  # Must exist in the clusterman_signals.<role_name> module.
        period_minutes: 10  # How frequently the signal will be evaluated.
        required_metrics:
            - name: cpus_allocated
              type: system_metrics
              minute_range: 10  # The metric will be queried for the most recent data in this range.


The ``mesos`` section provides information for loading the :py:class:`MesosRoleManager <clusterman.mesos.mesos_role_manager.MesosRoleManager>` resource groups.
There must be one section for each Mesos cluster with this role that should be managed by Clusterman.

The ``scaling_limits`` section provides global role-level limits on scaling that the autoscaler and
other Clusterman commands should respect.

The ``autoscale_signal`` section defines the autoscaling signal used by this role.
This section is optional. If it is not present, then the ``autoscale_signal`` from the service configuration
will be used.

Reloading
---------
The autoscaling batch watches for changes to the service configuration and all role configuration files.
It only responds to signal-related changes; in those cases, it will automatically reload the signals.

TODO: details on methods for loading configs?
