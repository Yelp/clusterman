Configuration
=============

There are two levels of configuration for Clusterman.
The first configures the Clusterman application or service itself, for operators of the service.
The second provides per-role configuration, for client applications to customize scaling behavior.

.. _service_configuration:

Service Configuration
----------------------

The following is an example configuration file for the core Clusterman service and application:

.. code-block:: yaml

    aws:
        access_key_file: /etc/boto_cfg/clusterman.json
        region: us-west-1

    autoscale_signal:
        name: MostRecentCPU

        # What version of the signal to use (a branch or tag in the clusterman_signals Git repo)
        branch_or_tag: v1.0.2

        # How frequently the signal will be evaluated.
        period_minutes: 10

        required_metrics:
            - name: cpus_allocated
              type: system_metrics

              # The metric will be queried for the most recent data in this range.
              minute_range: 10

    autoscaling:
        # Conversion from CPUs to capacity units.
        cpus_per_weight: 8

        # Module where the default signal is defined in clusterman_signals.
        default_signal_role: clusterman

        # Percentage utilization that Clusterman will try to maintain.
        setpoint: 0.7

        # Clusterman will only scale if utilization is beyond this margin from the setpoint.
        setpoint_margin: 0.1

    batches:
        cluster_metrics:
            # How frequently the batch should run to collect metrics.
            run_interval_seconds: 60

        spot_prices:
            # Max one price change for each (instance type, AZ) in this interval.
            dedupe_interval_seconds: 60

            # How frequently the batch should run to collect metrics.
            run_interval_seconds: 60

    mesos_clusters:
        cluster-name:
            aws_region: us-west-2
            fqdn: <Mesos cluster FQDN>

    role_config_directory: /nail/srv/configs/clusterman-roles/

    module_config:
      - namespace: clog
        config:
            log_stream_name: clusterman
        file: /nail/srv/configs/clog.yaml
        initialize: yelp_servlib.clog_util.initialize

      - namespace: clusterman_metrics
        file: /nail/srv/configs/clusterman_metrics.yaml

      - namespace: yelp_batch
        config:
            watchers:
              - aws_key_rotation: /etc/boto_cfg/clusterman.json
              - clusterman_yaml: /nail/srv/configs/clusterman.yaml


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
The following is an example configuration file for a particular Clusterman role:

.. code-block:: yaml

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
        # Must exist in the clusterman_signals.<role_name> module.
        name: CustomSignal

        # What version of the signal to use (a branch or tag in the clusterman_signals Git repo)
        branch_or_tag: v3.7

        # How frequently the signal will be evaluated.
        period_minutes: 10

        required_metrics:
            - name: cpus_allocated
              type: system_metrics

              # The metric will be queried for the most recent data in this range.
              minute_range: 10


The ``mesos`` section provides information for loading the :py:class:`MesosRoleManager <clusterman.mesos.mesos_role_manager.MesosRoleManager>` resource groups.
There must be one section for each Mesos cluster with this role that should be managed by Clusterman.

The ``scaling_limits`` section provides global role-level limits on scaling that the autoscaler and
other Clusterman commands should respect.

The ``autoscale_signal`` section defines the autoscaling signal used by this role.
This section is optional. If it is not present, then the ``autoscale_signal`` from the service configuration
will be used.

Reloading
---------
The Clusterman batches will automatically reload on changes to the clusterman service config file and the AWS
credentials file.  This is specified in the ``namespace: yelp_batch`` section of the main configuration file.  The
autoscaler batch and the metrics collector batch also will automatically reload for changes to any roles that are
configured to run on the specified cluster.
