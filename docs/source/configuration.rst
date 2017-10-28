Configuration
=============

There are two levels of configuration for Clusterman; first configures the Clusterman application or service itself, and
the second provides per-client configuration.

Clusterman Configuration
------------------------

The following is an example configuration file for the core Clusterman service and application::

   aws:
       access_key_file: /etc/boto_cfg/clusterman.json
       region: us-west-1

   batches:
        cluster_metrics:
            run_interval_seconds: 60

        spot_prices:
            dedupe_interval_seconds: 60
            run_interval_seconds: 60

    mesos_clusters:
       cluster-name:
            aws_region: us-west-2
            leader_service: <Mesos master service hostname/port>

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

The ``batches`` section configures specific Clusterman batches, such as the autoscaler and metrics collection batches.

The ``mesos_clusters`` section provides the location of the Mesos clusters which Clusterman knows about.

The ``module_config`` section loads additional configuration values for Clusterman modules, such as
``clusterman_metrics``.

Clusterman Role Configuration
-----------------------------

The following is an example configuration file for a particular Clusterman role::

    mesos:
        resource_groups:
            s3:
                bucket: clusterman-s3-bucket
                prefix: cluster-name

    defaults:
        min_capacity: 1
        max_capacity: 800
        max_weight_to_add: &max_weight_to_add 100
        max_weight_to_remove: &max_weight_to_remove 100


    autoscale_signals:
        - name: ClusterOverutilizedSignal
          priority: 0
          query_period_minutes: 10
          scale_up_threshold: 0.65
          units_to_add: *max_weight_to_add

        - name: ClusterUnderutilizedSignal
          priority: 1
          query_period_minutes: 60
          scale_down_threshold: 0.20
          units_to_remove: *max_weight_to_remove

The ``mesos`` section provides information for loading the :ref:`MesosRoleManager` resource groups.

The ``defaults`` section provides global role-level defaults that the autoscaler and other Clusterman commands should
respect.

The ``autoscale_signals`` section includes information for each signal which is used by this role.
