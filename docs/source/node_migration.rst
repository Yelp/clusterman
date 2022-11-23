Node Migration
==============

*Node Migration* is a functionality which allows Clusterman to recycle nodes of a pool
according to various criteria, in order to reduce the amount of manual work necessary
when performing infrastructure migrations.

**NOTE**: this is only compatible with Kubernetes clusters.


Node Migration Batch
--------------------

The *Node Migration batch* is the entrypoint of the migration logic. It takes care of fetching migration trigger
events, spawning the worker processes actually performing the node recycling procedures, and monitoring their health.

Batch specific configuration values are described as part of the main service configuration in :ref:`service_configuration`.

The batch code can be invoked from the ``clusterman.batch.node_migration`` Python module.


.. _node_migration_configuration:

Pool Configuration
------------------

The behaviour of the migration logic for a pool is controlled by the ``node_migration`` section of the pool configuration.
The allowed values for the migration settings are as follows:

* ``trigger``:

  * ``max_uptime``: if set, monitor nodes' uptime to ensure it stays lower than the provided value; human readable time string (e.g. 30d).
  * ``event``: if set to ``true``, accept async migration trigger for this pool; details about event triggers are described below in :ref:`node_migration_trigger`.

* ``strategy``:

  * ``rate``: rate at which nodes are selected for termination; percentage or absolute value (required).
  * ``prescaling``: if set, pool size is increased by this amount before performing node recycling; percentage or absolute value (0 by default).
  * ``precedence``: precedence with which nodes are selected for termination:
    * ``highest_uptime``: select older nodes first (default);
    * ``lowest_task_count``: select node with fewer running tasks first;
    * ``az_name_alphabetical``: group nodes by availability zone, and select group in alphabetical order;
  * ``bootstrap_wait``: indicative time necessary for a node to be ready to run workloads after boot; human readable time string (3 minutes by default).
  * ``bootstrap_timeout``: maximum wait for nodes to be ready after boot; human readable time string (10 minutes by default).
  * ``allowed_failed_drains``: allow for up to this many nodes to fail draining and be requeued before aborting (3 by default)

* ``disable_autoscaling``: turn off autoscaler while recycling instances (false by default).

* ``ignore_pod_health``: avoid loading and checking pod information to determine pool health (false by default).

* ``health_check_interval``: how much to wait between checks when monitoring pool health (2 minutes by default).

* ``orphan_capacity_tollerance``: acceptable ratio of orphan capacity over target capacity to still consider the pool healthy (float, 0 by default).

* ``expected_duration``: estimated duration for migration of the whole pool; human readable time string (1 day by default).

See :ref:`pool_configuration` for how an example configuration block would look like.


.. _node_migration_trigger:

Migration Event Trigger
-----------------------

Migration trigger events are submitted as Kubernetes custom resources of type ``nodemigration``.
They can be easily generated and submitted by using the ``clusterman migrate`` CLI sub-command and it related options.
In case jobs for a pool need to be stopped, it is possible to use the ``clusterman migrate-stop`` utility.
The manifest for the custom resource defintion is as follows:


.. code-block:: yaml

    ---
    apiVersion: apiextensions.k8s.io/v1
    kind: CustomResourceDefinition
    metadata:
      name: nodemigrations.clusterman.yelp.com
    spec:
      scope: Cluster
      group: clusterman.yelp.com
      names:
        plural: nodemigrations
        singular: nodemigration
        kind: NodeMigration
      versions:
        - name: v1
          served: true
          storage: true
          schema:
            openAPIV3Schema:
              type: object
              required:
                - spec
              properties:
                spec:
                  type: object
                  required:
                    - cluster
                    - pool
                    - condition
                  properties:
                    cluster:
                      type: string
                    pool:
                      type: string
                    label_selectors:
                      type: array
                      items:
                        type: string
                    condition:
                      type: object
                      properties:
                        trait:
                          type: string
                          enum: [kernel, lsbrelease, instance_type, uptime]
                        target:
                          type: string
                        operator:
                          type: string
                          enum: [gt, ge, eq, ne, lt, le, in, notin]


In more readable terms, an example resource manifest would look like:

.. code-block:: yaml

    ---
    apiVersion: "clusterman.yelp.com/v1"
    kind: NodeMigration
    metadata:
      name: my-test-migration-220912
      labels:
        clusterman.yelp.com/migration_status: pending
    spec:
      cluster: kubestage
      pool: default
      condition:
        trait: uptime
        operator: lt
        target: 90d


The fields in each migration event allow to control which nodes are affected by the event
and what is the desired final condition for them. More specifically:

* ``cluster``: name of the cluster to be targeted.
* ``pool``: name of the pool to be targeted.
* ``label_selectors``: list of additional Kubernetes label selectors to filter affected nodes.
* ``condition``: the desired final state for the node, i.e. all nodes must be have kernel version higher than X.

  * ``trait``: metadata to be compared; currently supports ``kernel``, ``lsbrelease``, ``instance_type``, or ``uptime``.
  * ``operator``: comparison operator; supports ``gt``, ``ge``, ``eq``, ``ne``, ``lt``, ``le``, ``in``, ``notin``.
  * ``target``: right side of the comparison expression, e.g. a kernel version or an instance type;
    may be a single string or a comma separated list when using ``in`` / ``notin`` operators.
