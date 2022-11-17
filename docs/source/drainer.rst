Drainer
==============

*Drainer* is the component to drain pods off the node before terminating.
It may drain and terminate nodes for three reasons:

* ``spot_interruption``
* ``node_migration``
* ``scaling_down``

**NOTE**: all settings are only compatible with Kubernetes clusters.


Drainer Batch
--------------------

The *Drainer batch* is the entrypoint of the draining logic.

The batch code can be invoked from the ``clusterman.batch.drainer`` Python module.


.. _drainer_configuration:

Pool Configuration
------------------

The behaviour of the drainer logic for a pool is controlled by the ``draining`` section of the pool configuration.
The allowed values for the drainer settings are as follows:

* ``draining_time_threshold_seconds``: maximum time to complete draining process (1800 by default)
* ``redraining_delay_seconds``: how much to wait between draining tries in case of draining failure (15 by default).
* ``force_terminate``: forcibly terminate the node after reaching `draining_time_threshold_seconds` (false by default).


See :ref:`pool_configuration` for how an example configuration block would look like.
