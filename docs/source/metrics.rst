Metrics
=======

Metrics are used by Clusterman to record state about clusters that can be used later for autoscaling or
simulation.

Clusterman uses a metrics interface layer to ensure that all metric values are stored in a consistent format that can be
used both for autoscaling and simulation workloads.  At present, all metric data is stored in DynamoDB, and accessed
using the :py:class:`ClustermanMetricsBotoClient <clusterman_metrics.ClustermanMetricsBotoClient>`. In the future, the interface layer allows us to transparently change backends if necessary.

This section describes the format in which metrics are stored, and how to access the metrics.

Metric Schemas
--------------
Metrics in Clusterman can be classified into one of three different types. Each metric type is stored in a
separate namespace. Within each namespace, metric values are uniquely identified by their key and timestamp.

.. _metric_types:

Metric Types
~~~~~~~~~~~~
.. py:module:: clusterman_metrics.util.constants

.. autodata:: clusterman_metrics.util.constants.SYSTEM_METRICS
   :annotation:
.. autodata:: clusterman_metrics.util.constants.APP_METRICS
   :annotation:
.. autodata:: clusterman_metrics.util.constants.METADATA
   :annotation:

Application metrics are designed to be read and written by the application owners to provide input into their
autoscaling signals.  System metrics and metadata can be read by application owners, but are written by batch jobs
inside the Clusterman code base.

Metric Keys
~~~~~~~~~~~
Metric keys have two components, a metric name and a set of dimensions.  The metric key format is::

    metric_name|dimension1=value1,dimension2=value2

This allows for metrics to be easily converted into SignalFX datapoints, where the metric name is used as the timeseries
name, and the dimensions are converted to SignalFX dimensions.  The following helper, ``generate_key_with_dimensions``,
will return the full metric key in its proper format. Use it to get the correct key when reading or writing metrics.

.. automodule:: clusterman_metrics.util.meteorite
   :members: generate_key_with_dimensions


Accessing Metrics
------------------
Metric values should be read and written through a metrics client. To be usable by Clusterman, a metrics client needs to
implement two functions at a minimum, ``get_writer`` and ``get_metric_values``. For now, there is one metrics client implemented,
the ``ClustermanMetricsBotoClient``.

.. note:: Application owners may use the metrics client to write application metrics, for input into their autoscaling signals.
   In general, they should not need to read metrics through the metrics client, because the ``BaseSignal`` takes care of
   reading metrics for the signal.

.. py:module:: clusterman_metrics
.. autoclass:: clusterman_metrics.ClustermanMetricsBotoClient
   :members: __init__, get_writer, get_metric_values

DynamoDB example
~~~~~~~~~~~~~~~~

The following tables show examples of how our data is stored in DynamoDB:

============= ========== =====
Application Metrics
------------------------------
metric name   timestamp  value
============= ========== =====
app_A,my_runs 1502405756     2
app_B,my_runs 1502405810   201
app_B,metric2 1502405811   1.3
============= ========== =====

================================================= ========== =====
System Metrics
------------------------------------------------- ---------- -----
metric name                                       timestamp  value
================================================= ========== =====
cpus_allocated|cluster=norcal-prod,role=appA_role 1502405756    22
mem_allocated|cluster=norcal-prod,role=appB_role  1502405810    20
================================================= ========== =====

+---------------------------------------------------------------------------------------------------+-------------------------+-------------------------+
| Metadata                                                                                          |                         |                         |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+
| metric name                                         | timestamp  | value                          | <c3.xlarge, us-west-2a> | <c3.xlarge, us-west-2c> |
+=====================================================+============+================================+=========================+=========================+
| spot_prices|aws_availability_zone=us-west-2a,aws_instance_type=c3.xlarge   | 1502405756 | 1.30                           |                         |                         |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+
| spot_prices|aws_availability_zone=us-west-2c,aws_instance_type=c3.xlarge   | 1502405756 | 5.27                           |                         |                         |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+
| fulfilled_capacity|cluster=norcal-prod,role=seagull | 1502409314 |                                |                       4 |                      20 |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+

.. _metric_name_reference:

Metric Name Reference
---------------------
The following is a list of metric names and dimensions that Clusterman collects:

System
~~~~~~
* ``cpus_allocated|cluster=<cluster name>,role=<Mesos role>``

Metadata
~~~~~~~~
* ``cpus_total|cluster=<cluster name>,role=<Mesos role>``
* ``fulfilled_capacity|cluster=<cluster name>,role=<Mesos role>`` (separate column per InstanceMarket)
* ``spot_prices|aws_availability_zone=<availability zone>,aws_instance_type=<AWS instance type>``
* ``target_capacity|cluster=<cluster name>,role=<Mesos role>``
