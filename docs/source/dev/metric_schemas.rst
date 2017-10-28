Metric Schemas
==============

Metric Keys
-----------

Metric keys have two components, a metric name and a set of dimensions.  The metric key format is::

    metric_name|dimension1=value1,dimension2=value2

This allows for metrics to be easily converted into SignalFX datapoints, where the metric name is used as the timeseries
name, and the dimensions are converted to SignalFX dimensions.  The following helper function will return the full
metric key in its proper format:

.. automodule:: clusterman_metrics.util.meteorite
   :members: generate_key_with_dimensions

List of Metric Names
--------------------

We are currently using the following list of metric names and dimensions

* ``cpu_allocation|cluster=<cluster name>,role=<Mesos role>``
* ``spot_prices|AZ=<availability zone>,instance_type=<AWS instance type>``
* ``capacity|cluster=<cluster name>,role=<Mesos role>``

DynamoDB
--------

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
------------------------------------------------------------------
metric name   timestamp  value
================================================= ========== =====
cpu_allocation|cluster=norcal-prod,role=appA_role 1502405756    22
mem_allocation|cluster=norcal-prod,role=appB_role 1502405810    20
================================================= ========== =====

+-------------------------------------------------------------------------------------------------+
| Metadata                                                                                        |
+===================================================+============+================================+
| metric name                                       | timestamp  | value                          |
+---------------------------------------------------+------------+--------------------------------+
| spot_prices|AZ=us-west-2a,instance_type=c3.xlarge | 1502405756 | 1.30                           |
+---------------------------------------------------+------------+--------------------------------+
| spot_prices|AZ=us-west-2c,instance_type=c3.xlarge | 1502405756 | 5.27                           |
+---------------------------------------------------+------------+--------------------------------+
| capacity|cluster=norcal-prod,role=seagull         | 1502409314 | | {"c3.xlarge,us-west-2a": 4,  |
|                                                   |            | | "c3.xlarge,us-west-2c": 20}  |
+---------------------------------------------------+------------+--------------------------------+
