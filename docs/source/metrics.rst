Metrics
=======

Clusterman uses a metrics interface layer to ensure that all metric values are stored in a consistent format that can be
used both for autoscaling and simulation workloads.  At present, all metric data is stored in DynamoDB, but the
interface layer allows us to transparently change backends in the future if necessary.  Metrics in Clusterman can be
classified into one of three different types, described below.

Metric Types
------------
.. autodata:: clusterman_metrics.util.constants.SYSTEM_METRICS
   :annotation:
.. autodata:: clusterman_metrics.util.constants.APP_METRICS
   :annotation:
.. autodata:: clusterman_metrics.util.constants.METADATA
   :annotation:

Application metrics are designed to be read and written by the application owners to provide input into their
autoscaling signals.  System metrics and metadata can be read by application owners, but are written by batch jobs
inside the Clusterman code base.


To be usable by Clusterman, a metrics client needs to implement two functions at a minimum, ``get_writer`` and
``get_metric_values``.  These functions are described in further detail below.

Writing Metrics
---------------

.. py:class:: clusterman_metrics.ClustermanMetricsBotoClient

   .. automethod:: get_writer

Reading Metrics
---------------

.. py:class:: clusterman_metrics.ClustermanMetricsBotoClient

   .. automethod:: get_metric_values
