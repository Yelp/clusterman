Metrics batches
===============

Cluster Metrics Collector
-------------------------
.. automodule:: clusterman.batch.cluster_metrics_collector

This batch runs for each cluster managed by Clusterman, collecting metrics.
All system metrics and metadata from :ref:`metric_name_reference` except spot prices are collected by this batch.

See ``batches.cluster_metrics`` under :ref:`service_configuration` for configuration values.

.. program-output:: python -m clusterman.batch.cluster_metrics_collector --help
   :cwd: ../../

Spot Price Collector
---------------------
.. automodule:: clusterman.batch.spot_price_collector

This batch runs in each AWS region in each AWS account that Clusterman manages.
It records spot prices for every instance type and availability zone (AZ) available when the price changes.

It runs in each AWS account because AZs may be named differently in each account.

See ``batches.spot_prices`` under :ref:`service_configuration` for configuration values.

.. program-output:: python -m clusterman.batch.spot_price_collector --help
   :cwd: ../../
