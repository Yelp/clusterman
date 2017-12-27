.. clusterman documentation master file, created by
   sphinx-quickstart on Thu Aug  3 09:34:59 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

clusterman
======================================

Clusterman autoscales Mesos clusters based on the values of user-defined signals
of resource utilization. It also provides tools to manually manage those clusters,
and simulate how changes to autoscaling logic will impact the cost and performance.


.. toctree::
   :titlesonly:

   scaling_overview

.. toctree::
   :maxdepth: 2
   :caption: Autoscaling

   metrics
   signals
   roles


.. toctree::
   :maxdepth: 2
   :caption: Tools

   configuration
   manage
   simulator
   tools


.. toctree::
   :maxdepth: 2
   :caption: Batches

   batches



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
