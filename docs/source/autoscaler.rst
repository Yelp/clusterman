Autoscaler
==========
.. automodule:: clusterman.batch.autoscaler

This batch controls the autoscaling function of Clusterman. It runs for each cluster managed by Clusterman.
Within each cluster, it evaluates the signals for each configured role. The difference between the signalled resources
and the current number of resources available for the role determines how the cluster will be scaled.

.. note:: Currently, Clusterman can only handle a single role per cluster.

Scaling logic
-------------
Clusterman tries to maintain a certain level of resource utilization, called the setpoint.
It uses the value of signals as the measure of utilization. If current utilization is more than the setpoint margin
away from the setpoint, then it will add or remove enough resources so that utilization will become the setpoint.
(The setpoint margin prevents it from scaling too frequently in response to small changes.)

The setpoint and margin are configured under ``autoscaling`` in :ref:`service_configuration`.
There are also some absolute limits on scaling, e.g. the maximum units that can be added or removed at a time.
These are configured under ``scaling_limits`` in :ref:`role_configuration`.

.. note:: The only resource considered now is CPUs.

For example, suppose the setpoint is 0.8 and the setpoint margin is 0.1. If the total number of CPUs is 100, and
the signalled number of CPUs is 96, the current level of utilization is 0.96, beyond the :math:`0.7-0.9` range
allowed by the setpoint.
Then, Clusterman will add 20 CPUs, because :math:`96/(100+20) = 0.8`.

.. program-output:: python -m clusterman.batch.autoscaler --help
   :cwd: ../../
