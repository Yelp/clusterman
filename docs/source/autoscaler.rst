Autoscaler Batch
================
The autoscaler controls the autoscaling function of Clusterman. It runs for each cluster and pool managed by Clusterman.
Within each cluster, it evaluates the signals for each configured application. The difference between the signalled
resources and the current number of resources available for the pool determines how the cluster will be scaled.

.. note:: Currently, Clusterman can only handle a single application per cluster.

.. _scaling_logic:

Scaling Logic
-------------
Clusterman tries to maintain a certain level of resource utilization, called the setpoint.
It uses the value of signals as the measure of utilization. If current utilization is more than the setpoint margin
away from the setpoint, then it will add or remove enough resources so that utilization will become the setpoint.
(The setpoint margin prevents it from scaling too frequently in response to small changes.)

The setpoint and margin are configured under ``autoscaling`` in :ref:`service_configuration`.
There are also some absolute limits on scaling, e.g. the maximum units that can be added or removed at a time.
These are configured under ``scaling_limits`` in :ref:`pool_configuration`.

For example, suppose the setpoint is 0.8 and the setpoint margin is 0.1. If the total number of CPUs is 100, and
the signalled number of CPUs is 96, the current level of utilization is 0.96, beyond the :math:`0.7-0.9` range
allowed by the setpoint.  Then, Clusterman will add 20 CPUs, because :math:`96/(100+20) = 0.8`.

.. _draining_logic:

Draining and Termination Logic
------------------------------
Clusterman uses a set of complex heuristics to identify hosts to terminate when scaling the cluster down.  In
particular, it looks to see if hosts have joined the Mesos cluster, if they are running any tasks, and if any of the
running tasks are "critical" workloads that should not be terminated.  It combines this information with information
about the resource groups in the pool, and it will work to prioritize agents to terminate based on this information,
while attempting to keep capacity balanced across each of the resource groups.  See the :py:class:`.MesosPoolManager`
class for more information.

Moreover, if the ``draining: true`` flag is set in the pool's configuration file, Clusterman will attempt to drain tasks
off the host before terminating.  This means that it will attempt to gracefully remove running tasks from the agent and
re-launch them elsewhere before terminating the host.  This system is controlled by submitting hostnames to an Amazon
SQS queue; a worker watches this queue and, for each hostname, places the host in `maintenance mode
<https://mesos.apache.org/documentation/latest/maintenance/>`_ (which prevents new tasks from being scheduled on the
host, and then removes any running tasks on the host.  Finally, this host is submitted to a termination SQS queue, where
another worker handles the final shutdown of the host.
