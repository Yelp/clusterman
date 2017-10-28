Cluster Management
==================

asdf
Clusterman comes with a number of command-line tools to help with cluster management.


Manage
------

The ``clusterman manage`` command can be used to directly change the state of the cluster:

.. command-output:: clusterman manage --help

The ``--target-capacity`` option allows users to directly change the size of the Mesos cluster specified by the
``--cluster`` and ``--role`` arguments.  A few caveats exist that users should be aware of.

.. py:currentmodule:: clusterman.mesos.mesos_role_manager

Firstly, the value of the ``--target-capacity`` option is passed directly to the :ref:`MesosRoleManager` that has been
:ref:`configured <Configuration>` for the Mesos cluster.  Thus all the caveats of use for the :ref:`MesosRoleManager`
object apply to this command; in particular, note that the cluster's :attr:`target capacity
<MesosRoleManager.target_capacity>` is used for scaling up but the :attr:`fulfilled capacity
<MesosRoleManager.fulfilled_capacity>` is used for scaling down.

Furthermore, note that there can be up to a few minutes of "lag time" between when the manage command is issued and when
changes are reflected in the cluster.  This is due to potential delays introduced into the pipeline while AWS finds and
procures new instances for the cluster.  Therefore, it is not recommended to run ``clusterman manage`` repeatedly in
short succession, or immediately after the autoscaler batch has run.

.. note:: Future versions of Clusterman may include a rate-limiter for the manage command

.. todo:: The ``--recycle`` command-line argument is not currently implemented

Status
------

The ``clusterman status`` command provides information on the current state of the cluster:

.. command-output:: clusterman status --help

As noted above, the state of the cluster may take a few minutes to equilibrate after a ``clusterman manage`` command or
the autoscaler has run, so the output from ``clusterman status`` may not accurately reflect the desired status.
