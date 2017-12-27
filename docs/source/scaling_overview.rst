Overview
=========

Clusterman scales the roles in each Mesos cluster independently. In other words, it
treats each group of instances with the same reserved role in a Mesos cluster as a single unit.
For each role in a cluster, Clusterman determines the total target capacity by evaluating signals.
These signals are user-defined functions of metrics collected through Clusterman.

MesosRoleManager
----------------
Clusterman manages a group of instances with the same reserved role through a :py:class:`MesosRoleManager <clusterman.mesos.mesos_role_manager.MesosRoleManager>`.
A ``MesosRoleManager`` consists of one or more :py:class:`MesosRoleResourceGroup <clusterman.mesos.mesos_role_resource_group.MesosRoleResourceGroup>` units, which
represent groups of instances that can be modified together, such as an AWS spot fleet request.

Signals
-------
For each role manager, clusterman determines the target capacity by evaluating signals. Signals represent
the estimated resources (e.g. CPUs, memory) required by that role. Clusterman compares this estimate to the current
number of resources available and changes the target capacity for the role accordingly.

These signals are functions of metrics and may be defined per role, by application owners (see :ref:`adding_signal`).
If there is no custom signal defined for a role, there is also a default signal defined by Clusterman.

Metrics
-------
Signals are functions of metrics, values collected by Clusterman over time.
Clusterman uses a metrics interface layer to ensure that all metric values are stored in a consistent format that can be
used both for autoscaling and simulation workloads.  At present, all metric data is stored in DynamoDB.

Application owners may use the metrics library to record application-specific metrics. The clusterman service also
collects a number of metrics that may be used by anyone for autoscaling signals or simulation.
