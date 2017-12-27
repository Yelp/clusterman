Roles
==============

In a Mesos cluster, the resources on an agent can be reserved for a specific role.
Clusterman scales the roles in each Mesos cluster independently.
In other words, it treats each group of instances with the same reserved role in a Mesos cluster as a single unit,
and autoscales each such group's target capacity separately.

A ``MesosRoleManager`` abstracts the instances that are part of that role into a single object, which
Clusterman interacts with. Within that role, there may be more than one group of instances from
different sources (e.g. different AWS spot fleets or ASGs). Each group is abstracted by
a ``MesosRoleResourceGroup``.

``MesosRoleResourceGroup`` is an interface that should be implemented for each type of instance group.
Currently, the following groups are implemented:

* ``clusterman.mesos.spot_fleet_resource_group.SpotFleetResourceGroup``: an AWS spot fleet request

MesosRoleManager
----------------

.. automodule:: clusterman.mesos.mesos_role_manager
.. autoclass:: clusterman.mesos.mesos_role_manager.MesosRoleManager
   :members:

MesosRoleResourceGroup
----------------------
.. automodule:: clusterman.mesos.mesos_role_resource_group
.. autoclass:: clusterman.mesos.mesos_role_resource_group.MesosRoleResourceGroup
   :members:
