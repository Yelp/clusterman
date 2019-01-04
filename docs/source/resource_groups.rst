Resource Groups
===============

Resource groups are wrappers around cloud provider APIs to enable scaling up and down groups of machines.  A resource
group implments the :py:class:`.MesosPoolResourceGroup` interface, which provides the set of required methods for
Clusterman to interact with the resource group.  Currently, Clusterman supports the following types of resource groups:

* :py:class:`.SpotFleetResourceGroup`: `AWS spot fleet requests
  <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-fleet-requests.html>`_
* :py:class:`.AutoScalingResourceGroup`: `AWS autoscaling groups
  <https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html>`_
