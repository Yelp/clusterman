Signals
========

For each :py:class:`MesosRoleManager <clusterman.mesos.mesos_role_manager.MesosRoleManager>`, which manages the capacity
for a role in a Mesos cluster, Clusterman determines the target capacity by evaluating signals.  Signals are a function
of metrics and represent the estimated resources (e.g. CPUs, memory) required by that role.  Clusterman compares this
estimate to the current number of resources available and changes the target capacity for the role accordingly (see
:ref:`scaling_logic`).

Application owners may write and use their own signals.  If there is no custom signal defined for a role, Clusterman
will use its :ref:`default_signal`.

.. _adding_signal:

Custom signals
--------------------
Code for custom signals should be defined in the ``clusterman_signals`` package. Once a signal is defined there, the
:ref:`signal_configuration` section below describes how Clusterman can be configured to use it for a role.

Signal code
~~~~~~~~~~~
In ``clusterman_signals``, there is a separate subpackage for each role. If there is not a subpackage for your role
already, create a directory within ``clusterman_signals`` and create an ``__init__.py`` file within that directory. Make
sure the name of the directory matches your role.

Within that directory, application owners may choose how to organize signal classes within files.  The only requirement
is that the signal class must be able to be imported directly from that subpackage, i.e. ``from clusterman_signals.roleA
import MyCustomSignal``. Typically, in the ``__init__.py``, you would import the class and then add it to ``__all__``::

    from clusterman_signals.roleA.custom_signal import MyCustomSignal
    ...

    __all__ = [MyCustomSignal, ...]

Define a new class that implements :py:class:`clusterman_signals.base_signal.BaseSignal`.  (The class name should be
unique within this role.) In this class, you only need to overwrite the ``value`` method.  ``value`` should use metric
values to return a :py:class:`SignalResources <clusterman_signals.base_signal.SignalResources>` tuple, where the units
of the ``SignalResources`` tuple should match the Mesos units: shares for CPUs, MB for memory and disk.

When you :ref:`configure your custom signal <signal_configuration>`, you specify the metric names that your signal
requires and how far back the data for each metric should be queried. ``BaseSignal`` handles the querying of metrics for
you.  In ``value``, you can assume that each metric timeseries configured is available in the signal via::

    self.metrics_cache['my_metric_name']

where each metric timeseries is a list of ``(unix_timestamp_seconds, value)`` pairs, sorted from oldest to most recent.

.. note:: The autoscaler only responds to the ``cpus`` resource, but that may change in the future.

.. automodule:: clusterman_signals.base_signal
.. autoclass:: clusterman_signals.base_signal.SignalResources
.. autoclass:: clusterman_signals.base_signal.BaseSignal
   :members: value

.. _signal_configuration:

Configuration
~~~~~~~~~~~~~
Application-defined signals are configured via the ``autoscaling_signal`` section of the :ref:`role_configuration`.
Within this section, the following keys are available:

.. code-block:: yaml

    autoscaling_signal:
        name: name of signal class, e.g. CustomSignalClass
        branch_or_tag: a Git branch or tag referring to the version of the signal you want to run
        period_minutes: how often the signal should be evaluated by the autoscaler, e.g. 15
        required_metrics:
            - name: metric key
              type: metric type, e.g. system_metrics
              minute_range: minutes of data for the metric to query
            - ...
        parameters: (optional)
            - paramA: 'typeA'
            - paramB: 10
              ...

.. warning:: The ``branch_or_tag`` key allows you to pin a version of the signal to be used for scaling your
   application.  While this *can* be ``HEAD`` or ``master``, this is not a recommended value, as this may cause
   unintended behavior if the service restarts.

For required metrics, there can be any number of sections, each defining one desired metric.  The metric type must be
one of :ref:`metric_types`.

Custom parameters are optional. If defined, they are passed as a dictionary to the signal, in
``self.custom_parameters``. For example, if you wanted to use the value of ``paramA`` in ``value``::

    def value(self):
        my_param = self.custom_parameters['paramA']
        ...

Use the regular srv-configs workflow to deploy changes to these values.

.. warning:: Any changes to this section will cause the signal to be reloaded by the autoscaling batch.  Test your
   config values before pushing.  If the config values break the custom signal, then the role will start using the
   default signal.

Example
~~~~~~~
A custom signal class that averages ``cpus_allocated`` values::

    from clusterman_signals.base_signal import BaseSignal
    from clusterman_signals.base_signal import SignalResources

    class AverageCPUAllocation(BaseSignal):

        def value(self):
           cpu_values = [val for timestamp, val in self.metrics_cache['cpus_allocated']
           average = sum(cpu_values) / len(cpu_values)
           return SignalResources(cpus=average)

And configuration for a role, so that the autoscaler will evaluate that signal every 10 minutes, over data from the last
20 minutes:

.. code-block:: yaml

    autoscaling_signal:
        name: AverageCPUAllocation
        branch_or_tag: v1.0.0
        period_minutes: 10
        required_metrics:
            - name: cpus_allocated
              type: system_metrics
              minute_range: 20

Deploying changes
~~~~~~~~~~~~~~~~~

Testing
"""""""
These are the steps to test signal changes against the service autoscaler.

#. Push your ``clusterman_signals`` branch to origin:

   .. code-block:: text

       git push origin <my-dev-branch>

#. Run a simulation, and reference your newly-pushed branch:

   .. code-block:: text

       clusterman simulate --start-time <start> --end-time <end> --cluster <cluster> \
                --role <role> -S <my-dev-branch>  --reports cost
       mv cost.png test-cost.png

   .. note:: If the input parameters or metrics for your signal have changed, you will instead need to make a copy of the
      signal's configuration file, make the necessary changes, and pass that in to the simulator:

      .. code-block:: text

            cp -r /nail/srv/configs/clusterman-roles/ .
            <editor> ./clusterman-roles/<role>/config.yaml
            clusterman simulate --role-config-dir ./clusterman-roles <remaining arguments> ...

#. Then run a simulation with the same arguments against your existing signal version:

   .. code-block:: text

       clusterman simulate --start-time <start> --end-time <end> --cluster <cluster> \
                --role <role> --reports cost

   Compare the new signal configuration results in ``test-cost.png`` to the existing results in ``cost.png``.


Pushing
"""""""
Once you've tested your changes to ``clusterman_signals`` and you're satisfied with the results, follow these steps
to have the changes take effect in the production autoscaler.

#. Merge your changes with master:

   .. code-block:: text

       git checkout master
       git pull origin master
       git merge --no-ff <my-dev-branch>

#. Update the version and push to master:

   .. code-block:: text

       make version-bump
       git push origin HEAD --tags

   .. note:: The ``version-bump`` make target will automatically prompt you to update the version of the
      ``clusterman_signals`` repo, and will tag your commit with that version (e.g., 'v1.0.3').  However, you may find
      it useful to also tag specific versions of your signal with a more meaningful/human-readable tag to use in the
      ``branch_or_tag`` field.

   .. warning:: It is *possible* for you to reference a branch of ``clusterman_metrics`` that hasn't yet been merged to
      master for the production version of your signal.  However, this is not a recommended method of deployment, as
      this may introduce significant divergence from the master branch, including new features that are added to the
      ``BaseSignal`` class.

#. Update srv-configs to use the new version by updating the ``branch_or_tag`` key for your signal to the new version.
   Push your srv-configs changes; this will automatically restart the clusterman service with your new signal.

.. _default_signal:

Default signal
--------------
If a role does not define its own ``autoscale_signal``, or if Clusterman is unable to load the role-defined signal for
any reason, Clusterman will fall back to using a default signal, defined in Clusterman's own service configuration file.

See the configuration file and the ``clusterman`` package within ``clusterman_signals`` package for the latest
definitions.  In general, the default signal uses recent values of ``cpus_allocated`` to estimate the amount of
resources required, and does not consider any other metrics.  ``cpus_allocated`` is the number of CPUs that Mesos has
allocated to tasks, from agents in the cluster with the specified role.

Under the hood
--------------

In order to ensure that the autoscaler can work with multiple clients that specify different versions of the
``clusterman_signals`` repo, we do not import ``clusterman_signals`` into the autoscaler.  Instead, the autoscaler (and
the simulator) will clone the commit referenced by ``branch_or_tag`` into a local directory, build the virtualenv for
the signal, and then open a Unix abstract namespace socket to communicate with the signal.  All communication between
the autoscaler and the signal is done in JSON.

To initialize the signal, ``run.py`` is called in the ``clusterman_signals`` repo; this script takes three command-line
arguments: the name of the socket to connect to, the role of the signal to load, and the name of the signal to load.
The script then connects to the specified Unix socket and waits for the autoscaler to initialize the signal.  The JSON
object for signal initialization looks like the following:

.. code-block:: json

    {
        "cluster": what cluster this signal is operating on,
        "role": what role this signal is operating on for the specified cluster,
        "parameters": the values for any parameters from srv-configs that the signal should reference
    }

Once the signal is properly initialized, the ``run.py`` script waits for input from the autoscaler indefinitely.  This
input takes the form of the following JSON blob:

.. code-block:: json

    {
        "metrics": {
            "metric-name-1": [[timestamp, value1], [timestamp, value2], ...],
            "metric-name-2": [[timestamp, value1], [timestamp, value2], ...],
            ...
        }
    }

In other words, the autoscaler passes in all of the ``required_metrics`` values for the signal, which have been
collected over the last ``period_minutes`` window for each metric.  The signal then will give the following response to
the autoscaler:

 .. code-block:: json

    { "Resources": { "cpus": num_requested_cpus, ... } }

The value in this response is the result from running the signal with the specified data.
