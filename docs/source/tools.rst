Additional Tools
================


generate-data
-------------

The ``clusterman generate-data`` command is a helper function for the clusterman simulator to generate "fake" data,
either as some function of pre-existing metric data or as drawn from a specified random distribution.  The command takes
as input an experimental design YAML file, and produces as output a compressed JSON file that can be directly used in a
simulation.


Experimental Design File Specification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An experimental design file contains details for how to generate experimental metric data for use in a simulation.  The
specification for the experimental design is as follows::

    metric_name:
        start_time: <date-time string>
        end_time: <date-time string>
        frequency: <frequency specification>
        values: <values specification>

* The ``metric_name`` is arbitrary; it should correspond to a metric value that ``clusterman simulate`` will use when
  performing its simulation.  Multiple metrics can be specified for a given experimental design by repeating the above
  block in the YAML file for each desired metric; note that if multiple metrics should follow the same data generation
  specification, `YAML anchors and references <https://en.wikipedia.org/wiki/YAML#Advanced_components>`__ can be used.

* The ``<date-time string>`` fields can be in a wide variety of different formats, both relative and exact.  In most cases
  dates and times should be specifed in `ISO-8601 format <https://en.wikipedia.org/wiki/ISO_8601>`__; for example,
  ``2017-08-03T18:08:44+00:00``.  However, in some cases it may be useful to specify relative times; these can be in
  human-readable format, for example ``one month ago`` or ``-12h``.

* The ``<frequency specification>`` can take one of two formats:

  - Regular intervals: by providing an ``<date-time string>`` for the frequency specification, metric values will be
    generated periodically; for example, a frequency of ``1m`` will generate a new data point every minute.
  - Random intervals: to generate new metric event arrival times randomly, specify a ``<random generator>`` block for
    the frequency, as shown below::

        distribution: dist-function
        params:
            dist-param-a: param-value
            dist-param-b: param-value

    The ``dist-function`` should be the name of a function in the `Python random module
    <https://docs.python.org/3/library/random.html#>`__.  The ``params`` are the keyword arguments for the chosen
    function.  All parameter values relating to time should be defined in seconds; for example, if ``gauss`` is chosen
    for the distribution function, the units for the mean and standard deviation should be seconds.

.. note:: A common choice for the dist-function is expovariate, which creates an exponentially-distributed interarrival
   time, a.k.a, a `Poisson process <https://en.wikipedia.org/wiki/Poisson_point_process>`__.  This is a good baseline
   model for the arrival times of real-world data.

* Similarly, the ``<values specification>`` can take one of two formats:

  - Random values: for this mode, specify a ``<random generator>`` block as shown above for frequency.
  - Function of existing data: specify a string (function of ``x``) and a metric name to generate data as some function
    of pre-existing data

.. todo:: Currently you can only create a constant function, i.e., the metric value is always constant


Output Format
~~~~~~~~~~~~~

The ``generate-data`` command produces a compressed JSON containing the generated metric data.  The format for this file
is identical to the input for the :doc:`simulator <simulator>`, and is as follows::

    {
        'metric_name_1': {
            '__data__': [[<date-time-string>, value], [<date-time-string>, value], ...]
        },
        'metric_name_2': {
            '__data__': [[<date-time-string>, value], [<date-time-string>, value], ...]
        },
        ...
    }


Sample Usage
~~~~~~~~~~~~

::

    drmorr ~ > clusterman generate-data --input design.yaml --ouput metrics.json.gz
    Random Seed: 12345678

    drmorr ~ > clusterman simulate --metrics-data-file metrics.json.gz \
    > --start-time "2017-08-01T08:00:00+00:00" --end-time "2017-08-01T08:10:00+00:00"

    === Event 0 -- 2017-08-01T08:00:00+00:00        [Simulation begins]
    === Event 2 -- 2017-08-01T08:00:00+00:00        [SpotPriceChangeEvent]
    === Event 28 -- 2017-08-01T08:00:00+00:00       [SpotPriceChangeEvent]
    === Event 21 -- 2017-08-01T08:00:00+00:00       [SpotPriceChangeEvent]
    === Event 22 -- 2017-08-01T08:02:50+00:00       [SpotPriceChangeEvent]
    === Event 3 -- 2017-08-01T08:05:14+00:00        [SpotPriceChangeEvent]
    === Event 23 -- 2017-08-01T08:06:04+00:00       [SpotPriceChangeEvent]
    === Event 0 -- 2017-08-01T08:00:00+00:00        [Simulation ends]


Sample Experimental Design File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/design.yaml
    :language: yaml

The above design file, and a sample output file are located in ``docs/examples/design.yaml`` and
``docs/examples/metrics.json.gz``, respectively.
