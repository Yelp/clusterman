Simulation
==========

Experimental Input Data
-----------------------

The simulator can accept experimental input data for one or more metric timeseries using the ``--metrics-data-file``
argument to ``clusterman simulate``.  The simulator expects this file to be stored as a compressed (gzipped) JSON file;
the JSON schema is as follows::

    {
        'metric_name_1': {
            '__timeseries__': [[<date-time-string>, value], [<date-time-string>, value], ...]
        },
        'metric_name_2': {
            '__timeseries__': [[<date-time-string>, value], [<date-time-string>, value], ...]
        },
        ...
    }
