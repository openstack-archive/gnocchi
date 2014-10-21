=============
Gnocchi tools
=============

This page describes several tools build-in the Gnocchi (Time-Series Storage)
project.

Run Gnocchi dispatcher performance tests
========================================

There is the opportunity to run several performance benchmarks for the
implemented Gnocchi Ceilometer database dispatcher. This aims to approximate
amount of samples that might be processed by Gnocchi + Ceilometer.

To run these tests you will need to have lab with both Ceilometer and Gnocchi
installed and configured (please use instructions from the parent README.rst
file).

To run performance benchmarking use the following command:

    python gnocchi_dispatcher_perf_tests.py [...args]

This python script has the arguments structure as below:

    gnocchi_dispatcher_perf_tests.py [-h] [--interval INTERVAL] [--start START]
                                     [--end END] [--type {gauge,cumulative}]
                                     [--unit UNIT]
                                     [--random_min RANDOM_MIN]
                                     [--random_max RANDOM_MAX]
                                     [--batch_size BATCH_SIZE]
                                     [--resource_count RESOURCE_COUNT]
                                     [--counter COUNTER] [--volume VOLUME]
                                     [--log-file LOG_FILE] [--gnocchi-url URL]
                                     --project-id PROJECT_ID
                                     --user-id USER_ID

These arguments are having the following meaning:

    -h, --help            show this help message and exit
    --interval INTERVAL   The period between batches of samples are sent, in
                          minutes. Default interval is *10* minutes.
    --start START         The number of days to go back in time (to generate
                          realistic timestamps). Default value for this
                          argument is *31*.
    --end END             The number of days to go further in time (to generate
                          realistic timestamps). Default value for this
                          argument is *2*.
    --type {gauge,cumulative}
                          Meter type to be sent. By default this script is
                          sending *gauge* samples.
    --unit UNIT           Meter unit to be sent. By default *instance* unit is
                          used.
    --random_min RANDOM_MIN
                          Minimum value of random to be added as a correction
                          to the given default meter value (volume). By default
                          random_min is *0*.
    --random_max RANDOM_MAX
                          Maximum value of random to be added as a correction
                          to the given default meter value (volume). By default
                          random_max is *0*.
    --batch_size BATCH_SIZE
                          Number of samples in the batch (*100* by default).
    --resource_count RESOURCE_COUNT
                          Number of resources to generate samples for (*20* by
                          default).
    --counter COUNTER     The counter (meter) to use (*instance* by default).
    --volume VOLUME       The default value to be attached to the counter
                          (meter). Default volume value is *1*.
    --log-file LOG_FILE   File to write timing logs to -
                          */tmp/gnocchi_dispatcher.log* by default.
    --gnocchi-url URL     Gnocchi API URL to use (*http://localhost:8041* by
                          default).
    --project-id PROJECT_ID
                          *Project id* on behalf of which samples will be sent.
    --user-id USER_ID     *User id* of on behalf of whom samples will be sent.


User ID and project ID are the positional arguments, all other ones are having
the default values that will work OK in case is you're running all-in-one
OpenStack installation.

