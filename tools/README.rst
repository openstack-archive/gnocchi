=============
Gnocchi tools
=============

This page describes several tools build-in the Gnocchi (Time-Series Storage)
project.

Run Gnocchi basic performance tests
===================================

This part of document describes how to run basic performance tests for the
Gnochhi API - for entities and measures.

To run these tests you will need to have lab with both only Gnocchi
installed and configured (please use instructions from the parent README.rst
file).

To run this performance benchmarking use the following command:

    python gnocchi_base_perf_tests.py [...args]

This python script has the arguments structure as below:

    gnocchi_base_perf_tests.py [-h] [--entity-number ENTITY_NUMBER]
                                    [--measure-batches MEASURE_BATCHES_NUMBER]
                                    [--batch_size BATCH_SIZE]
                                    [--gnocchi-url GNOCCHI_URL]
                                    [--archive-policy ARCHIVE_POLICY]
                                    [--os-username USERNAME]
                                    [--os-tenant-name TENANT_NAME]
                                    [--os-password PASSWORD]
                                    [--os-auth-url AUTH_URL]
                                    [--result-dir DIR]
                                    [--need-authenticate NEED_AUTH]

These arguments are having the following meaning:

    -h, --help             show this help message and exit
    --entity-number ENTITY_NUMBER
                           Number of entities to be created. Entities are
                           created one by one. Default number of entities is
                           *100*.
    --measure-batches MEASURE_BATCHES_NUMBER
                           Number of measures batches to be sent. Default
                           number of batches is *100*.
    --batch_size BATCH_SIZE
                           Number of measurements in the batch (*100* by
                           default).
    --gnocchi-url GNOCCHI_URL
                           Gnocchi API URL to use (*http://localhost:8041* by
                           default).
    --archive-policy ARCHIVE_POLICY
                           Archive policy to use (default value is *low*).
    --os-username USERNAME User name to use for OpenStack service access
                           (*admin* by default).
    --os-tenant-name TENANT_NAME
                           Tenant name to use for OpenStack service access
                           (*admin* by default).
    --os-password PASSWORD Password to use for OpenStack service access
                           (*nova* by default).
    --os-auth-url AUTH_URL Auth URL to use for OpenStack service access
                           (*http://localhost:5000/v2.0* by default)
    --result-dir DIR       Directory to write results to (*/tmp/* by default).
    --need-authenticate NEED_AUTH
                           Boolean option that defines if we need to
                           authenticate. We need to authenticate by default.


All arguments are optional, so tests running on usual Devstack all-in-one
installation won't need any arguments to be passed.

