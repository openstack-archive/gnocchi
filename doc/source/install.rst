==============
 Installation
==============

To install Gnocchi, run the standard Python installation procedure:

::

    python setup.py install


Configuration
=============

Configure Gnocchi by editing `/etc/gnocchi/gnocchi.conf`.

No config file is provided with the source code, but one can be easily
created by running:

::

    tox -e genconfig

This command will create an `etc/gnocchi/gnocchi.conf` file which can be used
as a base for the default configuration file at `/etc/gnocchi/gnocchi.conf`.

The configuration file should be pretty explicit, but here are some of the base
options you want to change and configure:


+---------------------+---------------------------------------------------+
| Option name         | Help                                              |
+=====================+===================================================+
| storage.driver      | The storage driver for metrics.                   |
+---------------------+---------------------------------------------------+
| indexer.url         | URL to your indexer.                              |
+---------------------+---------------------------------------------------+
| storage.file_*      | Configuration options to store files              |
|                     | if you use the file storage driver.               |
+---------------------+---------------------------------------------------+
| storage.swift_*     | Configuration options to access Swift             |
|                     | if you use the Swift storage driver.              |
+---------------------+---------------------------------------------------+
| storage.ceph_*      | Configuration options to access Ceph              |
|                     | if you use the Ceph storage driver.               |
+---------------------+---------------------------------------------------+


Gnocchi provides these storage drivers:

- File (default)
- `Swift`_
- `Ceph`_
- `InfluxDB`_ (experimental)

Gnocchi provides these indexer drivers:

- `PostgreSQL`_ (recommended)
- `MySQL`_

.. _`Swift`: https://launchpad.net/swift
.. _`Ceph`: http://ceph.com/
.. _`PostgreSQL`: http://postgresql.org
.. _`MySQL`: http://mysql.com
.. _`InfluxDB`: http://influxdb.com

Indexer Initialization
======================

Once you have configured Gnocchi properly, you need to initialize the indexer:

::

    gnocchi-dbsync


Running Gnocchi
===============

To run Gnocchi, simply run the HTTP server:

::

    gnocchi-api

You then need to run the `gnocchi-metricd` daemon to enable new measures
processing and metrics expunge in the background.

Running As A WSGI Application
=============================

It's possible – and strongly advised – to run Gnocchi through a WSGI
service such as `mod_wsgi`_ or any other WSGI application. The file
`gnocchi/rest/app.wsgi` provided with Gnocchi allows you to enable Gnocchi as
a WSGI application.
For other WSGI setup you can refer to the `pecan deployement`_ documentation.

.. _`mod_wsgi`: https://modwsgi.readthedocs.org/en/master/
.. _`pecan deployement`: http://pecan.readthedocs.org/en/latest/deployment.html#deployment


Drivers notes
=============

Carbonara based drivers (file, swift, ceph)
-------------------------------------------

To ensure consistency accross all gnocchi-api and gnocchi-metricd workers,
these drivers need a distributed locking mechanism. This is provided by the
'coordinator' of the `tooz`_ library.

By default, the configured backend for `tooz`_ is 'file', this allows locking
across workers on the same node.

In a multi-nodes deployement, the coordinator needs to be changed via
the storage/coordination_url configuration options to one of the other
`tooz backends`_.

For example::

    coordination_url = redis://<sentinel host>?sentinel=<master name>
    coordination_url = zookeeper:///hosts=<zookeeper_host1>&hosts=<zookeeper_host2>

.. _`tooz`: http://docs.openstack.org/developer/tooz/
.. _`tooz backends`: http://docs.openstack.org/developer/tooz/drivers.html
