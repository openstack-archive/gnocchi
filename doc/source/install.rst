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
'coordinator' of the `tooz_` library.

By default, the configured backend for `tooz_` is 'file', that allows to
distribute locks across workers on the same node.

In case of multi-nodes deployement, the coordinator need to be changed via
the storage/coordination_url configuration options to one of the other
`tooz backends`_.

.. _`tooz`: http://docs.openstack.org/developer/tooz/
.. _`tooz backends`: http://docs.openstack.org/developer/tooz/drivers.html


Ceph driver implementation details
----------------------------------

Each batch of measurements to process are stored into one rados object.
This object are named 'measures_<metric_id>_<random_uuid>_<timestamp>

Also a special empty object called 'measures' have the list of measures to
process stored in its xattr attributes.

Because of the async nature of how we store measurements in gnocchi,
gnocchi-metricd need to known the list of objects that wait to be processed:

- Listing rados objects for this is not a solution that take to much times.
- Using a custom format into a rados object, will enforce us to use a lock
  each time we change it.

Instead, the xattrs of one empty rados object is used. No lock are need to
add/remove a xattr.

But depending of the filesystem used by ceph OSDs, this xattrs can have
limitation in term of numbers and size if ceph if not correclty configured.
See `Ceph extended attributes documentation` for more details.

Next, each carbonara generated files are stored in 'one' rados object.
So each metric have one rados object per aggregation in the archive policy.

Because of this, the OSDs filling can looks less balanced comparing of the RBD.
Some object will be big and so other small depending on how archive policy are
setuped.

We can imagine an unrealisting case like 1 point per second during one year,
the rados object size will be ~384Mo.

And some more realistic scenario, a 4Mo rados object (like rbd uses) could
come from:

- 20 days with 1 point every seconds
- 100 days with 1 point every 5 seconds

So, in realistic scenarios, the direct relation between the archive policy and
the size of the rados objects created by gnocchi is not a problem.

.. _`Ceph extended attributes documentation`: http://docs.ceph.com/docs/master/rados/configuration/filestore-config-ref/#extended-attributes


