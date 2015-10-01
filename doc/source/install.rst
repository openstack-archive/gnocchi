
Configuration
=============

Configure Gnocchi by editing `/etc/gnocchi/gnocchi.conf`.

No config file is provided with the source code, but one can be easily
created by running:

::

    tox -e genconfig

This command will create an `etc/gnocchi/gnocchi.conf` file which can be used
as a base for the default configuration file at `/etc/gnocchi/gnocchi.conf`.
This will not be system wide, but installed in a virtualenv, to not only generate
the config file but installing dependencies, before installing run:
::
    pip install -U -r requirements.txt

On ubuntu/debian extra packages must be installed before running requirements,
and some upgrades must be executed:
::
    apt-get install libpq-dev libxslt1-dev
    pip install -U tox==2.1.1
    pip install -U six==1.9.0

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

- `PostgreSQL`_
- `MySQL`_

.. _`Swift`: https://launchpad.net/swift
.. _`Ceph`: http://ceph.com/
.. _`PostgreSQL`: http://postgresql.org
.. _`MySQL`: http://mysql.com
.. _`InfluxDB`: http://influxdb.com

gnocchi.conf example
====================
::

    [DEFAULT]
    debug = True
    verbose = True
    log_file = /var/log/gnocchi/gnocchi.log

    [api]
    port = 8041
    host = 0.0.0.0
    workers = 2
    
    [metricd]
    workers = 2
    
    [indexer]
    url = mysql://gnocchi:NOTgnocchi@dbserver/gnocchi?charset=utf8

    [keystone_authtoken]
    signing_dir = /var/cache/gnocchi
    auth_uri = http://keystone:5000/v2.0
    auth_url = http://keystone:35357/v2.0
    project_domain_id = default
    project_name = service
    project_name = admin
    password = CLOUDADMINpassword
    username = cloudadmin
    auth_plugin = password
    memcached_servers = memcached1:11211,memcached2:11211
    memcache_security_strategy = ENCRYPT
    memcache_secret_key = s3cr3tkey

    [storage]
    driver = ceph
    metric_processing_delay = 5
    ceph_pool = gnocchi
    ceph_username = gnocchi
    ceph_keyring = /etc/ceph/ceph.client.gnocchi.keyring
    ceph_conffile = /etc/ceph/ceph.conf
    file_basepath = /var/lib/gnocchi
    file_basepath_tmp = ${file_basepath}/tmp

==============
 Installation
==============

To install Gnocchi, run the standard Python installation procedure:

::

    python setup.py install

Indexer Initialization
======================

Once you have configured Gnocchi properly, you need to initialize the indexer:

eg. on MySQL

::

    mysql> create database gnocchi;
    mysql> GRANT ALL PRIVILEGES ON gnocchi.* TO 'gnocchi'@'localhost' \
       IDENTIFIED BY 'NOTgnocchi';
    mysql> GRANT ALL PRIVILEGES ON gnocchi.* TO 'gnocchi'@'%' \
      IDENTIFIED BY 'NOTgnocchi';

::

    gnocchi-dbsync


Running Gnocchi
===============

To run Gnocchi, simply run the HTTP server:

::

    gnocchi-api

You then need to run the `gnocchi-metricd` daemon to enable new measures
processing in the background and to appear on the measures get API call. 
Some storage drivers (such as `influxdb`) do not need this process to run
so it will exit gracefully at startup.

Configuring CEPH
================
For CEPH backend to work with the example configuration, this minimal commands needs to be run from either a ceph monitor or a ceph osd:

::

    ceph osd pool create gnocchi 512
    ceph osd pool set gnocchi size 1
    ceph auth get-or-create client.gnocchi mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=gnocchi' > /tmp/key

On the gnocchi api side execute:

::

    apt-get install ceph ceph-common
    echo the output of the command ceph auth get-or-create client.gnocchi on ceph mon > /etc/ceph/ceph.client.gnocchi.keyring
    echo the /etc/ceph/ceph.conf on ceph mon > /etc/ceph/ceph.comf 


Then on gnocchi api server verify ceph configuration:

::

    ceph -n client.gnocchi -s

Recieving Metrics for the first time
====================================
Before receiving metrics for the first time, remember that you will need to add (in this order):

::

    1. archive policies
    2. archive policies rules
    3. default archive policy rule

You can refer to the `REST API Usage`_ guide on how to add Archive Policies and Rules.

.. _`REST API Usage`: http://docs.openstack.org/developer/gnocchi/rest.html

Running As A WSGI Application
=============================

It's possible – and strongly advised – to run Gnocchi through a WSGI
service such as `mod_wsgi`_ or any other WSGI application. The file
`gnocchi/rest/app.wsgi` provided with Gnocchi allows you to enable Gnocchi as
a WSGI application.
For other WSGI setup you can refer to the `pecan deployement`_ documentation.

.. _`mod_wsgi`: https://modwsgi.readthedocs.org/en/master/
.. _`pecan deployement`: http://pecan.readthedocs.org/en/latest/deployment.html#deployment

