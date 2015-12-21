======================
 Project Architecture
======================

Gnocchi is split in two parts: the two front-ends and an asynchronous
processing daemon. The provided front-ends are an HTTP REST API (see
:doc:`rest`) and a statsd-compatible daemon (see :doc:`statsd`). The
asynchronous processing daemon, called `gnocchi-metricd`, handle asynchronous
operation (statistics computing, metric cleanup, etc) in the background.

Both the HTTP REST API and the asynchronous processing daemon are stateless and
are scalable. You can run more of them in order to speed up Gnocchi execution.

The statsd-compatible daemon, called `gnocchi-statsd`, may be scaled in certain
case, but trade-offs have to been made due to the nature of the statsd
protocol.


Back-ends
---------

Gnocchi needs two different back-ends for storing data: one for storing the
time series (the storage driver) and one for indexing the data (the index
driver).

The *storage* is responsible for storing measures of created metrics. It
receives timestamps and values and computes aggregations according to the
defined archive policies.

The *indexer* is responsible for storing the index of all resources, along with
their types and their properties. Gnocchi only knows resource types from the
OpenStack project, but also provides a *generic* type so you can create basic
resources and handle the resource properties yourself. The indexer is also
responsible for linking resources with metrics.

How to choose back-ends
~~~~~~~~~~~~~~~~~~~~~~~

Gnocchi currently offers 4 storage drivers:

* File
* Swift
* Ceph (preferred)
* InfluxDB (experimental)

The first three drivers are based on an intermediate library, named
*Carbonara*, which handles the time series manipulation, since neither of these
storage technology handle time series natively. `InfluxDB`_ does not need this
layer since it is itself a time series database. However, The InfluxDB driver
is still experimental and suffers from bugs in InfluxDB itself that are yet to
be fixed as of this writing.

The 3 *Carbonara* based drivers are working well and are as scalable as their
backend technology permits. Obviously, Ceph and Swift are inherently more
scalable than the file driver.

Depending on the size of your architecture, using the file driver and storing
your data on a disk might be enough. If you need to scale the number of server
with the file driver, you can export and share the data via NFS among all
Gnocchi processes. In any case, it is obvious that Ceph and Swift drivers are
largely more scalable. Ceph also offers better consistency, and hence was
selected as Red Hat’s preferred driver in Gnocchi nowadays.

.. _InfluxDB: http://influxdb.com

How to plan for Gnocchi’s storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Gnocchi uses a custom file format based on its library *Carbonara*. In Gnocchi,
a time serie is a collection of points, where a point is a given measure, or
sample, in the lifespan of a time serie. The storage format is pretty
straightforward, therefore the computing of a time serie size can be done with
the following formula::

    number of points × (64 bits timestamp + 64 bits floating value) × 1.12
    = number of points × 16 bytes × 1.12
    = number of points × 17.92
    = size in bytes

The number of points you want to keep is usually determined by the following
formula::

    number of points = timespan ÷ granularity

For example, if you want to keep a year of data with a one minute resolution::

    number of points = (365 days × 24 hours × 60 minutes) ÷ 1 minute
    number of points = 525 600

Then::

    size in bytes = 525 600 × 17.92 = 9 418 752 bytes = 9 198 KiB

This is just for an entire aggregated time serie. If your archive policy uses
the 8 default aggregation methods (mean, min, max, sum, std, median, count,
95pct) with only one timeserie of a year of data with a one minute aggregation
period, the space used will go up to 8 × 9 MiB = 72 MiB.

How to set the archive policy and granularity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Gnocchi, the archive policy is expressed in number of points. If your
archive policy define a policy of 10 points with a granularity of 1 second, the
time serie archive will keep up to 10 points representing an aggregation over 1
second. This mean the time serie will at maximum retains 10 seconds of data,
**but** that does not mean it has to be 10 successive seconds: there might be
gap if data were fed irregularly.

Consequently, there is no expiry of data relative to the current timestamp, and
you cannot delete old data points (at least for now).

Therefore both the archive policy and the granularity entirely depends on your
use case. Depending on the usage of your data, you can define several archiving
policy. A typical low grained use case could be::

    3600 points with a granularity of 1 second = 1 hour
    1440 points with a granularity of 1 minute = 24 hours
    1800 points with a granularity of 1 hour = 30 days
    365 points with a granularity of 1 day = 1 year

This would represent 7205 points × 17.92 = 126 KiB per aggregation method. If
you use the 8 standard aggregation method, your metric will take up to 8 × 126
KiB = 0.98 MiB of disk space.

