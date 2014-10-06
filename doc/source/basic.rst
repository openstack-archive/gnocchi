==================
 What is Gnocchi?
==================

.. image:: gnocchi-logo.jpg

Gnocchi is service providing features to manage a set of resources and store
metrics about them. It allows its users to create resources with properties and
to associate these resources with entities that are going to be metered (e.g.
CPU usage or network interface bytes sent).

The point of Gnocchi is to provide this service and its features in a scalable
and resilient way. Its functionalities are exposed over an HTTP REST API.

============================
 A Brief History of Gnocchi
============================

The Gnocchi project has been started in 2014 as a spin-off of the `OpenStack
Ceilometer`_ project. Its primary goal has been to address the performance
issues that Ceilometer encountered while using standard databases as a storage
backends for metrics. More information are available on `Julien's blog post on
Gnocchi
<https://julien.danjou.info/blog/2014/openstack-ceilometer-the-gnocchi-experiment>`_.

.. _`OpenStack Ceilometer`: http://launchpad.net/ceilometer

======================
 Project Architecture
======================

Gnocchi is built around 2 main components: a storage driver and an indexer
driver. The REST API exposed to the user actually manipulates both these
drivers to provide all the features that are needed to provide correct
infrastructure measurement.

The *storage* is responsible to store metrics of created entities. It receive
timestamp and values and compute aggregation according the the defined archive
policies.

The *indexer* is responsible for storing the index of all resourecs, along with
their types and their properties. Gnocchi only knows resources types from the
OpenStack project, but also provide a *generic* type so you can create basic
resources and handle the resource properties yourself. The indexer is also
responsible for linking resources with entities.
