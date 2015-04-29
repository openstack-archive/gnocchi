========
 Gnocchi
========

Most of the IT infrastructures out there consists of a set of resources. These
resources have properties: some of them are simple attributes whereas others
might be measurable quantities (also known as metrics).

And in this context, the cloud infrastructures make no exception. We talk about
instances, volumes, networks… which are all different kind of resources. The
problems that are arising with the cloud trend is the scalability of storing
all this data and being able to request them later, for whatever usage.

What Gnocchi provides is a REST API that allows the user to manipulate
resources (CRUD) and their attributes, while preserving the history of those
resources and their attributes.

Gnocchi is fully documented and the documentation is available online. We are
the first OpenStack project to require patches to integrate the documentation.
We want to raise the bar, so we took a stand on that. That's part of our
policy, the same way it's part of the OpenStack policy to require unit tests.

There is a more consistent `presentation of Gnocchi
<https://julien.danjou.info/blog/2015/openstack-gnocchi-first-release>`_ and
`online documentation <http://gnocchi.readthedocs.org/en/latest/index.html>`_
