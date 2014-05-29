====
 Gnocchi
====

REST API to store metrics in object storage.

Gnocchi uses Pandas to analyze metrics, with Swift as the storage driver.

====
Installation Instructions
====

#. Install Swift, either by enabling the required services in a full devstack environment, or a swift stand-alone installation.

To enable Swift in Devstack, type the following into your localrc file:
::
    enable_service s-proxy s-account s-container s-object

and run `./stack.sh`.

For directions on installing Swift by itself, see `these instructions <https://docs.openstack.org/developer/swift/development_saio.html>`_.

#. Clone the gnocchi git repo (if you have a full Devstack environment, the usual directory in which to install gnocchi would be /opt/stack/).
::
    cd /opt/stack && git clone https://github.com/stackforge/gnocchi.git

#. You may need to install the following libraries, depending on your system (shown are commands for Ubuntu users):
::
    sudo apt-get install build-essential libpq-dev libx11-dev libasound2-dev

#. Install the requirements and run the setup.py file:
::
    sudo pip install -r requirements.txt; sudo pip install -r test-requirements.txt; sudo python setup.py install

#. If it doesn't exist, create a gnocchi.conf file in /etc/gnocchi/ (you may need to make the gnocchi sub-directory as well). Write the following code into the file:
::
    [api]
    port = 8041
    host = 0.0.0.0
    
    [storage]
    swift_auth_version = 1
    swift_authurl = http://localhost:8080/auth/v1.0
    swift_user = admin:admin
    swift_key = admin
    swift_coordination_driver = memcached
    
    [indexer]
    driver = sqlalchemy
    
    [database]
    connection = sqlite:////opt/stack/gnocchi/gnocchi/openstack/common/db/gnocchi.sqlite
    sqlite_db = gnocchi.sqlite

The line specifying the connection string has the general form `sqlite:////<absolute-path-to-db>/gnocchi.sqlite`; if you installed gnocchi to a different directory other than /opt/stack/, make sure to modify this line accordingly.

#. Run the gnocchi api service.
::
    gnocchi-api --debug --config-file /etc/gnocchi/gnocchi.conf`

You can now send requests to the API in a different terminal - see the `API specs <http://docs-draft.openstack.org/34/94834/9/gate/gate-telemetry-specs-docs/4654c39/doc/build/html/specs/gnocchi.html>`_ for examples of specific queries.


