#!/bin/bash -x
memcached &
python -m lockfile
python -m gnocchi.openstack.common.db.sqlalchemy.test_migrations

python setup.py testr --slowest --testr-args="$*"

ret=$?
kill $(jobs -p)
exit $ret
