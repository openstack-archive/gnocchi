#!/bin/bash -x
memcached &
python -c 'import lockfile.linklockfile'
python -m gnocchi.openstack.common.db.sqlalchemy.test_migrations

python setup.py testr --slowest --testr-args="$*"

ret=$?
kill $(jobs -p)
exit $ret
