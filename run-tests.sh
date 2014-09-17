#!/bin/bash -x
memcached &

[ "$1" = "--coverage" ] && COVERAGE=1

ret=0
if [ ! "$COVERAGE" ]; then
   # Ceilometer dispatcher  tests
   bash tools/init_testr_if_needed.sh
   python setup.py testr --slowest --testr-args="--here=ceilometer_tests $*"
   ret=$?
fi

[ "$ret" == "0" ] && python setup.py testr --slowest --testr-args="$*"

ret=$?
kill $(jobs -p)
exit $ret
