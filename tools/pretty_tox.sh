#!/usr/bin/env bash


git clone https://github.com/openstack/oslo.db
cd oslo.db
git fetch git://git.openstack.org/openstack/oslo.db refs/changes/86/406786/1 && git checkout FETCH_HEAD
pip install -U ./
cd -

set -o pipefail

TESTRARGS=$1

# --until-failure is not compatible with --subunit see:
#
# https://bugs.launchpad.net/testrepository/+bug/1411804
#
# this work around exists until that is addressed
if [[ "$TESTARGS" =~ "until-failure" ]]; then
    python setup.py testr --slowest --testr-args="$TESTRARGS"
else
    python setup.py testr --slowest --testr-args="--subunit $TESTRARGS" | subunit-trace -f
fi
