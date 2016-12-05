#!/usr/bin/env bash


pip install https://review.openstack.org/changes/406779/revisions/09188819587e8c4425b94d388252a54a2431de0c/archive?format=tgz

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
