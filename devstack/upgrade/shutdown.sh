#!/bin/bash
#
#

set -o errexit

source $GRENADE_DIR/grenaderc
source $GRENADE_DIR/functions

source $BASE_DEVSTACK_DIR/functions
source $BASE_DEVSTACK_DIR/stackrc # needed for status directory
source $BASE_DEVSTACK_DIR/lib/tls
source $BASE_DEVSTACK_DIR/lib/apache

# Locate the Gnocchi plugin and get its functions
GNOCCHI_DEVSTACK_DIR=$(dirname $(dirname $0))
source $GNOCCHI_DEVSTACK_DIR/plugin.sh

set -o xtrace

stop_gnocchi

# ensure everything is stopped
ensure_services_stopped gnocchi-api gnocchi-metricd gnocchi-statsd
