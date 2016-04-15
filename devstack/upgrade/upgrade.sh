echo "*********************************************************************"
echo "Begin $0"
echo "*********************************************************************"

# Clean up any resources that may be in use
cleanup() {
    set +o errexit

    echo "*********************************************************************"
    echo "ERROR: Abort $0"
    echo "*********************************************************************"

    # Kill ourselves to signal any calling process
    trap 2; kill -2 $$
}

trap cleanup SIGHUP SIGINT SIGTERM

# Keep track of the grenade directory
RUN_DIR=$(cd $(dirname "$0") && pwd)

# Source params
source $GRENADE_DIR/grenaderc

# Import common functions
source $GRENADE_DIR/functions

# This script exits on an error so that errors don't compound and you see
# only the first error that occurred.
set -o errexit

# Get Gnocchi functions from devstack plugin
source $GNOCCHI_DEVSTACK_DIR/settings

# Print the commands being run so that we can see the command that triggers
# an error.
set -o xtrace

# Install the target gnocchi
source $GNOCCHI_DEVSTACK_DIR/plugin.sh stack install

# calls upgrade-gnocchi for specific release
upgrade_project gnocchi $RUN_DIR $BASE_DEVSTACK_BRANCH $TARGET_DEVSTACK_BRANCH

GNOCCHI_BIN_DIR=$(dirname $(which gnocchi-upgrade))
$GNOCCHI_BIN_DIR/gnocchi-upgrade || die $LINENO "Gnocchi upgrade"

# Start Gnochi
start_gnocchi

ensure_services_started  gnocchi-api gnocchi-metricd gnocchi-statsd

set +o xtrace
echo "*********************************************************************"
echo "SUCCESS: End $0"
echo "*********************************************************************"
