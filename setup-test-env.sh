#!/bin/bash
set -e
GNOCCHI_TEST_INDEXER_DRIVER=${GNOCCHI_TEST_INDEXER_DRIVER:-postgresql}
source $(which overtest) $GNOCCHI_TEST_INDEXER_DRIVER
set -x
export GNOCCHI_INDEXER_URL=${OVERTEST_URL/#mysql:/mysql+pymysql:}
export GNOCCHI_COORDINATION_URL=${OVERTEST_URL}
$*
