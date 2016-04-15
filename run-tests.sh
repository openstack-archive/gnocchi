#!/bin/bash -x
set -e
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    export GNOCCHI_TEST_STORAGE_DRIVER=$storage
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        if [ "$storage" = "s3" ]
        then
            pifpaf -e GNOCCHI_STORAGE run s3rver -- \
                   pifpaf -e GNOCCHI_INDEXER run $indexer -- \
                   ./tools/pretty_tox.sh $*
        else
            pifpaf -e GNOCCHI_INDEXER run $indexer -- \
                   ./tools/pretty_tox.sh $*
        fi
    done
done
