#!/bin/bash -x
EXIT_CODE=0
INDEXER_DRIVERS="$1"
shift
STORAGE_DRIVERS="$1"
shift
for storage in $STORAGE_DRIVERS
do
    for indexer in $INDEXER_DRIVERS
    do
        GNOCCHI_TEST_STORAGE_DRIVER=$storage ./setup-${indexer}-tests.sh ./tools/pretty_tox.sh $*
        RETURN_CODE=$?
        if [ $RETURN_CODE -ne 0 ]
        then
            EXIT_CODE=$RETURN_CODE
        fi
    done
done
exit $EXIT_CODE
