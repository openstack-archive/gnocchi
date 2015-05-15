#!/bin/bash -x

wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

# Start MongoDB process for tests
MONGO_DATA=`mktemp -d /tmp/gnocchi-mongodb-XXXXX`
MONGO_PORT=30000
mkfifo ${MONGO_DATA}/out
mongod --maxConns 32 --nojournal --noprealloc --smallfiles --quiet --noauth --port ${MONGO_PORT} --dbpath "${MONGO_DATA}" --bind_ip localhost --config /dev/null &>${MONGO_DATA}/out &
# Wait for Mongo to start listening to connections
wait_for_line "waiting for connections on port ${MONGO_PORT}" ${MONGO_DATA}/out
# Read the fifo for ever otherwise mongod would block
cat ${MONGO_DATA}/out > /dev/null &
export GNOCCHI_TEST_INDEXER_URL="mongodb://localhost:${MONGO_PORT}/gnocchi"

mkdir $MONGO_DATA/tooz
export GNOCCHI_COORDINATION_URL="file:///$MONGO_DATA/tooz"

# Yield execution to venv command
$*

ret=$?
mongod --dbpath "${MONGO_DATA}" --shutdown
rm -rf "${MONGO_DATA}"
exit $ret
