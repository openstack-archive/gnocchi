#!/bin/bash -x
wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

INFLUXDB_DATA=`mktemp -d /tmp/gnocchi-influxdb-XXXXX`
mkfifo ${INFLUXDB_DATA}/out
cd ${INFLUXDB_DATA}
influxd > ${INFLUXDB_DATA}/out 2>&1 &
cd -
# Wait for InfluxDB to start listening to connections
wait_for_line "data node #1 listening on" ${INFLUXDB_DATA}/out

$*

ret=$?
kill $(jobs -p)
rm -rf "${INFLUXDB_DATA}"
exit $ret
