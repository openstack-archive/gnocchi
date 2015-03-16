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

mkdir ${INFLUXDB_DATA}/{broker,data}
mkfifo ${INFLUXDB_DATA}/out

# TODO(sileht): don't use default port
cat > $INFLUXDB_DATA/config <<EOF
port = 8086
[admin]
  enabled = false
[broker]
 dir = "${INFLUXDB_DATA}/broker"
[data]
 dir = "${INFLUXDB_DATA}/data"
EOF

influxd -config $INFLUXDB_DATA/config > ${INFLUXDB_DATA}/out 2>&1 &
# Wait for InfluxDB to start listening to connections
wait_for_line "API server listening on" ${INFLUXDB_DATA}/out

$*

ret=$?
kill $(jobs -p)
rm -rf "${INFLUXDB_DATA}"
exit $ret
