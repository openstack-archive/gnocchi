#!/bin/bash
set -eux

if [ -z "$(which influxdb)" ]; then
    INFLUXDB_VERSION=1.1.0
    INFLUXDB_RELEASE_URL=https://dl.influxdata.com/influxdb/releases
    case `uname -s` in
        Linux)
            influxdb_file="influxdb-${INFLUXDB_VERSION}_linux_amd64.tar.gz"
            ;;
        *)
            echo "Unknown operating system"
            exit 1
            ;;
    esac
    influxdb_dir=`basename $influxdb_file .tar.gz`
    mkdir -p $influxdb_dir
    curl -L $INFLUXDB_RELEASE_URL/$influxdb_file > $influxdb_dir/$influxdb_file
    tar xf $influxdb_dir/$influxdb_file -C $influxdb_dir
    export PATH=$PATH:$influxdb_dir/influxdb-${INFLUXDB_VERSION}-1/usr/bin/
fi

# Yield execution to venv command
$*
