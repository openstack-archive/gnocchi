#!/bin/bash
set -e

export OS_AUTH_PLUGIN=gnocchi-noauth
export GNOCCHI_ENDPOINT=http://localhost:8041
export GNOCCHI_USER_ID=99aae-4dc2-4fbc-b5b8-9688c470d9cc
export GNOCCHI_PROJECT_ID=c8d27445-48af-457c-8e0d-1de7103eae1f
export GNOCCHI_DATA=$(readlink -f $(mktemp -d /tmp/gnocchi.XXXX))

RESOURCE_IDS=(
    "5a301761-aaaa-46e2-8900-8b4f6fe6675a"
    "5a301761-bbbb-46e2-8900-8b4f6fe6675a"
    "5a301761-cccc-46e2-8900-8b4f6fe6675a"
)

gen_conf(){
    cat > $VIRTUAL_ENV/etc/gnocchi/gnocchi.conf <<EOF
[storage]
file_basepath = $GNOCCHI_DATA/file
driver = file
[indexer]
url = $PIFPAF_URL
EOF
}

dump_data(){
    dir="$1"
    mkdir -p $dir
    echo "* Dumping measures aggregations to $dir"
    for resource_id in $RESOURCE_IDS; do
        for agg in min max mean sum ; do
            gnocchi measures show --aggregation $agg --resource-id $resource_id metric > $dir/${agg}.txt
        done
    done
}

inject_data() {
    echo "* Injecting measures in Gnocchi"
    # TODO(sileht): Generate better data that ensure we have enought split that cover all
    # situation
    for resource_id in $RESOURCE_IDS; do
        gnocchi resource create generic --attribute id:$resource_id -n metric:high >/dev/null
    done

    {
        echo -n '{'
        resource_sep=""
        for resource_id in $RESOURCE_IDS; do
            echo -n "$resource_sep \"$resource_id\": { \"metric\": [ "
            measures_sep=""
            for i in $(seq 1 1000); do
                now=$(date --iso-8601=s -d "-${i}minute") ; value=$((RANDOM % 13 + 52))
                echo -n "$measures_sep {\"timestamp\": \"$now\", \"value\": $value }"
                measures_sep=","
            done
            echo -n "] }"
            resource_sep=","
        done
        echo -n '}'
    } | gnocchi measures batch-resources-metrics -

    echo "* Waiting for measures computation"
    while [ $(gnocchi status -f value -c "storage/total number of measures to process") -gt 0 ]; do sleep 1 ; done
}

wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

cleanup(){
    kill -9 $(jobs -p)
    wait 2>/dev/null
    rm -rf $GNOCCHI_DATA
}
trap cleanup EXIT

mkfifo ${GNOCCHI_DATA}/out
gen_conf
gnocchi-upgrade --config-dir=$VIRTUAL_ENV/etc/gnocchi
gnocchi-metricd --config-dir=$VIRTUAL_ENV/etc/gnocchi &>/dev/null &
gnocchi-api -p 8041 -- --config-dir=$VIRTUAL_ENV/etc/gnocchi &>$GNOCCHI_DATA/out &
wait_for_line "Available at" $GNOCCHI_DATA/out
inject_data $GNOCCHI_DATA
dump_data $GNOCCHI_DATA/old

kill -9 $(jobs -p)
wait 2>/dev/null

old_version=$(pip freeze | sed -n '/gnocchi==/s/.*==\(.*\)/\1/p')
new_version=$(python setup.py --version)
echo "* Upgrading Gnocchi from $old_version to $new_version"
pip install -q -U .[test,postgresql,mysql,file,ceph,swift]
gen_conf
gnocchi-upgrade --config-dir=$VIRTUAL_ENV/etc/gnocchi
gnocchi-metricd --config-dir=$VIRTUAL_ENV/etc/gnocchi &>/dev/null &
gnocchi-api -p 8041 -- --config-dir=$VIRTUAL_ENV/etc/gnocchi &>$GNOCCHI_DATA/out &
wait_for_line "Available at" $GNOCCHI_DATA/out
dump_data $GNOCCHI_DATA/new

echo "* Checking output difference between Gnocchi $old_version and $new_version"
diff -uNr $GNOCCHI_DATA/old $GNOCCHI_DATA/new
