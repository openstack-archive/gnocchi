#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# This script is executed inside gate_hook function in devstack gate.

STORAGE_DRIVER="$1"
SQL_DRIVER="$2"

ENABLED_SERVICES="key,gnocchi-api,"
ENABLED_SERVICES+="ceilometer-acentral,ceilometer-collector,ceilometer-api,"
ENABLED_SERVICES+="ceilometer-alarm-notifier,ceilometer-alarm-evaluator,ceilometer-anotification,"

export DEVSTACK_GATE_INSTALL_TESTONLY=1
export DEVSTACK_GATE_NO_SERVICES=1
export DEVSTACK_GATE_TEMPEST=0
export DEVSTACK_GATE_EXERCISES=0
export KEEP_LOCALRC=1

case $STORAGE_DRIVER in
    file)
        DEVSTACK_LOCAL_CONFIG+=$'\nexport GNOCCHI_STORAGE_BACKEND=file'
        ;;
    swift)
        ENABLED_SERVICES+="s-proxy,s-account,s-container,s-object,"
        DEVSTACK_LOCAL_CONFIG+=$'\nexport GNOCCHI_STORAGE_BACKEND=swift'
        ;;
    ceph)
        ENABLED_SERVICES+="ceph"
        DEVSTACK_LOCAL_CONFIG+=$'\nexport GNOCCHI_STORAGE_BACKEND=ceph'
        ;;
esac


# default to mysql
case $SQL_DRIVER in
    postgresql)
        export DEVSTACK_GATE_POSTGRES=1
        ;;
esac

DEVSTACK_LOCAL_CONFIG+=$'\nexport USE_VENV=True'
DEVSTACK_LOCAL_CONFIG+=$'\nexport GNOCCHI_USE_KEYSTONE=False'
export ENABLED_SERVICES
export DEVSTACK_LOCAL_CONFIG

$BASE/new/devstack-gate/devstack-vm-gate.sh
