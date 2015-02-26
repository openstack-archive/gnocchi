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

# This script is executed inside post_test_hook function in devstack gate.

source $BASE/new/devstack/functions-common
source $BASE/new/devstack/openrc admin admin

set -x

cd $BASE/new/gnocchi

keystone endpoint-list
keystone service-list
keystone endpoint-get --service metric
demo2_project_id=$(keystone tenant-create --name demo2 | grep ' id ' | get_field 2)
demo2_user_id=$(keystone user-create --name demo2 --pass secrete | grep ' id ' | get_field 2)
keystone user-role-add --user $demo2_user_id --role Member --tenant $demo2_project_id

gnocchi_endpoint=$(keystone endpoint-get --service metric | grep ' metric.publicURL ' | get_field 2)
die_if_not_set $LINENO gnocchi_endpoint "Keystone fail to get gnocchi endpoint"
token=$(keystone token-get | grep ' id ' | get_field 2)
die_if_not_set $LINENO token "Keystone fail to get token"

# NOTE(sileht): Just list policies for now
curl -X GET $gnocchi_endpoint/v1/archive_policy -H "Content-Type: application/json" -H "X-Auth-Token: $token"

add_dash(){
    read uuid
    echo ${uuid:0:8}-${uuid:8:4}-${uuid:12:4}-${uuid:16:4}-${uuid:20}
}

source $BASE/new/devstack/openrc demo demo
export GABBI_TOKEN_DEMO=$(keystone token-get | grep ' id ' | get_field 2)
source $BASE/new/devstack/openrc demo2 demo2
export OS_PASSWORD=secrete
export GABBI_TOKEN_DEMO2=$(keystone token-get | grep ' id ' | get_field 2)
source $BASE/new/devstack/openrc admin admin
export GABBI_TOKEN_ADMIN=$(keystone token-get | grep ' id ' | get_field 2)

export GABBI_USER_ID_ADMIN=$(keystone user-get admin | grep ' id ' | get_field 2 | add_dash)
export GABBI_PROJECT_ID_ADMIN=$(keystone tenant-get admin | grep ' id ' | get_field 2 | add_dash)
export GABBI_USER_ID_DEMO=$(keystone user-get demo | grep ' id ' | get_field 2 | add_dash)
export GABBI_PROJECT_ID_DEMO=$(keystone tenant-get demo | grep ' id ' | get_field 2 | add_dash)
export GABBI_USER_ID_DEMO2=$(keystone user-get demo2 | grep ' id ' | get_field 2 | add_dash)
export GABBI_PROJECT_ID_DEMO2=$(keystone tenant-get demo2 | grep ' id ' | get_field 2 | add_dash)

export GABBI_GNOCCHI_HOST=localhost
export GABBI_GNOCCHI_PORT=8041

sudo -E tox -epy27-gabbi test_gabbi_gate.*
