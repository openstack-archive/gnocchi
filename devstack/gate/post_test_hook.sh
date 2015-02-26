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

set

source $BASE/new/devstack/openrc admin admin

cd $BASE/new/gnocchi

keystone endpoint-list
keystone service-list
keystone endpoint-get --service metric

curl -X GET http://localhost:8041/v1/archive_policy -H "Content-Type: application/json"

export GABBI_GNOCCHI_HOST=localhost
export GABBI_GNOCCHI_PORT=8041

sudo -E tox -epy27-gabbi
