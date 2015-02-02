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

cd $BASE/new/gnocchi

# NOTE(sileht): Just list policies for now
token=$(keystone token-get | grep ' id ' | get_field 2)
die_if_not_set $LINENO token "Keystone fail to get token"
curl -X GET ${GNOCCHI_SERVICE_PROTOCOL}://${GNOCCHI_SERVICE_HOST}:${GNOCCHI_SERVICE_PORT}/v1/archive_policy -H "Content-Type: application/json" -H "X-Auth-Token: $token"
