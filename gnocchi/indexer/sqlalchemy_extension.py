# -*- encoding: utf-8 -*-

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

from __future__ import absolute_import

import sqlalchemy
import sqlalchemy_utils

from gnocchi import resource_type

# NOTE(sileht): this code is also in alembic migration
legacy_ceilometer_tablenames = {
    "instance_network_interface": "instance_net_int"
}
legacy_ceilometer_resources = {
    "generic": {},
    "image": {
        "name": {"type": "string", "length": 255, "required": True},
        "container_format": {"type": "string", "length": 255,
                             "required": True},
        "disk_format": {"type": "string", "length": 255, "required": True},
    },
    "instance": {
        "flavor_id": {"type": "string", "length": 255, "required": True},
        "image_ref": {"type": "string", "length": 255, "required": False},
        "host": {"type": "string", "length": 255, "required": True},
        "display_name": {"type": "string", "length": 255, "required": True},
        "server_group": {"type": "string", "length": 255, "required": False},
    },
    "instance_disk": {
        "name": {"type": "string", "length": 255, "required": True},
        "instance_id": {"type": "uuid", "required": True},
    },
    "instance_network_interface": {
        "name": {"type": "string", "length": 255, "required": True},
        "instance_id": {"type": "uuid", "required": True},
    },
    "volume": {
        "display_name": {"type": "string", "length": 255, "required": False},
    },
    "swift_account": {},
    "ceph_account": {},
    "network": {},
    "identity": {},
    "ipmi": {},
    "stack": {},
}


class StringSchema(resource_type.StringSchema):
    @property
    def satype(self):
        return sqlalchemy.String(self.length)


class UUIDSchema(resource_type.UUIDSchema):
    satype = sqlalchemy_utils.UUIDType()


class IntSchema(resource_type.IntSchema):
    satype = sqlalchemy.Integer


class FloatSchema(resource_type.FloatSchema):
    satype = sqlalchemy.Float(53)


class BoolSchema(resource_type.BoolSchema):
    satype = sqlalchemy.Boolean
