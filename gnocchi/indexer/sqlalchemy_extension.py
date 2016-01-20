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

import six
import sqlalchemy
import sqlalchemy_utils
import voluptuous

from gnocchi import utils


class Image(object):
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    container_format = sqlalchemy.Column(sqlalchemy.String(255),
                                         nullable=False)
    disk_format = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)


class Instance(object):
    flavor_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    image_ref = sqlalchemy.Column(sqlalchemy.String(255))
    host = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    server_group = sqlalchemy.Column(sqlalchemy.String(255))


class InstanceDisk(object):
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    instance_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                                    nullable=False)


class InstanceNetworkInterface(object):
    __tablename__ = 'instance_net_int'
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    instance_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                                    nullable=False)


class Volume(object):
    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=True)


class StringSchema(object):
    @staticmethod
    def schema():
        return {
            voluptuous.Required('type'): 'string',
            voluptuous.Required('required', default=True): bool,
            voluptuous.Required('length', default=255):
                voluptuous.All(int, voluptuous.Range(min=1, max=255))
        }

    @staticmethod
    def resource_schema(name, conf):
        schema = voluptuous.All(six.text_type,
                                voluptuous.Length(max=conf['length']))
        if conf['required']:
            return {name: schema}
        else:
            return {voluptuous.Optional(name): schema}

    @staticmethod
    def column(conf):
        return sqlalchemy.Column(sqlalchemy.String(conf['length']),
                                 nullable=not conf['required'])


class UUIDSchema(object):
    @staticmethod
    def schema():
        return {
            voluptuous.Required('type'): 'uuid',
            voluptuous.Required('required', default=True): bool,
        }

    @staticmethod
    def resource_schema(name, conf):
        if conf['required']:
            return {name: utils.UUID}
        else:
            return {voluptuous.Optional(name): utils.UUID}

    @staticmethod
    def column(conf):
        return sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                                 nullable=not conf['required'])


class IntSchema(object):
    schema_name = "int"
    schema_type = int
    sql_type = sqlalchemy.Integer

    @classmethod
    def schema(cls):
        return {
            voluptuous.Required('type'): cls.schema_name,
            voluptuous.Required('required', default=True): bool,
            voluptuous.Required('min', default=None): voluptuous.Any(
                None, voluptuous.All(cls.schema_type,
                                     voluptuous.Range(min=0))),
            voluptuous.Required('max', default=None): voluptuous.Any(
                None, voluptuous.All(cls.schema_type,
                                     voluptuous.Range(min=0)))
        }

    @classmethod
    def resource_schema(cls, name, conf):
        schema = voluptuous.All(cls.schema_type,
                                voluptuous.Range(min=conf['min'],
                                                 max=conf['max']))
        if conf['required']:
            return {name: schema}
        else:
            return {voluptuous.Optional(name): schema}

    @classmethod
    def column(cls, conf):
        return sqlalchemy.Column(cls.sql_type, nullable=not conf['required'])


class FloatSchema(IntSchema):
    schema_name = "float"
    schema_type = float
    # NOTE(sileht): precision based on what we use in Ceilometer
    # Gnocchi should offer more ?
    sql_type = sqlalchemy.Float(53)
