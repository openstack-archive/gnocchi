# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
from __future__ import absolute_import

import datetime

from oslo_db.sqlalchemy import models
import six
import sqlalchemy
from sqlalchemy.dialects import mysql
from sqlalchemy.ext import declarative
from sqlalchemy import types
import sqlalchemy_utils

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import storage

Base = declarative.declarative_base()

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}


class PreciseTimestamp(types.TypeDecorator):
    """Represents a timestamp precise to the microsecond."""

    impl = sqlalchemy.DateTime

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.DATETIME(fsp=6))
        return self.impl


class GnocchiBase(models.ModelBase):
    pass


class ArchivePolicyDefinitionType(sqlalchemy_utils.JSONType):
    def process_result_value(self, value, dialect):
        values = super(ArchivePolicyDefinitionType,
                       self).process_result_value(value, dialect)
        return [archive_policy.ArchivePolicyItem(**v) for v in values]


class SetType(sqlalchemy_utils.JSONType):
    def process_result_value(self, value, dialect):
        return set(super(SetType,
                         self).process_result_value(value, dialect))


class ArchivePolicy(Base, GnocchiBase, archive_policy.ArchivePolicy):
    __tablename__ = 'archive_policy'
    __table_args__ = (
        sqlalchemy.Index('ix_archive_policy_name', 'name'),
        COMMON_TABLES_ARGS,
    )

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
    back_window = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    definition = sqlalchemy.Column(ArchivePolicyDefinitionType, nullable=False)
    # TODO(jd) Use an array of string instead, PostgreSQL can do that
    aggregation_methods = sqlalchemy.Column(SetType,
                                            nullable=False)


class Metric(Base, GnocchiBase, storage.Metric):
    __tablename__ = 'metric'
    __table_args__ = (
        sqlalchemy.Index('ix_metric_id', 'id'),
        sqlalchemy.UniqueConstraint("resource_id", "name",
                                    name="uniq_metric0resource_id0name"),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           primary_key=True)
    archive_policy_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey('archive_policy.name',
                              ondelete="RESTRICT"),
        nullable=False)
    archive_policy = sqlalchemy.orm.relationship(ArchivePolicy)
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    resource_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                                    sqlalchemy.ForeignKey('resource.id',
                                                          ondelete="CASCADE"))
    name = sqlalchemy.Column(sqlalchemy.String(255))

    def jsonify(self):
        d = {
            "id": self.id,
            "created_by_user_id": self.created_by_user_id,
            "created_by_project_id": self.created_by_project_id,
            "name": self.name,
            "resource_id": self.resource_id,
        }
        if 'archive_policy' in sqlalchemy.inspect(self).unloaded:
            d['archive_policy_name'] = self.archive_policy_name
        else:
            d['archive_policy'] = self.archive_policy
        return d

    def __eq__(self, other):
        # NOTE(jd) If `other` is a SQL Metric, we only compare
        # archive_policy_name, and we don't compare archive_policy that might
        # not be loaded. Otherwise we fallback to the original comparison for
        # storage.Metric.
        return ((isinstance(other, Metric)
                 and self.id == other.id
                 and self.archive_policy_name == other.archive_policy_name
                 and self.created_by_user_id == other.created_by_user_id
                 and self.created_by_project_id == other.created_by_project_id
                 and self.name == other.name
                 and self.resource_id == other.resource_id)
                or (storage.Metric.__eq__(self, other)))


class Resource(Base, GnocchiBase, indexer.Resource):
    __tablename__ = 'resource'
    __table_args__ = (
        sqlalchemy.Index('ix_resource_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           primary_key=True)
    type = sqlalchemy.Column(sqlalchemy.Enum('metric', 'generic', 'instance',
                                             'swift_account', 'volume',
                                             'ceph_account', 'network',
                                             'identity', 'ipmi', 'stack',
                                             'image',
                                             name="resource_type_enum"),
                             nullable=False, default='generic')
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    metrics = sqlalchemy.orm.relationship(Metric)
    # NOTE(jd) I wish we could use server_default…
    started_at = sqlalchemy.Column(PreciseTimestamp, nullable=False,
                                   default=datetime.datetime.utcnow)
    ended_at = sqlalchemy.Column(PreciseTimestamp)
    user_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))
    project_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))

    def jsonify(self):
        d = dict(self)
        if 'metrics' not in sqlalchemy.inspect(self).unloaded:
            d['metrics'] = dict((m['name'], six.text_type(m['id']))
                                for m in self.metrics)
        return d


class ResourceExtMixin(object):
    @declarative.declared_attr
    def __table_args__(cls):
        return (sqlalchemy.Index('ix_%s_id' % cls.__tablename__, 'id'),
                COMMON_TABLES_ARGS)

    @declarative.declared_attr
    def id(cls):
        return sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                                 sqlalchemy.ForeignKey('resource.id',
                                                       ondelete="CASCADE"),
                                 primary_key=True)
