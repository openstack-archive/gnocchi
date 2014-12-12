# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
import calendar
import datetime
import decimal
import itertools
import operator
import uuid

from oslo.db import exception
from oslo.db import options
from oslo.db.sqlalchemy import models
from oslo.db.sqlalchemy import session
from oslo.utils import timeutils
from oslo.utils import units
import six
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy import types
import sqlalchemy_utils

from gnocchi import indexer


Base = declarative.declarative_base()

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}

_marker = indexer._marker


class PreciseTimestamp(types.TypeDecorator):
    """Represents a timestamp precise to the microsecond."""

    impl = sqlalchemy.DateTime

    @staticmethod
    def _decimal_to_dt(dec):
        """Return a datetime from Decimal unixtime format."""
        if dec is None:
            return None

        integer = int(dec)
        micro = (dec - decimal.Decimal(integer)) * decimal.Decimal(units.M)
        daittyme = datetime.datetime.utcfromtimestamp(integer)
        return daittyme.replace(microsecond=int(round(micro)))

    @staticmethod
    def _dt_to_decimal(utc):
        """Datetime to Decimal.

        Some databases don't store microseconds in datetime
        so we always store as Decimal unixtime.
        """
        if utc is None:
            return None

        decimal.getcontext().prec = 30
        return (decimal.Decimal(str(calendar.timegm(utc.utctimetuple()))) +
                (decimal.Decimal(str(utc.microsecond)) /
                 decimal.Decimal("1000000.0")))

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(
                types.DECIMAL(precision=20,
                              scale=6,
                              asdecimal=True))
        return self.impl

    def process_bind_param(self, value, dialect):
        if dialect.name == 'mysql':
            return self._dt_to_decimal(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'mysql':
            return self._decimal_to_dt(value)
        return value


class GnocchiBase(models.ModelBase):
    pass


class ResourceMetric(Base, GnocchiBase):
    __tablename__ = 'resource_metric'
    __table_args__ = (
        sqlalchemy.UniqueConstraint('resource_id', 'name', name="name_unique"),
        COMMON_TABLES_ARGS,
    )

    resource_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                                    sqlalchemy.ForeignKey('resource.id',
                                                          ondelete="CASCADE"),
                                    primary_key=True)
    metric_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                                  sqlalchemy.ForeignKey('metric.id',
                                                        ondelete="CASCADE"),
                                  primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    resources = sqlalchemy.orm.relationship('Resource')


class Resource(Base, GnocchiBase):
    __tablename__ = 'resource'
    __table_args__ = (
        sqlalchemy.Index('ix_resource_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           primary_key=True)
    type = sqlalchemy.Column(sqlalchemy.Enum('metric', 'generic', 'instance',
                                             'swift_account',
                                             name="resource_type_enum"),
                             nullable=False, default='generic')
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
    metrics = sqlalchemy.orm.relationship(ResourceMetric)
    started_at = sqlalchemy.Column(PreciseTimestamp, nullable=False,
                                   # NOTE(jd): We would like to use
                                   # sqlalchemy.func.now, but we can't
                                   # because the type of PreciseTimestamp in
                                   # MySQL is not a Timestamp, so it would
                                   # not store a timestamp but a date as an
                                   # integer…
                                   default=datetime.datetime.utcnow)
    ended_at = sqlalchemy.Column(PreciseTimestamp)
    user_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))
    project_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))


class ArchivePolicy(Base, GnocchiBase):
    __tablename__ = 'archive_policy'
    __table_args__ = (
        sqlalchemy.Index('ix_archive_policy_name', 'name'),
        COMMON_TABLES_ARGS,
    )

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
    back_window = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    definition = sqlalchemy.Column(sqlalchemy_utils.JSONType, nullable=False)


class Metric(Base, GnocchiBase):
    __tablename__ = 'metric'
    __table_args__ = (
        sqlalchemy.Index('ix_metric_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           primary_key=True)
    archive_policy = sqlalchemy.Column(
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey('archive_policy.name',
                              ondelete="RESTRICT"),
        nullable=False)
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)


class Instance(Resource):
    __tablename__ = 'instance'
    __table_args__ = (
        sqlalchemy.Index('ix_instance_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           sqlalchemy.ForeignKey('resource.id',
                                                 ondelete="CASCADE"),
                           primary_key=True)

    flavor_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    image_ref = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    host = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    server_group = sqlalchemy.Column(sqlalchemy.String(255))


class SwiftAccount(Resource):
    __tablename__ = 'swift_account'
    __table_args__ = (
        sqlalchemy.Index('ix_swift_account_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           sqlalchemy.ForeignKey('resource.id',
                                                 ondelete="CASCADE"),
                           primary_key=True)


class SQLAlchemyIndexer(indexer.IndexerDriver):
    # TODO(jd) Use stevedore instead to allow extending?
    _RESOURCE_CLASS_MAPPER = {
        'generic': Resource,
        'instance': Instance,
        'swift_account': SwiftAccount,
    }

    def __init__(self, conf):
        options.set_defaults(conf)
        self.conf = conf

    def connect(self):
        self.engine_facade = session.EngineFacade.from_config(self.conf)

    def disconnect(self):
        self.engine_facade.get_engine().dispose()

    def upgrade(self):
        engine = self.engine_facade.get_engine()
        Base.metadata.create_all(engine, checkfirst=True)

    def _resource_type_to_class(self, resource_type):
        if resource_type not in self._RESOURCE_CLASS_MAPPER:
            raise indexer.UnknownResourceType(resource_type)
        return self._RESOURCE_CLASS_MAPPER[resource_type]

    def list_archive_policies(self):
        session = self.engine_facade.get_session()
        return [dict(ap) for ap in session.query(ArchivePolicy).all()]

    def get_archive_policy(self, name):
        session = self.engine_facade.get_session()
        ap = session.query(ArchivePolicy).get(name)
        if ap:
            return dict(ap)

    def get_metric(self, uuid, details=False):
        session = self.engine_facade.get_session()
        if details:
            metric, archive_policy = session.query(
                Metric, ArchivePolicy).filter(
                    Metric.id == uuid).filter(
                        ArchivePolicy.name == Metric.archive_policy).first()
            metric['archive_policy'] = self._resource_to_dict(archive_policy)
        else:
            metric = session.query(Metric).get(uuid)

        if metric:
            return self._resource_to_dict(metric)

    def create_archive_policy(self, name, back_window, definition):
        ap = ArchivePolicy(name=name, back_window=back_window,
                           definition=definition)
        session = self.engine_facade.get_session()
        session.add(ap)
        try:
            session.flush()
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyAlreadyExists(name)
        return dict(ap)

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy):
        m = Metric(id=id,
                   created_by_user_id=created_by_user_id,
                   created_by_project_id=created_by_project_id,
                   archive_policy=archive_policy)
        session = self.engine_facade.get_session()
        session.add(m)
        session.flush()
        return self._resource_to_dict(m)

    def list_metrics(self, user_id=None, project_id=None):
        session = self.engine_facade.get_session()
        q = session.query(Metric)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        if project_id is not None:
            q = q.filter(project_id=project_id)
        return [self._resource_to_dict(m) for m in q.all()]

    def create_resource(self, resource_type, id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        resource_cls = self._resource_type_to_class(resource_type)
        if (started_at is not None
           and ended_at is not None
           and started_at > ended_at):
            raise ValueError("Start timestamp cannot be after end timestamp")
        r = resource_cls(
            id=id,
            type=resource_type,
            created_by_user_id=created_by_user_id,
            created_by_project_id=created_by_project_id,
            user_id=user_id,
            project_id=project_id,
            started_at=started_at,
            ended_at=ended_at,
            **kwargs)
        session = self.engine_facade.get_session()
        with session.begin():
            session.add(r)
            try:
                session.flush()
            except exception.DBDuplicateEntry:
                raise indexer.ResourceAlreadyExists(id)
            except exception.DBReferenceError as ex:
                raise indexer.ResourceValueError(r.type,
                                                 ex.key,
                                                 getattr(r, ex.key))
            if metrics is None:
                metrics = {}
            for name, e in six.iteritems(metrics):
                session.add(ResourceMetric(resource_id=r.id,
                                           metric_id=e,
                                           name=name))
                try:
                    session.flush()
                except exception.DBReferenceError as ex:
                    if ex.table == 'resource_metric':
                        if ex.key == 'metric_id':
                            raise indexer.NoSuchMetric(e)
                        if ex.key == 'resource_id':
                            raise indexer.NoSuchResource(r.id)
                    raise

        return self._resource_to_dict(r)

    @staticmethod
    def _resource_to_dict(resource):
        r = dict(resource)
        # FIXME(jd) Convert UUID to string; would be better if Pecan JSON
        # serializer could be patched to handle that.
        for k, v in six.iteritems(r):
            if isinstance(v, uuid.UUID):
                r[k] = six.text_type(v)
        if isinstance(resource, Resource):
            r['metrics'] = dict((k.name, str(k.metric_id))
                                for k in resource.metrics)
        return r

    def update_resource(self, resource_type,
                        uuid, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        **kwargs):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        with session.begin():
            q = session.query(
                resource_cls).filter(
                    resource_cls.id == uuid)
            r = q.first()
            if r is None:
                raise indexer.NoSuchResource(uuid)

            if ended_at is not _marker:
                # NOTE(jd) Could be better to have check in the db for that so
                # we can just run the UPDATE
                if r.started_at is not None and ended_at is not None:
                    # Convert to UTC because we store in UTC :(
                    ended_at = timeutils.normalize_time(ended_at)
                    if r.started_at > ended_at:
                        raise ValueError(
                            "Start timestamp cannot be after end timestamp")
                r.ended_at = ended_at

            if kwargs:
                for attribute, value in six.iteritems(kwargs):
                    if hasattr(r, attribute):
                        setattr(r, attribute, value)
                    else:
                        raise indexer.ResourceAttributeError(
                            r.type, attribute)

            if metrics is not _marker:
                if not append_metrics:
                    session.query(ResourceMetric).filter(
                        ResourceMetric.resource_id == uuid).delete()
                for name, eid in six.iteritems(metrics):
                    with session.begin(subtransactions=True):
                        session.add(ResourceMetric(resource_id=uuid,
                                                   metric_id=eid,
                                                   name=name))
                        try:
                            session.flush()
                        except exception.DBReferenceError as e:
                            if e.key == 'metric_id':
                                raise indexer.NoSuchMetric(eid)
                            if e.key == 'resource_id':
                                raise indexer.NoSuchResource(uuid)
                            raise
                        except exception.DBDuplicateEntry as e:
                            raise indexer.NamedMetricAlreadyExists(name)

        return self._resource_to_dict(r)

    def delete_resource(self, id):
        session = self.engine_facade.get_session()
        if session.query(Resource).filter(Resource.id == id).delete() == 0:
            raise indexer.NoSuchResource(id)

    def get_resource(self, resource_type, uuid):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls).filter(
                resource_cls.id == uuid)
        r = q.first()
        if r:
            return self._resource_to_dict(r)

    def list_resources(self, resource_type='generic',
                       started_after=None,
                       ended_before=None,
                       attributes_filter=None,
                       details=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls)
        if started_after is not None:
            q = q.filter(resource_cls.started_at >= started_after)
        if ended_before is not None:
            q = q.filter(resource_cls.ended_at < ended_before)
        if attributes_filter is not None:
            for attribute, value in six.iteritems(attributes_filter):
                try:
                    q = q.filter(getattr(resource_cls, attribute) == value)
                except AttributeError:
                    raise indexer.ResourceAttributeError(
                        resource_type, attribute)
        if details:
            grouped_by_type = itertools.groupby(q.all(),
                                                operator.attrgetter('type'))
            all_resources = []
            for type, resources in grouped_by_type:
                if type == 'generic':
                    # No need for a second query
                    all_resources.extend(resources)
                else:
                    resources_ids = [r.id for r in resources]
                    all_resources.extend(
                        session.query(
                            self._RESOURCE_CLASS_MAPPER[type]).filter(
                                self._RESOURCE_CLASS_MAPPER[type].id.in_(
                                    resources_ids)).all())
        else:
            all_resources = q.all()

        return [self._resource_to_dict(r) for r in all_resources]

    def delete_metric(self, id):
        session = self.engine_facade.get_session()
        session.query(Metric).filter(Metric.id == id).delete()
        session.flush()
