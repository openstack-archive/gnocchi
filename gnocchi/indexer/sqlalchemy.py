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
import itertools
import operator
import uuid

from oslo_db import exception
from oslo_db.sqlalchemy import session
from oslo_utils import timeutils
import six
import sqlalchemy
from stevedore import extension

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy_base as base
from gnocchi import utils

Base = base.Base
Metric = base.Metric
ArchivePolicy = base.ArchivePolicy
Resource = base.Resource
ResourceId = base.ResourceId

_marker = indexer._marker


class SQLAlchemyIndexer(indexer.IndexerDriver):
    resources = extension.ExtensionManager('gnocchi.indexer.resources')

    _RESOURCE_CLASS_MAPPER = {ext.name: ext.plugin
                              for ext in resources.extensions}

    def __init__(self, conf):
        conf.set_override("connection", conf.indexer.url, "database")
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

    @staticmethod
    def _fixup_created_by_uuid(obj):
        # FIXME(sileht): so weird, sqlachemy_utils.UUIDTYPE try to convert any
        # input to a UUID to write it in db but don't update the orm object
        # if the object doesn't come from the database
        if (obj.created_by_user_id
           and not isinstance(obj.created_by_user_id, uuid.UUID)):
            obj.created_by_user_id = uuid.UUID(obj.created_by_user_id)
        if (obj.created_by_project_id
           and not isinstance(obj.created_by_project_id, uuid.UUID)):
            obj.created_by_project_id = uuid.UUID(obj.created_by_project_id)

    def list_archive_policies(self):
        session = self.engine_facade.get_session()
        return [dict(ap) for ap in session.query(ArchivePolicy).all()]

    def get_archive_policy(self, name):
        session = self.engine_facade.get_session()
        ap = session.query(ArchivePolicy).get(name)
        if ap:
            return dict(ap)

    def delete_archive_policy(self, name):
        session = self.engine_facade.get_session()
        try:
            if session.query(ArchivePolicy).filter(
                    ArchivePolicy.name == name).delete() == 0:
                raise indexer.NoSuchArchivePolicy(name)
        except exception.DBError as e:
            # TODO(jd) Add an exception in oslo.db to match foreign key
            # violations
            if isinstance(e.inner_exception, sqlalchemy.exc.IntegrityError):
                raise indexer.ArchivePolicyInUse(name)

    def _get_metrics(self, query, details=False):
        if details:
            query = query.options(sqlalchemy.orm.joinedload(
                Metric.archive_policy))
            metrics = []
            for m in query:
                metric = self._resource_to_dict(m)
                metric['archive_policy'] = self._resource_to_dict(
                    m.archive_policy)
                del metric['archive_policy_name']
                metrics.append(metric)
            return metrics

        return list(map(self._resource_to_dict, query.all()))

    def get_metrics(self, uuids, details=False):
        if not uuids:
            return []
        session = self.engine_facade.get_session()
        query = session.query(Metric).filter(Metric.id.in_(uuids))
        return self._get_metrics(query, details=details)

    def create_archive_policy(self, archive_policy):
        ap = ArchivePolicy(
            name=archive_policy.name,
            back_window=archive_policy.back_window,
            definition=[d.to_dict()
                        for d in archive_policy.definition],
            aggregation_methods=list(archive_policy.aggregation_methods),
        )
        session = self.engine_facade.get_session()
        session.add(ap)
        try:
            session.flush()
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)
        return dict(ap)

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name,
                      name=None, resource_id=None):
        m = Metric(id=id,
                   created_by_user_id=created_by_user_id,
                   created_by_project_id=created_by_project_id,
                   archive_policy_name=archive_policy_name,
                   name=name,
                   resource_id=resource_id)
        session = self.engine_facade.get_session()
        session.add(m)
        session.flush()
        return self._resource_to_dict(m)

    def list_metrics(self, user_id=None, project_id=None):
        session = self.engine_facade.get_session()
        q = session.query(Metric)
        if user_id is not None:
            q = q.filter(Metric.created_by_user_id == user_id)
        if project_id is not None:
            q = q.filter(Metric.created_by_project_id == project_id)
        return [self._resource_to_dict(m) for m in q.all()]

    def create_resource(self, resource_type, resource_id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):

        resource_cls = self._resource_type_to_class(resource_type)
        if (started_at is not None
           and ended_at is not None
           and started_at > ended_at):
            raise ValueError("Start timestamp cannot be after end timestamp")

        if resource_type != "generic":
            # NOTE(sileht): To be able to keep the resource attributes history
            # AND allow to "patch" a resource, we have to be able to
            # duplicate a row and then update this duplicate in an atomic maner
            # To do that in update_resource use "insert from select" scheme
            # but to identity the latest revision of the attributes of the
            # extra resource table, we have to put the id into this table too.
            kwargs['eid'] = resource_id

        r = resource_cls(
            id=resource_id,
            type=resource_type,
            created_by_user_id=created_by_user_id,
            created_by_project_id=created_by_project_id,
            user_id=user_id,
            project_id=project_id,
            started_at=started_at,
            ended_at=ended_at,
            **kwargs)
        self._fixup_created_by_uuid(r)

        session = self.engine_facade.get_session()
        with session.begin():
            session.add(ResourceId(id=resource_id))
            try:
                session.flush()
            except exception.DBDuplicateEntry:
                raise indexer.ResourceAlreadyExists(resource_id)

            session.add(r)
            try:
                session.flush()
            except exception.DBReferenceError as ex:
                raise indexer.ResourceValueError(r.type,
                                                 ex.key,
                                                 getattr(r, ex.key))
            if metrics is not None:
                self._set_metrics_for_resource(session, resource_id,
                                               created_by_user_id,
                                               created_by_project_id,
                                               metrics)

        return self._resource_to_dict(r, with_metrics=True)

    @staticmethod
    def _resource_to_dict(resource, with_metrics=False):
        r = dict(resource)
        if isinstance(resource, Resource):
            if 'eid' in r:
                del r['eid']
            del r['seq']
            if with_metrics:
                r['metrics'] = dict((m['name'], six.text_type(m['id']))
                                    for m in resource.metrics)
        return r

    @staticmethod
    def _build_select_names(columns, kwargs):
        names = []
        for col in columns:
            if col.name in kwargs:
                names.append(
                    sqlalchemy.sql.expression.bindparam(
                        col.name, kwargs[col.name], col.type))
            else:
                names.append(col)
        return names

    def update_resource(self, resource_type,
                        resource_id, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        creates=False,
                        **kwargs):

        kwargs['updated_at'] = datetime.datetime.utcnow()
        if ended_at is not _marker:
            if ended_at is None:
                kwargs['ended_at'] = None
            else:
                # Convert to UTC because we store in UTC :(
                kwargs['ended_at'] = timeutils.normalize_time(ended_at)

        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        with session.begin():
            base_table = sqlalchemy.inspect(Resource).local_table
            extra_table = sqlalchemy.inspect(resource_cls).local_table
            base_columns = [c for c in base_table.c
                            if c.name != "seq" and
                            (not creates or c.name in kwargs)]
            extra_columns = [c for c in extra_table.c
                             if not creates or c.name in kwargs]
            columns_names = ([c.name for c in base_columns] +
                             [c.name for c in extra_columns])
            invalid_attributes = [attr for attr in kwargs
                                  if attr not in columns_names]
            if invalid_attributes:
                raise indexer.ResourceAttributeError(
                    resource_type, invalid_attributes[0])

            q = session.query(*self._build_select_names(base_columns, kwargs))
            q = q.filter(Resource.id == resource_id)
            q = q.order_by(Resource.seq.desc()).limit(1)

            insert_stmt = base_table.insert().from_select(base_columns, q)
            try:
                result = session.execute(insert_stmt.returning(
                    base_table.c.seq)).fetchone()
            except sqlalchemy.exc.CompileError:
                # Fallback to inserted_primary_key
                resource_seq = session.execute(
                    insert_stmt).inserted_primary_key[0]
                if resource_seq is None:
                    # Fallback to select
                    q = session.query(Resource.seq)
                    q = q.filter(Resource.id == resource_id)
                    q = q.order_by(Resource.seq.desc()).limit(1)
                    r = q.first()
                    if r is not None:
                        resource_seq = r.seq
            else:
                resource_seq = result[0] if result is not None else None

            if not resource_seq:
                raise indexer.NoSuchResource(resource_id)

            if resource_cls is not Resource:
                kwargs['seq'] = resource_seq
                q = session.query(*self._build_select_names(
                    extra_columns, kwargs))
                q = q.filter(resource_cls.eid == resource_id)
                q = q.order_by(resource_cls.seq.desc()).limit(1)
                session.execute(extra_table.insert().from_select(
                    extra_columns, q))

            # Requery the whole resource, this query is concurrency safe
            # because
            q = session.query(resource_cls).filter(
                Resource.seq == resource_seq)
            r = q.first()
            if r is None:
                raise indexer.NoSuchResource(resource_id)

            if (ended_at is not _marker
                    and r.started_at is not None and r.ended_at is not None
                    and r.started_at > r.ended_at):
                # NOTE(jd) Could be better to have check in the db for that so
                # we can just run the INSERT
                raise ValueError(
                    "Start timestamp cannot be after end timestamp")

            if metrics is not _marker:
                if not append_metrics:
                    session.query(Metric).filter(
                        Metric.resource_id == resource_id).update(
                            {"resource_id": None})
                self._set_metrics_for_resource(session, r.id,
                                               r.created_by_user_id,
                                               r.created_by_project_id,
                                               metrics)

        return self._resource_to_dict(r, with_metrics=True)

    @staticmethod
    def _set_metrics_for_resource(session, resource_id,
                                  user_id, project_id, metrics):
        for name, metric_id in six.iteritems(metrics):
            try:
                update = session.query(Metric).filter(
                    Metric.id == metric_id,
                    Metric.created_by_user_id == user_id,
                    Metric.created_by_project_id == project_id).update(
                        {"resource_id": resource_id, "name": name})
            except exception.DBDuplicateEntry:
                raise indexer.NamedMetricAlreadyExists(name)
            if update == 0:
                raise indexer.NoSuchMetric(metric_id)

    def delete_resource(self, resource_id, delete_metrics=None):
        session = self.engine_facade.get_session()
        with session.begin():
            qr = session.query(ResourceId).filter(
                ResourceId.id == resource_id)
            r = qr.first()
            if r is None:
                raise indexer.NoSuchResource(resource_id)

            if delete_metrics is not None:
                qm = session.query(Metric).filter(
                    Metric.resource_id == resource_id)
                delete_metrics(self._get_metrics(qm, details=True))

            qr.delete()

    def get_resource(self, resource_type, resource_id, with_metrics=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()

        q = session.query(resource_cls).filter(resource_cls.id == resource_id)
        q = q.order_by(resource_cls.seq.desc()).limit(1)
        if with_metrics:
            q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))

        r = q.first()
        if r:
            return self._resource_to_dict(r, with_metrics)

    def list_resources(self, resource_type='generic',
                       attribute_filter=None,
                       details=False, history=False):

        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()

        if history:
            q = session.query(resource_cls)
        else:
            s = sqlalchemy.select(
                [sqlalchemy.func.max(Resource.seq).label("seq")],
                group_by=Resource.id).alias()
            q = session.query(resource_cls).join(
                s, s.c.seq == resource_cls.seq)

        q = q.order_by(resource_cls.updated_at)

        if attribute_filter:
            try:
                f = QueryTransformer.build_filter(resource_cls,
                                                  attribute_filter)
            except indexer.QueryAttributeError as e:
                # NOTE(jd) The QueryAttributeError does not know about
                # resource_type, so convert it
                raise indexer.ResourceAttributeError(resource_type,
                                                     e.attribute)
            q = q.filter(f)

        # Always include metrics
        q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))

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

        return [self._resource_to_dict(r, with_metrics=True)
                for r in all_resources]

    def delete_metric(self, id):
        session = self.engine_facade.get_session()
        session.query(Metric).filter(Metric.id == id).delete()
        session.flush()


class QueryTransformer(object):
    unary_operators = {
        u"not": sqlalchemy.not_,
    }

    binary_operators = {
        u"=": operator.eq,
        u"==": operator.eq,
        u"eq": operator.eq,

        u"<": operator.lt,
        u"lt": operator.lt,

        u">": operator.gt,
        u"gt": operator.gt,

        u"<=": operator.le,
        u"≤": operator.le,
        u"le": operator.le,

        u">=": operator.ge,
        u"≥": operator.ge,
        u"ge": operator.ge,

        u"!=": operator.ne,
        u"≠": operator.ne,
        u"ne": operator.ne,

        u"in": lambda field_name, values: field_name.in_(values),

        u"like": lambda field, value: field.like(value),
    }

    multiple_operators = {
        u"or": sqlalchemy.or_,
        u"∨": sqlalchemy.or_,

        u"and": sqlalchemy.and_,
        u"∧": sqlalchemy.and_,
    }

    @classmethod
    def _handle_multiple_op(cls, table, op, nodes):
        return op(*[
            cls.build_filter(table, node)
            for node in nodes
        ])

    @classmethod
    def _handle_unary_op(cls, table, op, node):
        return op(cls.build_filter(table, node))

    @staticmethod
    def _handle_binary_op(table, op, nodes):
        try:
            field_name, value = list(nodes.items())[0]
        except Exception:
            raise indexer.QueryError()
        try:
            attr = getattr(table, field_name)
        except AttributeError:
            raise indexer.QueryAttributeError(table, field_name)

        # Convert value to the right type
        if isinstance(attr.type, base.PreciseTimestamp):
            value = utils.to_timestamp(value)

        return op(attr, value)

    @classmethod
    def build_filter(cls, table, tree):
        try:
            operator, nodes = list(tree.items())[0]
        except Exception:
            raise indexer.QueryError()

        try:
            op = cls.multiple_operators[operator]
        except KeyError:
            try:
                op = cls.binary_operators[operator]
            except KeyError:
                try:
                    op = cls.unary_operators[operator]
                except KeyError:
                    raise indexer.QueryInvalidOperator(operator)
                return cls._handle_unary_op(op, nodes)
            return cls._handle_binary_op(table, op, nodes)
        return cls._handle_multiple_op(table, op, nodes)
