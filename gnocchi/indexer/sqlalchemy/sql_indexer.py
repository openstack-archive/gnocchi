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
import copy
import fnmatch
import itertools
import operator
import uuid

from oslo.db import exception
from oslo.db import options
from oslo.db.sqlalchemy import session
from oslo.utils import timeutils
import six
import sqlalchemy

from gnocchi import indexer
from gnocchi.indexer.sqlalchemy import models


Base = models.Base

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}

_marker = indexer._marker


class SQLAlchemyIndexer(indexer.IndexerDriver):
    # TODO(jd) Use stevedore instead to allow extending?
    _RESOURCE_CLASS_MAPPER = {
        'generic': models.Resource,
        'instance': models.Instance,
        'swift_account': models.SwiftAccount,
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
        return [dict(ap) for ap in session.query(models.ArchivePolicy).all()]

    def get_archive_policy(self, name, session=None):
        session = session or self.engine_facade.get_session()
        ap = session.query(models.ArchivePolicy).get(name)
        if ap:
            ap_dict = dict(ap)
            ap_dict['rules'] = [dict(rule) for rule in ap.rules]
            return ap_dict

    def delete_archive_policy(self, name):
        session = self.engine_facade.get_session()
        try:
            if session.query(models.ArchivePolicy).filter(
                    models.ArchivePolicy.name == name).delete() == 0:
                raise indexer.NoSuchArchivePolicy(name)
        except exception.DBError as e:
            # TODO(jd) Add an exception in oslo.db to match foreign key
            # violations
            if isinstance(e.inner_exception, sqlalchemy.exc.IntegrityError):
                raise indexer.ArchivePolicyInUse(name)

    def get_metrics(self, uuids, details=False):
        session = self.engine_facade.get_session()
        query = session.query(models.Metric).filter(
            models.Metric.id.in_(uuids))
        if details:
            query = query.options(sqlalchemy.orm.joinedload(
                models.Metric.archive_policy))
            metrics = []
            for m in query:
                metric = self._resource_to_dict(m)
                metric['archive_policy'] = self._resource_to_dict(
                    m.archive_policy)
                del metric['archive_policy_name']
                metrics.append(metric)
            return metrics

        return list(map(self._resource_to_dict, query.all()))

    def create_archive_policy(self, archive_policy):
        ap = models.ArchivePolicy(
            name=archive_policy.name, back_window=archive_policy.back_window,
            definition=[d.to_dict() for d in archive_policy.definition])
        session = self.engine_facade.get_session()
        with session.begin():
            session.add(ap)
            for rule in archive_policy.rules:
                apr = models.ArchivePolicyRule(
                    id=uuid.uuid4(), filter=rule.filter, value=rule.value,
                    archive_policy_name=archive_policy.name)
                session.add(apr)
            try:
                session.flush()
            except exception.DBDuplicateEntry:
                raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)
            ap = self.get_archive_policy(archive_policy.name, session)
        return ap

    def rewrite_archive_policy_rules(self, name, rules):
        session = self.engine_facade.get_session()
        with session.begin():
            q = session.query(
                models.ArchivePolicy).filter(
                    models.ArchivePolicy.name == name)
            ap = q.first()

            if ap is None:
                raise indexer.NoSuchResource(name)

            session.query(models.Resource).filter(
                models.ArchivePolicyRule.archive_policy == name).delete()

            for rule in rules:
                apr = models.ArchivePolicyRule(archive_policy_name=name,
                                               id=uuid.uuid4(),
                                               filter=rule.filter,
                                               value=rule.value)
                session.add(apr)

            session.flush()

    def _get_metric_ap(self, metric_name, resource_attributes):
        filters = copy.deepcopy(resource_attributes)
        filters.pop('metrics')
        filters['metric_name'] = metric_name

        policies = self.list_archive_policies()

        # fixme(dbelova): make policies prioritized somehow, at least via order
        # of their creation! Otherwise we might have rules for low AP
        # "metric_name": "*" and "metric_name": "cpu_util" for, let's say, high
        # AP -> we definitely want high AP to be applied to cpu_util
        # measurements

        for ap in policies:
            policy_name = ap['name']
            for rule, value in six.iteritems(ap['rules']):
                if rule in filters and fnmatch.fnmatch(value, filters[rule]):
                    return policy_name

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      name=None, resource_id=None, resource_type=None):

        resource_attributes = {}
        if resource_id is not None and resource_type is not None:
            resource_attributes = self._resource_to_dict(
                self.get_resource(resource_type, resource_id))

        archive_policy_name = self._get_metric_ap(name, resource_attributes)

        m = models.Metric(id=id,
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
        q = session.query(models.Metric)
        if user_id is not None:
            q = q.filter(models.Metric.created_by_user_id == user_id)
        if project_id is not None:
            q = q.filter(models.Metric.created_by_project_id == project_id)
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
            if metrics is not None:
                self._set_metrics_for_resource(session, id,
                                               created_by_user_id,
                                               created_by_project_id,
                                               metrics)

        return self._resource_to_dict(r, with_metrics=True)

    @staticmethod
    def _resource_to_dict(resource, with_metrics=False):
        r = dict(resource)
        # FIXME(jd) Convert UUID to string; would be better if Pecan JSON
        # serializer could be patched to handle that.
        for k, v in six.iteritems(r):
            if isinstance(v, uuid.UUID):
                r[k] = six.text_type(v)
        if with_metrics and isinstance(resource, models.Resource):
            r['metrics'] = dict((m['name'], six.text_type(m['id']))
                                for m in resource.metrics)
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
                    session.query(models.Metric).filter(
                        models.Metric.resource_id == uuid).update(
                            {"resource_id": None})
                self._set_metrics_for_resource(session, uuid,
                                               r.created_by_user_id,
                                               r.created_by_project_id,
                                               metrics)

        return self._resource_to_dict(r, with_metrics=True)

    def _set_metrics_for_resource(self, session, resource_id,
                                  user_id, project_id, metrics):
        for name, metric_id in six.iteritems(metrics):
            try:
                update = session.query(models.Metric).filter(
                    models.Metric.id == metric_id,
                    models.Metric.created_by_user_id == user_id,
                    models.Metric.created_by_project_id == project_id).update(
                        {"resource_id": resource_id, "name": name})
            except exception.DBDuplicateEntry:
                raise indexer.NamedMetricAlreadyExists(name)
            if update == 0:
                raise indexer.NoSuchMetric(metric_id)

    def delete_resource(self, id):
        session = self.engine_facade.get_session()
        if session.query(models.Resource).filter(
                models.Resource.id == id).delete() == 0:
            raise indexer.NoSuchResource(id)

    def get_resource(self, resource_type, uuid, with_metrics=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls).filter(
                resource_cls.id == uuid)
        if with_metrics:
            q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))
        r = q.first()
        if r:
            return self._resource_to_dict(r, with_metrics)

    def list_resources(self, resource_type='generic',
                       started_after=None,
                       ended_before=None,
                       attributes_filter=None,
                       details=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls)
        # Always include metrics
        q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))
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

        return [self._resource_to_dict(r, with_metrics=True)
                for r in all_resources]

    def delete_metric(self, id):
        session = self.engine_facade.get_session()
        session.query(models.Metric).filter(models.Metric.id == id).delete()
        session.flush()
