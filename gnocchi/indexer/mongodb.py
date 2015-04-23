# -*- encoding: utf-8 -*-
#
# Copyright © 2015 Mirantis Inc.
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

from ceilometer.i18n import _
from ceilometer.storage.mongo import utils
from oslo_log import log
from oslo_utils import netutils
from oslo_utils import timeutils

import pymongo

from gnocchi import indexer

LOG = log.getLogger(__name__)

_marker = indexer._marker


class MongodbIndexer(indexer.IndexerDriver):

    def __init__(self, conf):
        conf.set_override("connection", conf.indexer.url, "database")
        self.conf = conf

    def connect(self):
        url = self.conf.database.connection
        splitted_url = netutils.urlsplit(url)
        LOG.info(_('Connecting to %s') % splitted_url.scheme)
        conn_options = pymongo.uri_parser.parse_uri(url)
        self.conn = pymongo.MongoClient(url)
        self.db = getattr(self.conn, conn_options['database'])

    def disconnect(self):
        self.conn.close()

    def upgrade(self):
        pass

    def list_archive_policies(self):
        return self.db.archive_policy.find()

    def get_archive_policy(self, name):
        return self.db.archive_policy.find({'_id': name})

    def delete_archive_policy(self, name):
        # we need to add check if removed policy was in database?

        self.db.archive_policy.remove({'name': name})

    def get_metrics(self, uuids, details=False):
        if not uuids:
            return []
        res = []
        for metrics in self.db.resource.find()['metrics']:
            for k, v in iteritems(metrics):
                if k in uuids:
                    res.append({k: v})
        return res

    def create_archive_policy(self, archive_policy):
        ap = {
            '_id': archive_policy.name,
            'name': archive_policy.name,
            'back_window': archive_policy.back_window,
            'definition': archive_policy.definition,
            'aggregation_methods': list(archive_policy.aggregation_methods),
        }
        try:
            self.db.archive_policy.insert(ap)
        except pymongo.errors.DuplicateKeyError:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)

    def create_resource(self, resource_type, id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        r_set = {'type': resource_type,
                 'user_id': user_id,
                 'project_id': project_id,
                 'started_at': started_at,
                 'ended_at': ended_at,
                 }
        if (started_at is not None and ended_at is not None):
            if started_at > ended_at:
                raise ValueError("Start timestamp cannot be after end timestamp")
            r_set['lifespan'] = (ended_at - started_at).total_seconds()
        if metrics:
            r_set['metrics'] = self._set_metrics_for_resource(r, metrics)
        if kwargs:
            r_set.update(kwargs)
        try:
            r = self.db.resource.find_and_modify(
                {'$and': [{'_id': id},
                          {'created_by_user_id': created_by_user_id},
                          {'created_by_project_id': created_by_project_id}]},
                {'$set': r_set,
                 '$setOnInsert': {
                     '_id': id,
                     'created_by_user_id': created_by_user_id,
                     'created_by_project_id': created_by_project_id,
                     }},
                upsert=True,
                new=True,
            )
        except pymongo.errors.DuplicateKeyError:
            raise indexer.ResourceAlreadyExists(id)
        return r

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name, name=None, resource_id=None,
                      details=False):

        if resource_id:
            self.db.resource.find_and_modify(
                {'$and': [{'_id': resource_id},
                          {'created_by_user_id': created_by_user_id},
                          {'created_by_project_id': created_by_project_id},
                          ]},
                {'$set': {
                    'metrics.%s' % id: {
                        'name': name,
                        'archive_policy_id': archive_policy_name}
                    },
                 '$setOnInsert': {
                    '_id': resource_id,
                    'created_by_user_id': created_by_user_id,
                    'created_by_project_id': created_by_project_id,
                    }
                 },
                upsert=True,
            )
        else:
            self.db.resource.find_and_modify(
                {'_id': 'free_metrics'},
                {'$set': {id: {
                    'name': name,
                    'archive_policy_name': archive_policy_name,
                    'created_by_user_id': created_by_user_id,
                    'created_by_project_id': created_by_project_id,
                }}},
                upsert=True,
                new=True,
            )

    def list_metrics(self, user_id=None, project_id=None):
        if user_id and project_id:
            q = {'$and': [{'created_by_user_id': user_id},
                          {'created_by_project_id': project_id}]}
        elif user_id or project_id:
            q = ({'created_by_user_id': user_id} if user_id
                 else {'created_by_project_id': project_id})
        else:
            q = {}
        metrics = {}
        for metric in self.db.resource.find(q, {'_id': 0, 'metrics': 1}):
            metrics.update(metric['metrics'])
        return metrics

    def update_resource(self, resource_type,
                        resource_id, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        **kwargs):

        now = timeutils.utcnow()

        r = self.db.resource.find_one({'_id': resource_id})

        if r is None:
            raise indexer.NoSuchResource(resource_id)
        r['revision_end'] = now
        self.db.history.insert(r)
        s = {}
        if ended_at is not _marker:
                if r['started_at'] is not None and ended_at is not None:
                    # Convert to UTC because we store in UTC :(
                    ended_at = timeutils.normalize_time(ended_at)
                    if r.started_at > ended_at:
                        raise ValueError(
                            "Start timestamp cannot be after end timestamp")
                    s['lifespan'] = (
                        (ended_at - r['started_at']).total_seconds())
                s['ended_at'] = ended_at
        s['revision_start'] = now

        if kwargs:
            for attribute, value in six.iteritems(kwargs):
                if r.get[attribute]:
                    s[attribute] = value
                else:
                    raise indexer.ResourceAttributeError(
                        r['type'], attribute)

        if metrics is not _marker:
            if not append_metrics:
                s['metrics'] = self._set_metrics_for_resource(r, metrics)
            else:
                r['metrics'].update(self._set_metrics_for_resource(r, metrics))
                s['metrics'] = r['metrics']
        self.db.resource.find_and_modify(
            {'_id': resource_id},
            {'$set': s},
        )

    def get_resource(self, resource_type, resource_id, with_metrics=False):
        proj = None if with_metrics else {'metrics': 0}
        return self.db.resource.find_one({'_id': resource_id}, proj)

    def list_resources(self, resource_type='generic',
                       attribute_filter=None,
                       details=False,
                       history=False):

        db = self.db.history if history else self.db.resource

        if attribute_filter:
            f = {'$and': [{'type': resource_type},
                          QueryTransformer().transform_filter(attribute_filter)
                          ]
                 }

        all_resources = list(i for i in db.find(f))

        return all_resources

    def delete_resource(self, resource_id, delete_metrics=None):
        self.db.resource.remove({'_id': resource_id})

    def delete_metric(self, id):
        self.db.resource.update({'metrics._id': id},
                                {'$pull': {'metrics._id': id}},
                                {'multi': True})

    def _set_metrics_for_resource(self, r, metrics):
        s = {}
        for name, metric_id in six.iteritems(metrics):
                m = self.db.resource.find_one(
                    {'$and': [{'_id': 'free_metrics'},
                              {'create_by_user_id': r['create_by_user_id']},
                              {'create_by_project_id':
                                  r['create_by_project_id']}]},
                    {'_id': 0})
                if metric_id in m:
                    s[metric_id] = {
                        'name': name,
                        'archive_policy': m[metric_id]['archive_policy']
                    }
        return s


class QueryTransformer(utils.QueryTransformer):
    operators = {
        u"=": u"$eq",
        u"==": u"$eq",
        u"eq": u"$eq",

        u"<": u"$lt",
        u"lt": u"$lt",

        u">": u"$gt",
        u"gt": u"$gt",

        u"<=": u"$lte",
        u"≤": u"$lte",
        u"le": u"$lte",

        u">=": u"$gte",
        u"≥": u"$gte",
        u"ge": u"$gte",

        u"!=": u"$ne",
        u"≠": u"$ne",
        u"ne": u"$ne",

        u"in": u"$in",

        u"like": u"$regex",
    }

    complex_operators = {
        u"or": u"$or",
        u"∨": u"$or",

        u"and": u"$and",
        u"∧": u"$and",
    }

