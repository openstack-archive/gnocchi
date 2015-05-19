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
import copy
import uuid

from ceilometer.i18n import _
from ceilometer.storage.mongo import utils
from oslo_log import log
from oslo_utils import netutils
from oslo_utils import timeutils
import pymongo
import six

from gnocchi import indexer
from gnocchi import storage

LOG = log.getLogger(__name__)

_marker = indexer._marker


def serialize_dt(value):
    """Serializes parameter if it is datetime."""
    return value.isoformat() if hasattr(value, 'isoformat') else value


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

    def upgrade(self, nocreate=False):
        pass

    def list_archive_policies(self):
        return self.db.archive_policy.find()

    def get_archive_policy(self, name):
        ap = self.db.archive_policy.find_one({'_id': name}, {'_id': 0})
        ap.pop('in_use')
        ap['aggregation_methods'] = set(ap['aggregation_methods'])
        return ap

    def delete_archive_policy(self, name):
        ap = self.db.archive_policy.remove({'$and': [{'_id': name},
                                                     {'in_use': 0}]})
        if ap['n'] == 0:
            if self.db.archive_policy.find_one({'_id': name}) is None:
                raise indexer.NoSuchArchivePolicy(name)
            else:
                raise indexer.ArchivePolicyInUse(name)

    def get_metrics(self, uuids):
        if not uuids:
            return []
        res = []
        for k, v in six.iteritems(
                self.db.resource.find_one({'_id': 'free_metrics'},
                                          {'_id': 0})):
            if uuid.UUID('{%s}' % k) in uuids:
                res.append(
                    storage.Metric(
                        uuid.UUID('{%s}' % k), v['archive_policy_id'],
                        v['created_by_user_id'], v['created_by_project_id'],
                        v['name']
                    )
                )
        for item in self.db.resource.find():
            for k, v in six.iteritems(item.get('metrics', {})):
                if uuid.UUID('{%s}' % k) in uuids:
                    res.append(
                        storage.Metric(
                            uuid.UUID('{%s}' % k), v['archive_policy_id'],
                            v['created_by_user_id'],
                            v['created_by_project_id'], v['name'],
                            resource_id=item['_id']
                        )
                    )
        return res

    def create_archive_policy(self, archive_policy):
        ap = {
            '_id': archive_policy.name,
            'name': archive_policy.name,
            'back_window': archive_policy.back_window,
            'definition': archive_policy.definition,
            'aggregation_methods': list(archive_policy.aggregation_methods),
            'in_use': 0,
            }
        try:
            self.db.archive_policy.insert(ap)
        except pymongo.errors.DuplicateKeyError:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)

    def create_resource(self, resource_type, id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, revision_start=None, ended_at=None,
                        metrics=None, **kwargs):

        r = {'_id': id,
             'type': resource_type,
             'created_by_user_id': created_by_user_id,
             'created_by_project_id': created_by_project_id,
             'started_at': started_at or self._utcnow(),
             'revision_start': revision_start or self._utcnow(),
             'user_id': user_id,
             'project_id': project_id,
             'ended_at': ended_at,
             }
        if (started_at is not None and ended_at is not None):
            if started_at > ended_at:
                raise ValueError(
                    "Start timestamp cannot be after end timestamp")
            r['lifespan'] = (ended_at - started_at).total_seconds()
        if metrics:
            r['metrics'] = self._set_metrics_for_resource(r, metrics)
        if kwargs:
            r.update(kwargs)
        try:
           self.db.resource.insert_one(r)
        except pymongo.errors.DuplicateKeyError:
            raise indexer.ResourceAlreadyExists(id)
        return indexer.Resource(r['_id'], r['type'], r['created_by_user_id'],
                                r['created_by_project_id'],
                                user_id=r.get('user_id'),
                                project_id=r.get('project_id'),
                                started_at=r.get('started_at'),
                                revision_start=r.get('revision_start'),
                                ended_at=r.get('ended_at'),
                                metrics={v['name']: k for k, v in
                                         six.iteritems(r.get('metrics', {}))},
                                **kwargs)

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name, name=None, resource_id=None,
                      details=False):

        if self.db.archive_policy.find_and_modify(
                {'_id': archive_policy_name}, {'$inc': {'in_use': 1}}) is None:
            raise indexer.NoSuchArchivePolicy(archive_policy_name)

        if resource_id:
            self.db.resource.find_one_and_update(
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
                {'$set': {str(id): {
                    'name': name,
                    'archive_policy_id': archive_policy_name,
                    'created_by_user_id': created_by_user_id,
                    'created_by_project_id': created_by_project_id,
                    }}},
                upsert=True,
            )

        return storage.Metric(
            id, archive_policy_name, created_by_user_id, created_by_project_id,
            name, resource_id
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

        now = self._utcnow()

        r = self.db.resource.find_one({'_id': resource_id})
        rh = copy.deepcopy(r)

        if rh is None:
            raise indexer.NoSuchResource(resource_id)
        rh['revision_end'] = now
        rh['id'] = rh.pop('_id')
        self.db.history.insert(rh)
        s = {}
        if ended_at is not _marker:
            if rh['started_at'] is not None and ended_at is not None:
                # Convert to UTC because we store in UTC :(
                ended_at = timeutils.normalize_time(ended_at)
                if rh['started_at'] > ended_at:
                    raise indexer.ResourceValueError(
                        resource_type, "ended_at", ended_at)
                s['lifespan'] = (
                    (ended_at - rh['started_at']).total_seconds())
            s['ended_at'] = ended_at
        s['revision_start'] = now

        if kwargs:
            for attribute, value in six.iteritems(kwargs):
                if rh[attribute]:
                    s[attribute] = value
                else:
                    raise indexer.ResourceAttributeError(
                        rh['type'], attribute)

        if metrics is not _marker:
            if not append_metrics:
                for id, rm in six.iteritems(r['metrics']):
                    self.db.resource.find_and_modify(
                        {'_id': 'free_metrics'},
                        {'$set': {id: {
                            'name': rm['name'],
                            'archive_policy_id': rm['archive_policy_id'],
                            'created_by_user_id': r['created_by_user_id'],
                            'created_by_project_id':
                                r['created_by_project_id'],
                            }}},
                        upsert=True,
                    )
                    self.db.archive_policy.find_and_modify(
                        {'_id': rm['archive_policy_id']},
                        {'$inc': {'in_use': -1}})
                s['metrics'] = self._set_metrics_for_resource(r, metrics)
            else:
                r['metrics'].update(self._set_metrics_for_resource(r, metrics))
                s['metrics'] = r['metrics']
        r = self.db.resource.find_and_modify(
            {'_id': resource_id},
            {'$set': s},
            new=True,
        )
        return indexer.Resource(r.pop('_id'), r.pop('type'),
                                r.pop('created_by_user_id'),
                                r.pop('created_by_project_id'),
                                metrics={v['name']: k for k, v in
                                         six.iteritems(r.pop('metrics', {}))},
                                **r)

    def get_resource(self, resource_type, resource_id, with_metrics=False):
        proj = None if with_metrics else {'metrics': 0}
        r = self.db.resource.find_one({'_id': resource_id}, proj)
        if r:
            return indexer.Resource(
                r['_id'], r['type'], r['created_by_user_id'],
                r['created_by_project_id'], user_id=r.get('user_id'),
                project_id=r.get('project_id'), started_at=r.get('started_at'),
                revision_start=r.get('revision_start'),
                ended_at=r.get('ended_at'),
                metrics={v['name']: k for k, v in
                         six.iteritems(r.get('metrics', {}))}
            )

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

        return list(
            indexer.Resource(
                r['_id'], r['type'], r['created_by_user_id'],
                r['created_by_project_id'], user_id=r.get('user_id'),
                project_id=r.get('project_id'), started_at=r.get('started_at'),
                revision_start=r.get('revision_start'),
                ended_at=r.get('ended_at'),
                metrics={v['name']: k for k, v in
                         six.iteritems(r.get('metrics', {}))})
            for r in db.find(f)
        )

    def delete_resource(self, resource_id, delete_metrics=None):
        r = self.db.resource.find_one({'_id': resource_id})
        if r is None:
            raise indexer.NoSuchResource(resource_id)
        if delete_metrics is None:
            for id, metric in six.iteritems(r.get('metrics', {})):
                self.create_metric(
                    id, r['created_by_user_id'], r['created_by_project_id'],
                    metric['archive_policy_id'], metric.get('name')
                )
        else:
            delete_metrics(self.get_metrics(
                [id for id in r.get('metrics', [])]))
        self.db.resource.remove({'_id': resource_id})

    def delete_metric(self, id):
        r = self.db.resource.find_and_modify(
            {'metrics.%s' % id: {'$exists': True}},
            {'$unset': {'metrics.%s' % id: ''}})
        if r:
            self.db.archive_policy.find_and_modify(
                {'_id': r['metrics'][str(id)]['archive_policy_id']},
                {'$inc': {'in_use': -1}})

    def _set_metrics_for_resource(self, r, metrics):
        s = {}
        for name, metric_id in six.iteritems(metrics):
            m = self.db.resource.find_one(
                {'$and': [{'_id': 'free_metrics'},
                          {'%s.created_by_user_id' % metric_id:
                              r['created_by_user_id']},
                          {'%s.created_by_project_id' % metric_id:
                              r['created_by_project_id']}]},
                {'_id': 0}
                )
            if m:
                self.db.archive_policy.find_and_modify(
                    {'_id': m[str(metric_id)]['archive_policy_id']},
                    {'$inc': {'in_use': 1}})
                s[str(metric_id)] = {
                    'name': name,
                    'archive_policy_id': m[str(metric_id)]['archive_policy_id']
                }
            elif self.db.resource.find_one(
                {'$and': [{'_id': r['_id']},
                          {'metrics.%s' % metric_id: {'$exists': True}}]}):
                raise indexer.NamedMetricAlreadyExists(name)
            else:
                raise indexer.NoSuchMetric(metric_id)
        return s

    def _utcnow(self):
        time = timeutils.utcnow()
        ms = time.microsecond / 1000 * 1000
        return time.replace(microsecond=ms)


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

    @staticmethod
    def _move_negation_to_leaf(condition):
        """Moves every not operator to the leafs.

        Moving is going by applying the De Morgan rules and annihilating
        double negations.
        """
        def _apply_de_morgan(tree, negated_subtree, negated_op):
            if negated_op == "and":
                new_op = "or"
            else:
                new_op = "and"

            tree[new_op] = [{"not": child}
                            for child in negated_subtree[negated_op]]
            del tree["not"]

        def transform(subtree):
            op = subtree.keys()[0]
            if op in ["and", "or"]:
                [transform(child) for child in subtree[op]]
            elif op == "not":
                negated_tree = subtree[op]
                negated_op = negated_tree.keys()[0]
                if negated_op == "and":
                    _apply_de_morgan(subtree, negated_tree, negated_op)
                    transform(subtree)
                elif negated_op == "or":
                    _apply_de_morgan(subtree, negated_tree, negated_op)
                    transform(subtree)
                elif negated_op == "not":
                    # two consecutive not annihilates themselves
                    new_op = negated_tree.values()[0].keys()[0]
                    subtree[new_op] = negated_tree[negated_op][new_op]
                    del subtree["not"]
                    transform(subtree)

        transform(condition)

    def transform_filter(self, condition):
        # in Mongo not operator can only be applied to
        # simple expressions so we have to move every
        # not operator to the leafs of the expression tree
        self._move_negation_to_leaf(condition)
        return self._process_json_tree(condition)

    def _handle_complex_op(self, complex_op, nodes):
        element_list = []
        for node in nodes:
            element = self._process_json_tree(node)
            element_list.append(element)
        complex_operator = self.complex_operators[complex_op]
        op = {complex_operator: element_list}
        return op

    def _handle_not_op(self, negated_tree):
        # assumes that not is moved to the leaf already
        # so we are next to a leaf
        negated_op = negated_tree.keys()[0]
        negated_field = negated_tree[negated_op].keys()[0]
        value = negated_tree[negated_op][negated_field]
        if negated_op == "=":
            return {negated_field: {"$ne": value}}
        elif negated_op == "!=":
            return {negated_field: value}
        else:
            return {negated_field: {"$not":
                                    {self.operators[negated_op]: value}}}

    def _handle_simple_op(self, simple_op, nodes):
        field_name = nodes.keys()[0]
        if field_name == 'id':
            field_name = '_id'
        field_value = nodes.values()[0]

        # no operator for equal in Mongo
        if simple_op == "=":
            op = {field_name: field_value}
            return op

        operator = self.operators[simple_op]
        op = {field_name: {operator: field_value}}
        return op

    def _process_json_tree(self, condition_tree):
        operator_node = condition_tree.keys()[0]
        nodes = condition_tree.values()[0]

        if operator_node in self.complex_operators:
            return self._handle_complex_op(operator_node, nodes)

        if operator_node == "not":
            negated_tree = condition_tree[operator_node]
            return self._handle_not_op(negated_tree)

        return self._handle_simple_op(operator_node, nodes)
