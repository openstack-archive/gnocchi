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
import datetime
import uuid

import bson.code
from ceilometer.i18n import _
from ceilometer.storage.mongo import utils as mutils
from oslo_log import log
from oslo_utils import netutils
import pymongo
import six
from stevedore import extension

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import storage
from gnocchi import utils

LOG = log.getLogger(__name__)

_marker = indexer._marker


def serialize_dt(value):
    """Serializes parameter if it is datetime."""
    return value.isoformat() if hasattr(value, 'isoformat') else value


class MongodbIndexer(indexer.IndexerDriver):

    EMIT_POLICY_IN_USE = bson.code.Code("""
        function () {
            for (var key in this%(metrics)s) {
                var metric = this%(metrics)s[key];
                if (metric.archive_policy_id == \"%(name)s\")
                    emit(this._id, metric.archive_policy_id);
            }
        }
    """)
    CONDITION = bson.code.Code("""
        %(this)s.created_by_user_id == \"%(user_id)s\" %(op)s
        %(this)s.created_by_project_id == \"%(project_id)s\"
    """)
    EMIT_METRICS = bson.code.Code("""
        function () {
            if (%s)
            emit(this.metrics);
            }
        }
    """)
    EMIT_FREE_METRICS = bson.code.Code("""
        function () {
            for(var key in this) {
                if (%s)
                emit(key, this.key);
            }
        }
    """)

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

    def _ap_def(self, definition):
        return [archive_policy.ArchivePolicyItem(**v) for v in definition]

    def get_archive_policy(self, name):
        ap = self.db.archive_policy.find_one({'_id': name}, {'_id': 0})
        ap['aggregation_methods'] = set(ap['aggregation_methods'])
        return archive_policy.ArchivePolicy(
            definition=self._ap_def(ap.pop('definition')), **ap)

    def delete_archive_policy(self, name):
        try:
            for i in ['.metrics', '']:
                emit = self.EMIT_POLICY_IN_USE % {'metrics': i, 'name': name}
                ap = self.db.resource.map_reduce(
                    emit,
                    "",
                    {'inline': 1}
                )
                if ap['counts']['emit'] > 0:
                    raise indexer.ArchivePolicyInUse(name)
        except pymongo.errors.OperationFailure:
            pass
        if self.db.archive_policy.remove({'_id': name})['n'] == 0:
            raise indexer.NoSuchArchivePolicy(name)

    def _archive_policy_model(self, name):
        ap = self.db.archive_policy.find_one({'_id': name}, {'_id': 0})
        return archive_policy.ArchivePolicy(
            definition=self._ap_def(ap.pop('definition')), **ap)

    def get_metrics(self, uuids):
        if not uuids:
            return []
        res = []
        for k, v in six.iteritems(
                self.db.resource.find_one({'_id': 'free_metrics'},
                                          {'_id': 0}) or {}):
            if uuid.UUID(k) in uuids:
                res.append(
                    storage.Metric(
                        uuid.UUID(k),
                        self._archive_policy_model(v['archive_policy_id']),
                        v['created_by_user_id'], v['created_by_project_id'],
                        v['name']
                    )
                )
        for item in self.db.resource.find():
            for k, v in six.iteritems(item.get('metrics', {})):
                if uuid.UUID(k) in uuids:
                    res.append(
                        storage.Metric(
                            uuid.UUID(k),
                            self._archive_policy_model(v['archive_policy_id']),
                            item['created_by_user_id'],
                            item['created_by_project_id'], name=v['name'],
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
            }
        try:
            self.db.archive_policy.insert(ap)
        except pymongo.errors.DuplicateKeyError:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)

    def _add_resource_schema(self, resource_type):
        if self.db.resource_schema.find_one({'_id': resource_type}):
            return
        resources = (extension.ExtensionManager('gnocchi.indexer.resources').
                     extensions)
        attr = ['id', 'type', 'created_by_user_id', 'created_by_project_id',
                'user_id', 'project_id', 'started_at', 'ended_at',
                'resource_id', 'lifespan', 'revision_start', 'revision_end',
                ]
        for r in resources:
            if r.name == resource_type:
                attr.extend([i for i in r.plugin.__dict__
                             if not i.startswith('_')])
                return self.db.resource_schema.insert(
                    {'_id': resource_type,
                     'attributes': attr}
                )

    def create_resource(self, resource_type, id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, revision_start=None, ended_at=None,
                        metrics=None, **kwargs):

        self._add_resource_schema(resource_type)

        r = {'_id': id,
             'id': id,
             'type': resource_type,
             'created_by_user_id': created_by_user_id,
             'created_by_project_id': created_by_project_id,
             'started_at': (_to_timestamp(started_at) if started_at
                            else self._utcnow()),
             'revision_start': (_to_timestamp(revision_start)
                                if revision_start else self._utcnow()),
             'user_id': user_id,
             'project_id': project_id,
             'ended_at': ended_at,
             }
        if (started_at is not None and ended_at is not None):
            if started_at > ended_at:
                raise ValueError(
                    "Start timestamp cannot be after end timestamp")
            r['lifespan'] = (ended_at - started_at).total_seconds()
        if metrics is not None:
            r['metrics'] = self._set_metrics_for_resource(r, metrics)
        if kwargs:
            r.update(kwargs)
        try:
            self.db.resource.insert(r)
        except pymongo.errors.DuplicateKeyError:
            raise indexer.ResourceAlreadyExists(id)
        return indexer.Resource(r['id'], r['type'], r['created_by_user_id'],
                                r['created_by_project_id'],
                                user_id=r.get('user_id'),
                                project_id=r.get('project_id'),
                                started_at=r.get('started_at'),
                                ended_at=r.get('ended_at'),
                                revision_start=r.get('revision_start'),
                                metrics=[storage.Metric(
                                    uuid.UUID(k),
                                    self._archive_policy_model(
                                        v['archive_policy_id']),
                                    name=v['name']) for k, v in
                                         six.iteritems(r.get('metrics', {}))],
                                **kwargs)

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name, name=None, resource_id=None,
                      details=False):

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
                     'id': resource_id,
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
            id, self._archive_policy_model(archive_policy_name),
            created_by_user_id, created_by_project_id, name, resource_id
        )

    def list_metrics(self, user_id=None, project_id=None, details=False,
                     **kwargs):
        metrics = []
        if kwargs:
            r_id = (kwargs['resource_id'] if
                    isinstance(kwargs['resource_id'], uuid.UUID) else
                    uuid.UUID(kwargs['resource_id']))
            r = self.db.resource.find_one({'_id': r_id})
            for m_id, m_value in six.iteritems(r['metrics']):
                if m_value['name'] == kwargs['name']:
                    if details:
                        metrics.append(
                            storage.Metric(
                                uuid.UUID(m_id),
                                self._archive_policy_model(
                                    m_value['archive_policy_id']),
                                created_by_user_id=r['created_by_user_id'],
                                created_by_project_id=r['created_by_project_id'],
                                name=m_value['name'],
                                resource_id=r_id
                            )
                        )
                    else:
                        metrics.append(
                            storage.Metric(
                                uuid.UUID(m_id),
                                m_value['archive_policy_id'],
                                created_by_user_id=r['created_by_user_id'],
                                created_by_project_id=r['created_by_project_id'],
                                name=m_value['name'],
                                resource_id=r_id
                            )
                        )
        q = {'user_id': user_id, 'project_id': project_id}
        if user_id and project_id:
            q['op'] = 'and'
        elif user_id or project_id:
            q['op'] = 'or'
        else:
            for r in self.db.resource.find():
                if r['_id'] == 'free_metrics':
                    r.pop('_id')
                    for m_id, m_v in six.iteritems(r):
                        metrics.append(
                            storage.Metric(
                                uuid.UUID(m_id),
                                m_v['archive_policy_id'],
                                created_by_user_id=m_v['created_by_user_id'],
                                created_by_project_id=m_v['created_by_project_id'],
                                name=m_v['name'],
                            )
                        )
                for m_id, m_v in six.iteritems(r.get('metrics', {})):
                    metrics.append(
                        storage.Metric(
                            uuid.UUID(m_id),
                            m_v['archive_policy_id'],
                            created_by_user_id=r['created_by_user_id'],
                            created_by_project_id=r['created_by_project_id'],
                            name=m_v['name'],
                            resource_id=r['id']
                        )

                    )
            return metrics
        q['this'] = 'this'
        f = self.EMIT_METRICS % (self.CONDITION % q)
        m = self.db.resource.map_reduce(f, "", {'inline': 1})['result']

        q['this'] = 'key'
        f = self.EMIT_FREE_METRICS % (self.CONDITION % q)
        m = self.db.resource.map_reduce(f, "", {'inline': 1})['result']
        cond = self.CONDITION % q

        return metrics

    def update_resource(self, resource_type,
                        resource_id, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        **kwargs):
        now = self._utcnow()

        if not isinstance(resource_id, uuid.UUID):
            resource_id = uuid.UUID(resource_id)
        r = self.db.resource.find_one({'_id': resource_id})
        rh = copy.deepcopy(r)

        # raise Exception(metrics, r)
        if rh is None:
            raise indexer.NoSuchResource(resource_id)
        rh['revision_end'] = now
        rh.pop('_id')
        self.db.history.insert(rh)
        s = {}
        if ended_at is not _marker:
            if rh['started_at'] is not None and ended_at is not None:
                # Convert to UTC because we store in UTC :(
                ended_at = _to_timestamp(ended_at)
                rh['started_at'] = _to_timestamp(rh.get('started_at'))
                if rh['started_at'] > ended_at:
                    raise indexer.ResourceValueError(
                        resource_type, "ended_at", ended_at)
                s['lifespan'] = (
                    (ended_at - rh['started_at']).total_seconds())
            s['ended_at'] = ended_at
        s['revision_start'] = now

        if kwargs:
            for attribute, value in six.iteritems(kwargs):
                if rh.get(attribute):
                    s[attribute] = value
                else:
                    raise indexer.ResourceAttributeError(
                        rh['type'], attribute)

        if metrics is not _marker:
            if not append_metrics:
                for id, rm in six.iteritems(r.pop('metrics', {})):
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

                s['metrics'] = self._set_metrics_for_resource(r, metrics)
            else:
                r['metrics'].update(self._set_metrics_for_resource(r, metrics))
                s['metrics'] = r['metrics']
        r = self.db.resource.find_and_modify(
            {'_id': resource_id},
            {'$set': s},
            new=True,
        )
        r.pop('_id')
        return indexer.Resource(r.pop('id'), r.pop('type'),
                                r.pop('created_by_user_id'),
                                r.pop('created_by_project_id'),
                                started_at=_to_timestamp(r.pop('started_at')),
                                ended_at=_to_timestamp(r.pop('ended_at')),
                                revision_start=_to_timestamp(
                                    r.pop('revision_start')),
                                metrics=[storage.Metric(
                                    uuid.UUID(k),
                                    self._archive_policy_model(
                                        v['archive_policy_id']),
                                    name=v['name']) for k, v in
                                         six.iteritems(r.pop('metrics', {}))],
                                **r)

    def get_resource(self, resource_type, resource_id, with_metrics=False):
        proj = None if with_metrics else {'metrics': 0}
        if not isinstance(resource_id, uuid.UUID):
            resource_id = uuid.UUID(resource_id)
        r = self.db.resource.find_one({'_id': resource_id}, proj)
        if r:
            r.pop('_id')
            return indexer.Resource(
                r.pop('id'), r.pop('type'), r.pop('created_by_user_id'),
                r.pop('created_by_project_id'),
                started_at=_to_timestamp(r.pop('started_at')),
                ended_at=_to_timestamp(r.pop('ended_at')),
                revision_start=_to_timestamp(r.pop('revision_start')),
                revision_end=_to_timestamp(r.pop('revision_end', None)),
                metrics=[storage.Metric(
                    uuid.UUID(k),
                    self._archive_policy_model(
                        v['archive_policy_id']), name=v['name']) for k, v in
                         six.iteritems(r.pop('metrics', {}))],
                **r)

    def _resources_list(self, found_resources):
        return list(
            indexer.Resource(
                r.pop('id'), r.pop('type'), r.pop('created_by_user_id'),
                r.pop('created_by_project_id'),
                started_at=_to_timestamp(r.pop('started_at')),
                ended_at=_to_timestamp(r.pop('ended_at')),
                revision_start=_to_timestamp(r.pop('revision_start')),
                revision_end=_to_timestamp(r.pop('revision_end', None)),
                metrics=[storage.Metric(
                    uuid.UUID(k),
                    self._archive_policy_model(
                        v['archive_policy_id']), name=v['name']) for k, v in
                        six.iteritems(r.pop('metrics', {}))],
                **r)
            for r in found_resources
        )

    def list_resources(self, resource_type='generic',
                       attribute_filter=None,
                       details=False,
                       history=False):

        if attribute_filter:
            resource_schema = (self.db.resource_schema.find_one(
                {'_id': resource_type}) or {'_id': resource_type})
            attr_query = QueryTransformer().transform_filter(
                attribute_filter, resource_schema)
        else:
            attr_query = {}

        type_query = {'type': ({'$exists': True} if resource_type == 'generic'
                               else resource_type)}
        sum_query = {'$and': [type_query, attr_query]}
        res = []
        if history:
            res.extend(self._resources_list(self.db.history.find(sum_query)))
        res.extend(self._resources_list(self.db.resource.find(sum_query)))
        return res

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
        if r is None:
            self.db.resource.update(
                {'$and': [{'_id': 'free_metrics'},
                          {str(id): {'$exists': True}}]},
                {'$unset': {str(id): ''}}
            )

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
                for id, metric in six.iteritems(r.get('metrics', {})):
                    if metric['name'] == name:
                        raise indexer.NamedMetricAlreadyExists(name)
                s[str(metric_id)] = {
                    'name': name,
                    'archive_policy_id': m[str(metric_id)]['archive_policy_id']
                }
            else:
                raise indexer.NoSuchMetric(metric_id)
        return s

    def _utcnow(self):
        time = utils.utcnow()
        ms = time.microsecond / 1000 * 1000
        return time.replace(microsecond=ms)


def _to_timestamp(value):
    if value:
        return utils.to_timestamp(value)


class QueryTransformer(mutils.QueryTransformer):
    operators = {
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

    time_fields = ('started_at', 'ended_at', 'revision_start', 'revision_end')
    uuid_fields = ('user_id', 'project_id', 'id')

    @staticmethod
    def _move_negation_to_leaf(condition):
        """Moves every not operator to the leafs.

        Moving is going by applying the De Morgan rules and annihilating
        double negations.
        """
        def _apply_de_morgan(tree, negated_subtree, negated_op):
            if negated_op in [u"and", u"∧"]:
                new_op = u"or"
            else:
                new_op = u"and"

            tree[new_op] = [{u"not": child}
                            for child in negated_subtree[negated_op]]
            del tree[u"not"]

        def transform(subtree):
            op = subtree.keys()[0]
            if op in [u"or", u"∨", u"and", u"∧"]:
                [transform(child) for child in subtree[op]]
            elif op == u"not":
                negated_tree = subtree[op]
                negated_op = negated_tree.keys()[0]
                if negated_op in [u"and", u"∧"]:
                    _apply_de_morgan(subtree, negated_tree, negated_op)
                    transform(subtree)
                elif negated_op in [u"or", u"∨"]:
                    _apply_de_morgan(subtree, negated_tree, negated_op)
                    transform(subtree)
                elif negated_op == u"not":
                    # two consecutive not annihilates themselves
                    new_op = negated_tree.values()[0].keys()[0]
                    subtree[new_op] = negated_tree[negated_op][new_op]
                    del subtree[u"not"]
                    transform(subtree)

        transform(condition)

    def transform_filter(self, condition, schema):
        # in Mongo not operator can only be applied to
        # simple expressions so we have to move every
        # not operator to the leafs of the expression tree
        self._move_negation_to_leaf(condition)
        return self._process_json_tree(condition, schema)

    def _handle_complex_op(self, complex_op, nodes, schema):
        element_list = []
        for node in nodes:
            element = self._process_json_tree(node, schema)
            element_list.append(element)
        complex_operator = self.complex_operators[complex_op]
        op = {complex_operator: element_list}
        return op

    def _handle_not_op(self, negated_tree, schema):
        # assumes that not is moved to the leaf already
        # so we are next to a leaf
        negated_op = negated_tree.keys()[0]
        negated_field = negated_tree[negated_op].keys()[0]
        if negated_field not in schema.get('attributes', []):
            raise indexer.QueryAttributeError(schema['_id'], negated_field)
        value = negated_tree[negated_op][negated_field]
        if negated_op in [u"=", u"==", u"eq"]:
            return {negated_field: {u"$ne": value}}
        elif negated_op in [u"!=", u"≠", u"ne"]:
            return {negated_field: value}
        else:
            return {negated_field: {u"$not":
                                    {self.operators[negated_op]: value}}}

    def _handle_simple_op(self, simple_op, nodes, schema):
        field_name = nodes.keys()[0]
        if field_name not in schema.get('attributes', []):
            raise indexer.QueryAttributeError(schema['_id'], field_name)
        if (field_name in self.time_fields and
                not isinstance(nodes.values()[0], datetime.datetime)):
            field_value = _to_timestamp(nodes.values()[0])
        elif (field_name in self.uuid_fields and
              not isinstance(nodes.values()[0], uuid.UUID)):
            field_value = uuid.UUID(nodes.values()[0])
        else:
            field_value = nodes.values()[0]

        # no operator for equal in Mongo
        if simple_op in [u"=", u"==", u"eq"]:
            op = {field_name: field_value}
            return op

        operator = self.operators[simple_op]
        op = {field_name: {operator: field_value}}
        return op

    def _process_json_tree(self, condition_tree, schema):
        operator_node = condition_tree.keys()[0]
        nodes = condition_tree.values()[0]

        if operator_node in self.complex_operators:
            return self._handle_complex_op(operator_node, nodes, schema)

        if operator_node == u"not":
            negated_tree = condition_tree[operator_node]
            return self._handle_not_op(negated_tree, schema)

        return self._handle_simple_op(operator_node, nodes, schema)
