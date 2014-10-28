#
# Copyright 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
# Mehdi Abaakouk <mehdi.abaakouk@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import

import itertools
import operator
import uuid
import datetime

from ceilometer import dispatcher
import iso8601
from oslo.config import cfg
from oslo.utils import timeutils
import stevedore.dispatch

from gnocchi import storage

from gnocchi import indexer


def Timestamp(v):
    if v is None:
        return v
    try:
        v = float(v)
    except (ValueError, TypeError):
        return timeutils.normalize_time(iso8601.parse_date(v))
    return datetime.datetime.utcfromtimestamp(v)

dispatcher_opts = [
    cfg.StrOpt('url',
               default="http://localhost:8041",
               help='URL to Gnocchi.'),
    cfg.StrOpt('archive_policy',
               default="middle",
               help='The archive policy to use when the dispatcher '
                    'create a new entity.')
]

cfg.CONF.register_opts(dispatcher_opts, group="dispatcher_gnocchi")
cfg.CONF.import_group('storage', 'gnocchi.storage')

CREATE_ENTITY = 'create_entity'
CREATE_RESOURCE = 'create_resource'


class UnexpectedWorkflowError(Exception):
    pass


class NoSuchEntity(Exception):
    pass


class EntityAlreadyExists(Exception):
    pass


class NoSuchResource(Exception):
    pass


class ResourceAlreadyExists(Exception):
    pass


def log_and_ignore_unexpected_workflow_error(func):
    def log_and_ignore(*args, **kwargs):
        try:
            #print 'Process %s' % func
            func(*args, **kwargs)
        except UnexpectedWorkflowError as e:
            #print "Error message %s" % e.message
            #print "Error args %s" % e.args
            raise

    return log_and_ignore


class GnocchiDispatcher(dispatcher.Base):
    def __init__(self, conf):
        super(GnocchiDispatcher, self).__init__(conf)
        print "CONFIG"
        self.gnocchi_url = conf.dispatcher_gnocchi.url
        self.storage = storage.get_driver(conf)
        self.indexer = indexer.get_driver(conf)
        self.archive_policy = None
        self.mgmr = stevedore.dispatch.DispatchExtensionManager(
            'gnocchi.ceilometer.resource', lambda x: True,
            invoke_on_load=True)
        self.entity_cache = {}

    def record_metering_data(self, data):
        # FIXME(sileht): This method bulk the processing of samples
        # grouped by resource_id and entity_name but this is not
        # efficient yet because the data received here doesn't often
        # contains a lot of different kind of samples
        # So perhaps the next step will be to pool the received data from
        # message bus.

        resource_grouped_samples = itertools.groupby(
            data, key=operator.itemgetter('resource_id'))

        for resource_id, samples_of_resource in resource_grouped_samples:
            resource_need_to_be_updated = True

            entity_grouped_samples = itertools.groupby(
                list(samples_of_resource),
                key=operator.itemgetter('counter_name'))
            for entity_name, samples in entity_grouped_samples:
                for ext in self.mgmr:
                    if entity_name in ext.obj.get_entities_names():
                        self._process_samples(
                            ext, resource_id, entity_name, list(samples),
                            resource_need_to_be_updated)

    @log_and_ignore_unexpected_workflow_error
    def _process_samples(self, ext, resource_id, entity_name, samples,
                         resource_need_to_be_updated):
        if resource_id not in self.entity_cache:
            self.entity_cache[resource_id] = {}
        resource_type = ext.name

        measure_attributes = [storage.Measure(
            **{'timestamp': Timestamp(sample['timestamp']),
               'value': float(sample['counter_volume'])})
            for sample in samples]

        entity_id = self.entity_cache[resource_id].get(entity_name)

        try:
            if not entity_id:
                raise NoSuchEntity
            self._post_measure(entity_id, measure_attributes)
        except NoSuchEntity:
            # NOTE(sileht): we try first to create the resource, because
            # they more chance that the resource doesn't exists than the entity
            # is missing, the should be reduce the number of resource API call
            resource_attributes = self._get_resource_attributes(
                ext, resource_id, samples)
            with self.indexer:
                resource = self.indexer.get_resource(resource_type, resource_id)
            if not resource:
                self._create_resource(resource_type,
                                      resource_id,
                                      resource_attributes)
            else:
                self.entity_cache[resource_id].update(resource['entities'])

            if entity_name not in self.entity_cache[resource_id]:
                self._create_entity(resource_id, entity_name,
                                    resource_attributes['user_id'],
                                    resource_attributes['project_id'])
                resource_need_to_be_updated = True

            # NOTE(sileht): we retry to post the measure but if it fail we
            # don't catch the exception to just log it and continue to process
            # other samples
            entity_id = self.entity_cache[resource_id].get(entity_name)
            self._post_measure(entity_id, measure_attributes)

        if resource_need_to_be_updated:
            resource_attributes = self._get_resource_attributes(
                ext, resource_id, samples, for_update=True)
            resource_attributes['entities'] = self.entity_cache.get(resource_id)
            self._update_resource(resource_type, resource_id,
                                  resource_attributes)

    @staticmethod
    def _get_resource_attributes(ext, resource_id, samples,
                                 for_update=False):
        # FIXME(sileht): Should I merge attibutes of all samples ?
        # Or keep only the last one is sufficient ?
        attributes = ext.obj.get_resource_extra_attributes(
            samples[-1])
        if not for_update:
            attributes["user_id"] = samples[-1]['user_id']
            attributes["project_id"] = samples[-1]['project_id']
        return attributes

    def _post_measure(self, entity_id,
                      measure_attributes):
        try:
            self.storage.add_measures(entity_id, measure_attributes)
        except storage.EntityDoesNotExist:
            raise NoSuchEntity
        except Exception as e:
            raise
        else:
            pass

    def _create_resource(self, resource_type, resource_id,
                         resource_attributes):
        try:
            with self.indexer:
                self.indexer.create_resource(resource_type, resource_id,
                                             **resource_attributes)
        except indexer.ResourceAlreadyExists:
            raise ResourceAlreadyExists
        except Exception:
            raise UnexpectedWorkflowError(
                "Resource %(resource_id)s creation failed with")
        else:
            pass

    def _update_resource(self, resource_type, resource_id,
                         resource_attributes):
        try:
            with self.indexer:
                self.indexer.update_resource(resource_type, resource_id,
                                             **resource_attributes)
        except indexer.NoSuchResource:
            raise
            # raise NoSuchResource
        except indexer.UnknownResourceType:
            raise NoSuchResource
        except Exception as e:
            raise
        else:
           pass

    def _create_entity(self, resource_id, entity_name,
                       user_id, project_id):


        id = str(uuid.UUID(uuid.uuid4().hex))
        try:
            with self.indexer:
                if not self.archive_policy:
                    policy_name = cfg.CONF.dispatcher_gnocchi.archive_policy
                    self.archive_policy = self.indexer.get_archive_policy(
                        policy_name)

                self.indexer.create_resource(
                    'entity', id,
                    user_id=user_id,
                    project_id=project_id,
                    archive_policy=self.archive_policy['name'])

                self.storage.create_entity(
                    id, self.archive_policy['definition'])
        except storage.EntityAlreadyExists:
            raise EntityAlreadyExists
        except Exception as e:
            raise
        else:
            if resource_id not in self.entity_cache:
                self.entity_cache[resource_id] = {}
            self.entity_cache[resource_id][entity_name] = id

    def record_events(self, events):
        raise NotImplementedError
