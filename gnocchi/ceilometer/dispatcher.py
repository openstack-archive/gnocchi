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
import json
import operator
import uuid

from ceilometer import dispatcher
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from oslo.config import cfg
import requests
import six
import stevedore.dispatch

from gnocchi import storage
from gnocchi import indexer

import pdb
pdb.set_trace()

LOG = log.getLogger(__name__)

dispatcher_opts = [
    cfg.StrOpt('url',
               default="http://localhost:8041",
               help='URL to Gnocchi.'),
    cfg.StrOpt('archive_policy',
               default="low",
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
            func(*args, **kwargs)
        except UnexpectedWorkflowError as e:
            LOG.error(six.text_type(e))

    return log_and_ignore


class GnocchiDispatcher(dispatcher.Base):
    def __init__(self, conf):
        super(GnocchiDispatcher, self).__init__(conf)
        self.gnocchi_url = conf.dispatcher_gnocchi.url
        self.gnocchi_archive_policy = {
            'archive_policy':
                cfg.CONF.dispatcher_gnocchi.archive_policy
        }
        self.mgmr = stevedore.dispatch.DispatchExtensionManager(
            'gnocchi.ceilometer.resource', lambda x: True,
            invoke_on_load=True)
        self.storage = storage.get_driver(cfg.CONF)
        self.indexer = indexer.get_driver(cfg.CONF)

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

                        # FIXME(sileht): Does it reasonable to skip the resource
                        # update here ? Does differents kind of counter_name
                        # can have different metadata set ?
                        # (ie: one have only flavor_id, and an other one have only
                        # image_ref ?)
                        #
                        # resource_need_to_be_updated = False

    @log_and_ignore_unexpected_workflow_error
    def _process_samples(self, ext, resource_id, entity_name, samples,
                         resource_need_to_be_updated):

        if not samples[-1]['project_id'] or not samples[-1]['project_id']:
            LOG.debug("Get resource without project_id/user_id: %s" %
                      samples)
            # NOTE(sileht): project_id/user_id are mandatory for gnocchi
            # but for example storage.api.request don't have that one
            return

        resource_type = ext.name

        measure_attributes = [storage.Measure(
            **{'timestamp': sample['timestamp'],
               'value': sample['counter_volume']})
            for sample in samples]
        # TODO(ityaptin) Add measures verification

        try:
            # storage.add_measures
            self._post_measure(resource_type, resource_id, entity_name,
                               measure_attributes)
        except NoSuchEntity:
            # NOTE(sileht): we try first to create the resource, because
            # they more chance that the resource doesn't exists than the entity
            # is missing, the should be reduce the number of resource API call
            pdb.set_trace()
            resource_attributes = self._get_resource_attributes(
                ext, resource_id, entity_name, samples)
            try:
                # indexer.create_resource
                self._create_resource(resource_type, resource_id,
                                      resource_attributes)
            except ResourceAlreadyExists:
                try:
                    # storage.create_entity(id, archive_policy definition)
                    pdb.set_trace()
                    self._create_entity(resource_type, resource_id,
                                        entity_name, resource_attributes)
                except EntityAlreadyExists:
                    # NOTE(sileht): Just ignore the entity have been created in
                    # the meantime.
                    pass
            else:
                # No need to update it we just created it
                # with everything we need
                resource_need_to_be_updated = False

            # NOTE(sileht): we retry to post the measure but if it fail we
            # don't catch the exception to just log it and continue to process
            # other samples
            # storage.post_measures
            pdb.set_trace()
            self._post_measure(resource_type, resource_id, entity_name,
                               measure_attributes)

        if resource_need_to_be_updated:
            resource_attributes = self._get_resource_attributes(
                ext, resource_id, entity_name, samples, for_update=True)
            # update resource
            self._update_resource(resource_type, resource_id,
                                  resource_attributes)

    def _get_resource_attributes(self, ext, resource_id, entity_name, samples,
                                 for_update=False):
        # FIXME(sileht): Should I merge attibutes of all samples ?
        # Or keep only the last one is sufficient ?
        attributes = ext.obj.get_resource_extra_attributes(
            samples[-1])
        if not for_update:
            attributes["id"] = resource_id
            attributes["user_id"] = samples[-1]['user_id']
            attributes["project_id"] = samples[-1]['project_id']
            attributes["entities"] = dict(
                (entity_name, self.gnocchi_archive_policy)
                for entity_name in ext.obj.get_entities_names()
            )
        return attributes

    def _post_measure(self, resource_type, resource_id, entity_name,
                      measure_attributes):
        try:
            pdb.set_trace()
            self.storage.add_measures(entity_name, measure_attributes)
        except storage.EntityDoesNotExist:
            raise NoSuchEntity
        except Exception as e:
            raise UnexpectedWorkflowError(e.message)
        else:
            LOG.debug("Measure posted on entity %s of resource %s",
                      entity_name, resource_id)

    def _create_resource(self, resource_type, resource_id,
                         resource_attributes):
        try:
            self.indexer.create_resource(resource_type, resource_id,
                                         **resource_attributes)
        except indexer.ResourceAlreadyExists:
            raise ResourceAlreadyExists
        except Exception:
            raise UnexpectedWorkflowError(
                _("Resource %(resource_id)s creation failed with"))
        else:
            LOG.debug("Resource %s created", resource_id)

    def _update_resource(self, resource_type, resource_id,
                         resource_attributes):
        try:
            self.indexer.update_resource(resource_type, resource_id,
                                         **resource_attributes)
        except indexer.NoSuchResource:
            raise NoSuchResource
        except indexer.UnknownResourceType:
            raise NoSuchResource
        except Exception as e:
            raise UnexpectedWorkflowError(e.message)
        else:
            LOG.debug("Resource %s updated", resource_id)

    def _create_entity(self, resource_type, resource_id, entity_name,
                       resource_attributes):
        id = uuid.uuid4().hex
        try:
            del resource_attributes['entities']
            self.indexer.create_resource('entity', id, **resource_attributes)
            self.storage.create_entity(entity_name,
                                       self.gnocchi_archive_policy)
        except storage.EntityAlreadyExists:
            raise EntityAlreadyExists
        except Exception as e:
            raise UnexpectedWorkflowError(e.message)
        else:
            LOG.debug("Entity %s of resource %s created",
                      entity_name, resource_id)

    def record_events(self, events):
        raise NotImplementedError
