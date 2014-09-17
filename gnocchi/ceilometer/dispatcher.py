#
# Copyright 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
#          Mehdi Abaakouk <mehdi.abaakouk@enovance.com>
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

import json

from ceilometer import dispatcher
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from oslo.config import cfg
import requests
import stevedore.dispatch

LOG = log.getLogger(__name__)

dispatcher_opts = [
    cfg.StrOpt('url',
               default="http://localhost:8041",
               help='URL to Gnocchi.'),
    cfg.StrOpt('archive_policy',
               default="low",
               help='The archive policy to use when the dispatcher '
               'create a new resource')
]

cfg.CONF.register_opts(dispatcher_opts, group="dispatcher_gnocchi")


class ResourceCreateOrUpdateFailure(Exception):
    def __init__(self, resource_id):
        self.resource_id = resource_id


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

    def record_metering_data(self, data):
        for sample in data:
            self.mgmr.map(self._filter_extension, self._handle_sample, sample)
            # NOTE(jd): Don't create any resource if we don't know what
            # the sample is about. We could create a generic resource,
            # but for now let's assume it's simpler to do nothing.

    @staticmethod
    def _filter_extension(ext, sample):
        return sample['counter_name'] in ext.obj.get_entities_names()

    def _handle_sample(self, ext, sample):
        try:
            self._create_or_update_resource(ext, sample)
        except ResourceCreateOrUpdateFailure:
            return
        self._send_datapoint(ext, sample)

    def _create_or_update_resource(self, ext, sample):
        resource_id = sample['resource_id']
        resource_type = ext.name

        params = ext.obj.get_resource_extra(sample)
        r = requests.patch(
            "%s/v1/resource/%s/%s"
            % (self.gnocchi_url, resource_type, resource_id),
            headers={'Content-Type': "application/json"},
            data=json.dumps(params))

        # If we get a 404, that's because the resource has not been
        # created yet, let's do it
        if r.status_code == 404:
            params["id"] = resource_id
            params["user_id"] = sample['user_id']
            params["project_id"] = sample['project_id']
            params["entities"] = dict(
                (entity_name, self.gnocchi_archive_policy)
                for entity_name in ext.obj.get_entities_names()
            )
            r = requests.post("%s/v1/resource/%s"
                              % (self.gnocchi_url, resource_type),
                              headers={'Content-Type': "application/json"},
                              data=json.dumps(params))
            if r.status_code == 409:
                LOG.debug(_("Resource %s have been created in the meantime"),
                          resource_id)
            elif r.status_code / 100 != 2:
                LOG.error(_("Resource %(resource_id)s creation failed with "
                            "status: %(status_code)d: %(msg)s"),
                          {'resource_id': resource_id,
                           'status_code': r.status_code,
                           'msg': r.text})
                raise ResourceCreateOrUpdateFailure(resource_id=resource_id)
            else:
                LOG.debug(_("Resource %s created"), resource_id)

        elif r.status_code / 100 != 2:
            LOG.error(_("Resource %(resource_id)s patch failed with "
                        "status: %(status_code)d: %(msg)s"),
                      {'resource_id': resource_id,
                       'status_code': r.status_code,
                       'msg': r.text})
            raise ResourceCreateOrUpdateFailure(resource_id=resource_id)
        else:
            LOG.debug(_("Resource %s patched"), resource_id)

    def _send_datapoint(self, ext, sample):
        entity_name = sample['counter_name']
        resource_id = sample['resource_id']
        resource_type = ext.name

        params = [{
            'timestamp': sample['timestamp'],
            'value': sample['counter_volume'],
        }]
        r = requests.post("%s/v1/resource/%s/%s/entity/%s/measures"
                          % (self.gnocchi_url, resource_type, resource_id,
                             entity_name),
                          headers={'Content-Type': "application/json"},
                          data=json.dumps(params))

        if r.status_code == 404:
            # TODO(jd/sileht) Create new entity
            # This case is only reached if the plugin handle a new entity
            # since the resource creation
            # Gnocchi API need to be improved to allow to add only 1 entity
            # without the need to submit all the previous created entities UUID
            # For now just raise a error
            LOG.error(_("The entity %(entity_name)s of "
                        "resource %(resource_id)s doesn't exists: "
                        "%(status_code)d, create it first"),
                      {'entity_name': entity_name,
                       'resource_id': resource_id,
                       'status_code': r.status_code})
        elif r.status_code / 100 != 2:
            LOG.error(_("Fail to post measure on entity %(entity_name)s of "
                        "resource %(resource_id)s with status: "
                        "%(status_code)d: %(msg)s"),
                      {'entity_name': entity_name,
                       'resource_id': resource_id,
                       'status_code': r.status_code,
                       'msg': r.text})
        else:
            LOG.debug("Resource %s created", resource_id)

    @staticmethod
    def record_events(events):
        pass
