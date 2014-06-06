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

import json

import requests

from ceilometer import dispatcher
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from oslo.config import cfg

LOG = log.getLogger(__name__)

dispatcher_opts = [
    cfg.StrOpt('gnocchi_url',
               default="http://localhost:8041",
               help='URL to Gnocchi.'),
]

cfg.CONF.register_opts(dispatcher_opts, group="dispatcher_gnocchi")


class GnocchiDispatcher(dispatcher.Base):

    def __init__(self, conf):
        super(GnocchiDispatcher, self).__init__(conf)
        self.gnocchi_url = conf.dispatcher_gnocchi.gnocchi_url

    def record_metering_data(self, data):
        for sample in data:
            if sample['counter_name'] in ('instance',
                                          'disk.root.size',
                                          'disk.ephemeral.size',
                                          'memory',
                                          'vcpus'):
                resource_type = 'instance'
                params = {
                    "host": sample['resource_metadata']['host'],
                    "flavor_id": int(
                        sample['resource_metadata']['instance_flavor_id']),
                    "image_ref":
                    sample['resource_metadata']['image_ref_url'],
                    "display_name":
                    sample['resource_metadata']['display_name'],
                }
            else:
                # NOTE(jd): Don't create any resource if we don't know what
                # the sample is about. We could create a generic resource,
                # but for now let's assume it's simpler to do nothing.
                continue

            r = requests.patch(
                "%s/v1/resource/%s/%s"
                % (self.gnocchi_url, resource_type, params['id']),
                headers={'Content-Type': "application/json"},
                data=json.dumps(params))

            # If we get a 404, that's because the resource has not been
            # created yet, let's do it
            if r.status_code == 404:
                params["id"] = sample['resource_id']
                params["user_id"] = sample['user_id']
                params["project_id"] = sample['project_id']
                # TODO(jd) Create entities
                r = requests.post("%s/v1/resource/%s"
                                  % (self.gnocchi_url, resource_type),
                                  headers={'Content-Type': "application/json"},
                                  data=json.dumps(params))
                if r.status_code / 100 != 2:
                    LOG.error(_("Resource %s creation failed with status: %d"),
                              params["id"], r.status_code)
                else:
                    LOG.debug(_("Resource %s created"), params["id"])

            # TODO(jd) Post metrics to entities

    @staticmethod
    def record_events(events):
        # TODO(jd) Write dat code.
        pass
