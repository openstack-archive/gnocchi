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
from oslo.config import cfg


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
            params = {
                "user_id": sample['user_id'],
                "project_id": sample['project_id']
            }
            if sample['counter_name'] == 'instance':
                resource_type = 'instance'
                params.update({
                    "host": sample['metadata']['host'],
                    "flavor_id": sample['metadata']['instance_type']['flavor_id'],
                    "image_ref": sample['metadata']['image_ref'],
                    "display_name": sample['metadata']['display_name'],
                    "architecture": sample['metadata']['architecture'],
                })
            else:
                resource_type = 'generic'
            r = requests.post("%s/v1/resource/%s"
                              % (self.gnocchi_url, resource_type),
                              data=json.dumps(params))
            if r.status_code == 409:
                # Update
                pass
            # TODO(jd) Post metrics to entities

    @staticmethod
    def record_events(events):
        # TODO(jd) Write dat code.
        pass
