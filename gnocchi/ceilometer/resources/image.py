#
# Copyright 2014 eNovance
#
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
from gnocchi.ceilometer.resources import base

from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)


class Image(base.ResourceBase):
    @staticmethod
    def get_resource_extra_attributes(sample):
        metadata = sample['resource_metadata']

        LOG.debug("Metadata: %s" % metadata)

        params = {
            "size": int(metadata['size'])
        }
        return params

    @staticmethod
    def get_metrics_names():
        return ['image',
                'image.size']
