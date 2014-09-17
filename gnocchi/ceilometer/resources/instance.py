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

from ceilometer.openstack.common import log

from gnocchi.ceilometer.resources import base

LOG = log.getLogger(__name__)


class Instance(base.ResourceBase):
    @staticmethod
    def get_resource_extra(sample):
        try:
            params = {
                "host": sample['resource_metadata']['host'],
                "image_ref":
                sample['resource_metadata']['image_ref_url'],
                "display_name":
                sample['resource_metadata']['display_name'],
            }
            if "instance_flavor_id" in sample['resource_metadata']:
                params["flavor_id"] = int(
                    sample['resource_metadata']['instance_flavor_id'])
            else:
                # NOTE(sileht): instance.exists have the flavor here
                params["flavor_id"] = int(
                    sample['resource_metadata']["flavor"]["id"])
        except KeyError:
            LOG.exception("Fail to convert resource metadata %s" %
                          sample['resource_metadata'])
            raise
        return params

    @staticmethod
    def get_entities_names():
        return ['instance',
                'disk.root.size',
                'disk.ephemeral.size',
                'memory',
                'vcpus']
