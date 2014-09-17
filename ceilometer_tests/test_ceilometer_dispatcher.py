#
# Copyright 2014 eNovance
#
# Authors: Mehdi Abaakouk <mehdi.abaakouk@enovance.com>
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

import json
import uuid

import mock
from oslo.config import fixture as config_fixture
import testscenarios
import testtools

from gnocchi.ceilometer import dispatcher


load_tests = testscenarios.load_tests_apply_scenarios


class json_matcher(object):
    def __init__(self, ref):
        self.ref = ref

    def __eq__(self, obj):
        return self.ref == json.loads(obj)

    def __repr__(self):
        return "<json_matcher \"%s\">" % self.ref


class DispatcherManagerTest(testtools.TestCase):
    def test_extensions_load(self):
        self.conf = self.useFixture(config_fixture.Config()).conf
        self.dispatcher = dispatcher.GnocchiDispatcher(self.conf)
        self.assertIn('instance', self.dispatcher.mgmr.names())


class DispatcherTest(testtools.TestCase, testscenarios.TestWithScenarios):

    sample_scenarios = [
        ('disk.root.size', dict(
            sample={
                'counter_name': 'disk.root.size',
                'counter_type': 'gauge',
                'counter_volume': '2',
                'user_id': 'test_user',
                'project_id': 'test_project',
                'source': 'openstack',
                'timestamp': '2012-05-08 20:23:48.028195',
                'resource_metadata': {
                    'host': 'foo',
                    'image_ref_url': 'imageref!',
                    'instance_flavor_id': 1234,
                    'display_name': 'myinstance',
                }
            },
            measures_attributes=[{
                'timestamp': '2012-05-08 20:23:48.028195',
                'value': '2'
            }],
            postable_attributes={
                'user_id': 'test_user',
                'project_id': 'test_project',
            },
            patchable_attributes={
                'host': 'foo',
                'image_ref': 'imageref!',
                'flavor_id': 1234,
                'display_name': 'myinstance',
            },
            entity_names=[
                'instance', 'disk.root.size', 'disk.ephemeral.size',
                'memory', 'vcpus'],
            resource_type='instance')),
    ]

    worflow_scenarios = [
        ('normal_workflow',
         dict(resource='exists', entity="exists", measure=True)),
        ('new_resource',
         dict(resource=None, entity="exists", measure=True)),
        ('resource_patch_fail',
         dict(resource="patch_fail", entity=None, measure=False)),
        ('resource_create_fail',
         dict(resource="create_fail", entity=None, measure=False)),
        ('new_entity',
         dict(resource='exists', entity=None, measure=True)),
        ('entity_fail',
         dict(resource='exists', entity="fail", measure=False)),
        ('new_both',
         dict(resource=None, entity=None, measure=True)),
        ('fail_post_measure',
         dict(resource='exists', entity="exists", measure=False)),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls.sample_scenarios,
                                                         cls.worflow_scenarios)

    def setUp(self):
        super(DispatcherTest, self).setUp()
        self.conf = self.useFixture(config_fixture.Config()).conf
        self.dispatcher = dispatcher.GnocchiDispatcher(self.conf)
        self.sample['resource_id'] = str(uuid.uuid4())

    @mock.patch('gnocchi.ceilometer.dispatcher.LOG')
    @mock.patch('gnocchi.ceilometer.dispatcher.requests')
    def test_workflow(self, requests, logger):
        url_params = {
            'url': 'http://localhost:8041',
            'resource_id': self.sample['resource_id'],
            'resource_type': self.resource_type,
            'entity_name': self.sample['counter_name']
        }

        expected_calls = [
            mock.call.patch("%(url)s/v1/resource/%(resource_type)s/"
                            "%(resource_id)s" % url_params,
                            headers={'Content-Type': 'application/json'},
                            data=json_matcher(self.patchable_attributes)),
        ]
        patch_responses = []
        post_responses = []
        if self.resource == "exists":
            patch_responses.append(mock.Mock(status_code=204))
        elif self.resource == "patch_fail":
            patch_responses.append(mock.Mock(status_code=500))
        else:
            patch_responses.append(mock.Mock(status_code=404))
            if self.resource == "create_fail":
                post_responses.append(mock.Mock(status_code=500))
            else:
                post_responses.append(mock.Mock(status_code=204))
            attributes = self.postable_attributes.copy()
            attributes.update(self.patchable_attributes)
            attributes['id'] = self.sample['resource_id']
            attributes['entities'] = dict((entity_name,
                                           {'archive_policy': 'low'})
                                          for entity_name in self.entity_names)
            expected_calls += [
                mock.call.post("%(url)s/v1/resource/%(resource_type)s"
                               % url_params,
                               headers={'Content-Type': 'application/json'},
                               data=json_matcher(attributes))
            ]

        if self.resource not in ["create_fail", "patch_fail"]:
            expected_calls += [
                mock.call.post("%(url)s/v1/resource/%(resource_type)s/"
                               "%(resource_id)s/entity/%(entity_name)s/"
                               "measures"
                               % url_params,
                               headers={'Content-Type': 'application/json'},
                               data=json_matcher(self.measures_attributes))
            ]
            if not self.entity:
                post_responses.append(mock.Mock(status_code=404))

                expected_calls += [
                    mock.call.post("%(url)s/v1/resource/%(resource_type)s/"
                                   "%(resource_id)s/entity" % url_params,
                                   headers={'Content-Type':
                                            'application/json'},
                                   data=json_matcher(
                                       {self.sample['counter_name']:
                                        {'archive_policy': 'low'}}))
                ]
                if self.entity == "fail":
                    post_responses.append(mock.Mock(status_code=500))
                else:
                    post_responses.append(mock.Mock(status_code=204))
                    expected_calls += [
                        mock.call.post("%(url)s/v1/resource/%(resource_type)s/"
                                       "%(resource_id)s/entity/"
                                       "%(entity_name)s/measures"
                                       % url_params,
                                       headers={'Content-Type':
                                                'application/json'},
                                       data=json_matcher(
                                           self.measures_attributes))
                    ]
                    if self.measure:
                        post_responses.append(mock.Mock(status_code=204))
                    else:
                        post_responses.append(mock.Mock(status_code=500))
            elif not self.measure:
                post_responses.append(mock.Mock(status_code=500))
            else:
                post_responses.append(mock.Mock(status_code=204))

        requests.patch.side_effect = patch_responses
        requests.post.side_effect = post_responses

        self.dispatcher.record_metering_data([self.sample])
        self.assertEqual(expected_calls, requests.mock_calls)

        # Check that the last log message is the expected one
        if self.resource in ["create_fail", "patch_fail"]:
            # resource fail
            logger.error.assert_called_with(
                mock.ANY,
                {'resource_id': self.sample['resource_id'],
                 'status_code': 500,
                 'msg': mock.ANY})
        elif self.entity == "fail":
            logger.error.assert_called_with(
                mock.ANY,
                {'entity_name': self.sample['counter_name'],
                 'resource_id': self.sample['resource_id'],
                 'status_code': 500,
                 'msg': mock.ANY})
        elif not self.measure:
            logger.error.assert_called_with(
                mock.ANY,
                {'entity_name': self.sample['counter_name'],
                 'resource_id': self.sample['resource_id'],
                 'status_code': 500,
                 'msg': mock.ANY})
        else:
            logger.debug.assert_called_with("Resource %s created",
                                            self.sample['resource_id'])


DispatcherTest.generate_scenarios()
