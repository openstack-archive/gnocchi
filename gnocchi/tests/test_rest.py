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
import uuid

import pecan.testing
import testscenarios

from gnocchi.openstack.common import jsonutils
from gnocchi.rest import app
from gnocchi import tests


load_tests = testscenarios.load_tests_apply_scenarios


class RestTest(tests.TestCase):
    def setUp(self):
        super(RestTest, self).setUp()
        c = {}
        c.update(app.PECAN_CONFIG)
        c['conf'] = self.conf
        c['indexer'] = self.index
        c['storage'] = self.storage
        self.app = pecan.testing.load_test_app(c)

    def test_root(self):
        result = self.app.get("/")
        self.assertEqual(b"Nom nom nom.", result.body)
        self.assertEqual("text/plain", result.content_type)
        self.assertEqual(200, result.status_code)


class EntityTest(RestTest):
    def test_post_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60),
                                                         (60, 60)]})
        self.assertEqual("application/json", result.content_type)
        self.assertEqual(201, result.status_code)
        entity = jsonutils.loads(result.body)
        self.assertEqual("http://localhost/v1/entity/" + entity['id'],
                         result.headers['Location'])
        self.assertEqual(entity['archives'], [[5, 60], [60, 60]])

    def test_delete_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60),
                                                         (60, 60)]})
        entity = jsonutils.loads(result.body)
        result = self.app.delete("/v1/entity/" + entity['id'])
        self.assertEqual(result.status_code, 204)

    def test_delete_entity_non_existent(self):
        e1 = str(uuid.uuid4())
        result = self.app.delete("/v1/entity/" + e1,
                                 expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn(
            b"Entity " + e1.encode('ascii') + b" does not exist",
            result.body)

    def test_post_entity_bad_archives(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60, 30),
                                                         (60, 60)]},
                                    expect_errors=True)
        self.assertEqual("text/plain", result.content_type)
        self.assertEqual(result.status_code, 400)
        self.assertIn(b"Invalid input: invalid list value @ data["
                      + repr(u'archives').encode('ascii') + b"][0]",
                      result.body)

    def test_add_measure(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60),
                                                         (60, 60)]})
        entity = jsonutils.loads(result.body)
        result = self.app.post_json(
            "/v1/entity/%s/measures" % entity['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}])
        self.assertEqual(result.status_code, 204)

    def test_add_measure_no_such_entity(self):
        e1 = str(uuid.uuid4())
        result = self.app.post_json(
            "/v1/entity/%s/measures" % e1,
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn(
            b"Entity " + e1.encode('ascii') + b" does not exist",
            result.body)

    def test_get_measure(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(1, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get("/v1/entity/%s/measures" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T23:23:23.000000': 1234.2},
                         result)

    def test_get_measure_start(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(1, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/entity/%s/measures?start='2013-01-01 23:23:20"
            % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T23:23:23.000000': 1234.2},
                         result)

    def test_get_measure_stop(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(1, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        ret = self.app.get("/v1/entity/%s/measures"
                           "?stop=2013-01-01 12:00:00" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T12:00:00.000000': 1234.2},
                         result)

    def test_get_measure_aggregation(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 123.2},
                                   {"timestamp": '2013-01-01 12:00:03',
                                    "value": 12345.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/entity/%s/measures?aggregation=max" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T12:00:00.000000': 12345.2},
                         result)


class ResourceTest(RestTest):

    resource_scenarios = [
        ('generic', dict(
            attributes={
                "started_at": "2014-01-03 02:02:02",
                "user_id": "foo",
                "project_id": "bar",
            },
            resource_type='generic')),
        ('instance', dict(
            attributes={
                "started_at": "2014-01-03 02:02:02",
                "user_id": "foo",
                "project_id": "bar",
                "host": "foo",
                "image_ref": "imageref!",
                "flavor_id": 123,
                "display_name": "myinstance",
                "architecture": "arm",
            },
            resource_type='instance')),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(
            cls.scenarios,
            cls.resource_scenarios)

    def setUp(self):
        super(ResourceTest, self).setUp()
        # Copy attributes so we can modify them in each test :)
        self.attributes = self.attributes.copy()
        # Set an id in the attribute
        self.attributes['id'] = str(uuid.uuid4())

    def test_post_resource(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = jsonutils.loads(result.body)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/" + self.attributes['id'],
                         result.headers['Location'])
        self.attributes['type'] = self.resource_type
        self.attributes['ended_at'] = None
        self.attributes['entities'] = {}
        self.assertEqual(resource, self.attributes)

    def test_post_resource_already_exist(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertIn("Resource %s already exists" % self.attributes['id'],
                      result.body)

    def test_post_unix_timestamp(self):
        self.attributes['started_at'] = "1400580045.856219"
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = jsonutils.loads(result.body)
        self.assertEqual(u"2014-05-20 10:00:45.856219",
                         resource['started_at'])

    def test_post_invalid_timestamp(self):
        self.attributes['started_at'] = "2014-01-01 02:02:02"
        self.attributes['ended_at'] = "2013-01-01 02:02:02"
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)

    def test_post_invalid_no_user(self):
        del self.attributes['user_id']
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)

    def test_post_invalid_no_project(self):
        del self.attributes['project_id']
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)

    def test_get_resource(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = jsonutils.loads(result.body)
        self.attributes['type'] = self.resource_type
        self.attributes['entities'] = {}
        self.attributes['ended_at'] = None
        self.assertEqual(self.attributes, result)

    def test_patch_resource_entities(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        r = jsonutils.loads(result.body)
        self.assertEqual(201, result.status_code)
        new_entities = {'foo': {'archives': [(1, 2)]}}
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'entities': new_entities})
        self.assertEqual(result.status_code, 204)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = jsonutils.loads(result.body)
        self.assertTrue(uuid.UUID(result['entities']['foo']))
        del result['entities']
        del r['entities']
        self.assertEqual(r, result)

    def test_patch_resource_non_existent_entities(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'entities': {'foo': e1}},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        # FIXME(jd) We should retrieve the real entity when oslo.db is improved
        self.assertIn("Entity ??? does not exist", result.body)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = jsonutils.loads(result.body)
        self.assertEqual(result['entities'], {})

    def test_patch_resource_ended_at(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + self.attributes['id'],
            params={'ended_at': "2043-05-05 23:23:23"})
        self.assertEqual(result.status_code, 204)
        result = self.app.get("/v1/resource/" + self.resource_type
                              + "/" + self.attributes['id'])
        result = jsonutils.loads(result.body)
        self.assertEqual("2043-05-05 23:23:23", result['ended_at'])

    def test_patch_resource_ended_at_before_started_at(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'ended_at': "2000-05-05 23:23:23"},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)

    def test_patch_resource_no_partial_update(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'ended_at': "2044-05-05 23:23:23",
                    'entities': {"foo": str(uuid.uuid4())}},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn("Entity ??? does not exist", result.body)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = jsonutils.loads(result.body)
        self.attributes['type'] = self.resource_type
        self.attributes['ended_at'] = None
        self.attributes['entities'] = {}
        self.assertEqual(self.attributes, result)

    def test_patch_resource_non_existent(self):
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params={},
            expect_errors=True)
        self.assertEqual(result.status_code, 404)

    def test_patch_resource_unknown_field(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'foobar': 123},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn(
            "Invalid input: extra keys not allowed @ data[u'foobar']",
            result.body)

    def test_delete_resource(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.delete("/v1/resource/" + self.resource_type + "/"
                                 + self.attributes['id'])
        self.assertEqual(204, result.status_code)

    def test_delete_resource_non_existent(self):
        result = self.app.delete("/v1/resource/" + self.resource_type + "/"
                                 + self.attributes['id'],
                                 expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertIn(
            u"Resource %s does not exist" % self.attributes['id'],
            result.body)

    def test_post_resource_invalid_uuid(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params={"id": "foobar"},
                                    expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Invalid input: not a valid value "
                      b"for dictionary value @ data["
                      + repr(u'id').encode('ascii') + b"]",
                      result.body)

    def test_post_resource_with_entities(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 10)]})
        entity = jsonutils.loads(result.body)
        self.attributes['entities'] = {"foo": entity['id']}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = jsonutils.loads(result.body)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.attributes['type'] = self.resource_type
        self.attributes['ended_at'] = None
        self.assertEqual(resource, self.attributes)

    def test_post_resource_with_null_entities(self):
        self.attributes['entities'] = {"foo": {"archives": [(10, 20)]}}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = jsonutils.loads(result.body)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.assertEqual(resource["id"], self.attributes['id'])
        entity_id = uuid.UUID(resource['entities']['foo'])
        result = self.app.get("/v1/entity/" + str(entity_id) + "/measures")
        self.assertEqual(200, result.status_code)

    def test_list_resources_by_user(self):
        u1 = str(uuid.uuid4())
        self.attributes['user_id'] = u1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = jsonutils.loads(result.body)
        result = self.app.get("/v1/resource/generic",
                              params={"user_id": u1})
        self.assertEqual(200, result.status_code)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"user_id": u1})
        self.assertEqual(200, result.status_code)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

    def test_list_resources_by_project(self):
        p1 = str(uuid.uuid4())
        self.attributes['project_id'] = p1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = jsonutils.loads(result.body)
        result = self.app.get("/v1/resource/generic",
                              params={"project_id": p1})
        self.assertEqual(200, result.status_code)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"project_id": p1})
        self.assertEqual(200, result.status_code)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

    def test_list_resources(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": "foo",
                "project_id": "bar",
            })
        g = jsonutils.loads(result.body)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = jsonutils.loads(result.body)
        result = self.app.get("/v1/resource/generic")
        self.assertEqual(200, result.status_code)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 2)

        i_found = False
        g_found = False
        for r in resources:
            if r['id'] == str(g['id']):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(i['id']):
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        result = self.app.get("/v1/resource/" + self.resource_type)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_started_after(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": "foo",
                "project_id": "bar",
            })
        g = jsonutils.loads(result.body)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = jsonutils.loads(result.body)
        result = self.app.get(
            "/v1/resource/generic?started_after=2014-01-01")
        self.assertEqual(200, result.status_code)
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 2)

        i_found = False
        g_found = False
        for r in resources:
            if r['id'] == str(g['id']):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(i['id']):
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "?started_after=2014-01-03")
        resources = jsonutils.loads(result.body)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

ResourceTest.generate_scenarios()
