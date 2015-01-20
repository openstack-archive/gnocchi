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
import contextlib
import datetime
import json
import uuid

from oslo.utils import timeutils
import pecan
import six
from six.moves.urllib import parse as urllib_parse
import testscenarios
import webtest

from gnocchi import rest
from gnocchi.rest import app
from gnocchi.tests import base as tests_base


load_tests = testscenarios.load_tests_apply_scenarios


class FakeMemcache(object):
    VALID_TOKEN_ADMIN = '4562138218392830'
    USER_ID_ADMIN = str(uuid.uuid4())
    PROJECT_ID_ADMIN = str(uuid.uuid4())

    VALID_TOKEN = '4562138218392831'
    USER_ID = str(uuid.uuid4())
    PROJECT_ID = str(uuid.uuid4())

    VALID_TOKEN_2 = '4562138218392832'
    # We replace "-" to simulate a middleware that would send UUID in a non
    # normalized format.
    USER_ID_2 = str(uuid.uuid4()).replace("-", "")
    PROJECT_ID_2 = str(uuid.uuid4()).replace("-", "")

    def get(self, key):
        dt = datetime.datetime(
            year=datetime.MAXYEAR, month=12, day=31,
            hour=23, minute=59, second=59)
        if key == "tokens/%s" % self.VALID_TOKEN_ADMIN:
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN_ADMIN,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID_ADMIN,
                    'name': 'adminusername',
                    'tenantId': self.PROJECT_ID_ADMIN,
                    'tenantName': 'myadmintenant',
                    'roles': [
                        {'name': 'admin'},
                    ]},
            }}, timeutils.isotime(dt)))
        elif key == "tokens/%s" % self.VALID_TOKEN:
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID,
                    'name': 'myusername',
                    'tenantId': self.PROJECT_ID,
                    'tenantName': 'mytenant',
                    'roles': [
                        {'name': 'member'},
                    ]},
            }}, timeutils.isotime(dt)))
        elif key == "tokens/%s" % self.VALID_TOKEN_2:
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN_2,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID_2,
                    'name': 'myusername2',
                    'tenantId': self.PROJECT_ID_2,
                    'tenantName': 'mytenant2',
                    'roles': [
                        {'name': 'member'},
                    ]},
            }}, timeutils.isotime(dt)))

    @staticmethod
    def set(key, value, **kwargs):
        pass


class TestingApp(webtest.TestApp):
    CACHE_NAME = 'fake.cache'

    def __init__(self, *args, **kwargs):
        super(TestingApp, self).__init__(*args, **kwargs)
        # Setup Keystone auth_token fake cache
        self.extra_environ.update({self.CACHE_NAME: FakeMemcache()})
        self.token = FakeMemcache.VALID_TOKEN

    @contextlib.contextmanager
    def use_admin_user(self):
        old_token = self.token
        self.token = FakeMemcache.VALID_TOKEN_ADMIN
        try:
            yield
        finally:
            self.token = old_token

    @contextlib.contextmanager
    def use_another_user(self):
        old_token = self.token
        self.token = FakeMemcache.VALID_TOKEN_2
        try:
            yield
        finally:
            self.token = old_token

    def do_request(self, req, *args, **kwargs):
        req.headers['X-Auth-Token'] = self.token
        return super(TestingApp, self).do_request(req, *args, **kwargs)


class RestTest(tests_base.TestCase):
    def setUp(self):
        super(RestTest, self).setUp()
        c = {}
        c.update(app.PECAN_CONFIG)
        c['conf'] = self.conf
        c['indexer'] = self.index
        c['storage'] = self.storage
        self.conf.import_opt("cache", "keystonemiddleware.auth_token",
                             group="keystone_authtoken")
        self.conf.set_override("cache", TestingApp.CACHE_NAME,
                               group='keystone_authtoken')
        self.app = TestingApp(pecan.load_app(c))

    def test_root(self):
        result = self.app.get("/", status=200)
        self.assertEqual(b"Nom nom nom.", result.body)
        self.assertEqual("text/plain", result.content_type)

    def test_deserialize_force_json(self):
        self.app.post(
            "/v1/archive_policy",
            params="foo",
            status=415)

    @staticmethod
    def runTest():
        pass


class ArchivePolicyTest(RestTest):
    def test_post_archive_policy(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{
            "granularity": "0:01:00",
            "points": 20,
            "timespan": "0:20:00",
        }], ap['definition'])

    def test_post_archive_policy_as_non_admin(self):
        self.app.post_json(
            "/v1/archive_policy",
            params={"name": str(uuid.uuid4()),
                    "definition":
                    [{
                        "granularity": "1 minute",
                        "points": 20,
                    }]},
            status=403)

    def test_post_archive_policy_infinite_points(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "2 minutes",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{
            "granularity": "0:02:00",
            "points": None,
            "timespan": None,
        }], ap['definition'])

    def test_post_archive_policy_invalid_multiple(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                            "timespan": "3 hours",
                        }]},
                status=400)
        self.assertIn(u"timespan ≠ granularity × points".encode('utf-8'),
                      result.body)

    def test_post_archive_policy_unicode(self):
        name = u'æ' + str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                headers={'content-type': 'application/json; charset=UTF-8'},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)

        location = "/v1/archive_policy/" + name
        if six.PY2:
            location = location.encode('utf-8')
        self.assertEqual("http://localhost"
                         + urllib_parse.quote(location),
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{
            "granularity": "0:01:00",
            "points": 20,
            "timespan": "0:20:00",
        }], ap['definition'])

    def test_post_archive_policy_with_timespan(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "granularity": "10s",
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:10",
                           'points': 360,
                           'timespan': '1:00:00'}], ap['definition'])

    def test_post_archive_policy_with_timespan_float_points(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "granularity": "7s",
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:07",
                           'points': 514,
                           'timespan': '0:59:58'}], ap['definition'])

    def test_post_archive_policy_with_timespan_float_granularity(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "points": 1000,
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:04",
                           'points': 1000,
                           'timespan': '1:06:40'}], ap['definition'])

    def test_post_archive_policy_with_timespan_and_points(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "points": 1800,
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:02",
                           'points': 1800,
                           'timespan': '1:00:00'}], ap['definition'])

    def test_post_archive_policy_invalid_unit(self):
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": str(uuid.uuid4()),
                        "definition": [{
                            "granularity": "10s",
                            "timespan": "1 shenanigan",
                        }]},
                status=400)

    def test_post_archive_policy_and_metric(self):
        ap = str(uuid.uuid4())
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": ap,
                        "definition": [{
                            "granularity": "10s",
                            "points": 20,
                        }]},
                status=201)
        self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": ap},
            status=201)

    def test_post_archive_policy_wrong_value(self):
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": "somenewname",
                        "definition": "foobar"},
                status=400)
        self.assertIn(b'Invalid input: expected a list '
                      b'for dictionary value @ data['
                      + repr(u'definition').encode('ascii') + b"]",
                      result.body)

    def test_post_archive_already_exists(self):
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": "high",
                        "definition": [{
                            "granularity": "10s",
                            "points": 20,
                        }]},
                status=409)
        self.assertIn('Archive policy high already exists', result.text)

    def test_create_archive_policy_with_granularity_integer(self):
        params = {"name": str(uuid.uuid4()),
                  "back_window": 0,
                  "definition": [{
                      "granularity": 10,
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params=params,
                status=201)
        ap = json.loads(result.text)
        params['definition'][0]['timespan'] = u'0:03:20'
        params['definition'][0]['granularity'] = u'0:00:10'
        self.assertEqual(params, ap)

    def test_create_archive_policy_with_back_window(self):
        params = {"name": str(uuid.uuid4()),
                  "back_window": 1,
                  "definition": [{
                      "granularity": "10s",
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params=params,
                status=201)
        ap = json.loads(result.text)
        params['definition'][0]['timespan'] = u'0:03:20'
        params['definition'][0]['granularity'] = u'0:00:10'
        self.assertEqual(params, ap)

    def test_get_archive_policy(self):
        result = self.app.get("/v1/archive_policy/medium")
        ap = json.loads(result.text)
        self.assertEqual(
            self.archive_policies['medium'].to_human_readable_dict(),
            ap)

    def test_delete_archive_policy(self):
        params = {"name": str(uuid.uuid4()),
                  "back_window": 1,
                  "definition": [{
                      "granularity": "10s",
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params=params)
            self.app.delete("/v1/archive_policy/%s" % params['name'],
                            status=204)

    def test_delete_archive_policy_non_existent(self):
        ap = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.delete("/v1/archive_policy/%s" % ap,
                                     status=404)
        self.assertIn(
            b"Archive policy " + ap.encode('ascii') + b" does not exist",
            result.body)

    def test_delete_archive_policy_in_use(self):
        ap = str(uuid.uuid4())
        params = {"name": ap,
                  "back_window": 1,
                  "definition": [{
                      "granularity": "10s",
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params=params)
        self.app.post_json("/v1/metric",
                           params={"archive_policy_name": ap})
        with self.app.use_admin_user():
            result = self.app.delete("/v1/archive_policy/%s" % ap,
                                     status=400)
        self.assertIn(
            b"Archive policy " + ap.encode('ascii') + b" is still in use",
            result.body)

    def test_get_archive_policy_non_existent(self):
        with self.app.use_admin_user():
            self.app.get("/v1/archive_policy/" + str(uuid.uuid4()),
                         status=404)

    def test_list_archive_policy(self):
        result = self.app.get("/v1/archive_policy")
        aps = json.loads(result.text)
        for name, ap in six.iteritems(self.archive_policies):
            self.assertIn(ap.to_human_readable_dict(), aps)


class MetricTest(RestTest):
    def test_post_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)
        metric = json.loads(result.text)
        self.assertEqual("http://localhost/v1/metric/" + metric['id'],
                         result.headers['Location'])
        self.assertEqual(metric['archive_policy_name'], "medium")

    def test_get_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)

        result = self.app.get(result.headers['Location'], status=200)
        metric = json.loads(result.text)
        self.assertEqual(metric['archive_policy_name'], "medium")

    def test_get_metric_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)

        with self.app.use_another_user():
            self.app.get(result.headers['Location'], status=403)

    def test_get_detailed_metric(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"},
            status=201)

        result = self.app.get(result.headers['Location'] + '?details=true',
                              status=200)
        metric = json.loads(result.text)
        self.assertEqual(
            self.archive_policies['medium'].to_human_readable_dict(),
            metric['archive_policy'])

    def test_get_metric_with_detail_in_accept(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"},
            status=201)

        result = self.app.get(
            result.headers['Location'],
            headers={"Accept": "application/json; details=true"},
            status=200)

        metric = json.loads(result.text)
        self.assertEqual(
            self.archive_policies['medium'].to_human_readable_dict(),
            metric['archive_policy'])

    def test_get_detailed_metric_with_bad_details(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"},
            status=201)

        result = self.app.get(result.headers['Location'] + '?details=awesome',
                              status=400)
        self.assertIn(
            b"Unable to parse details value in query: "
            b"Unrecognized value 'awesome', acceptable values are",
            result.body)

    def test_get_metric_with_bad_detail_in_accept(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"},
            status=201)

        result = self.app.get(
            result.headers['Location'],
            headers={"Accept": "application/json; details=awesome"},
            status=400)
        self.assertIn(
            b"Unable to parse details value in Accept: "
            b"Unrecognized value 'awesome', acceptable values are",
            result.body)

    def test_get_metric_with_wrong_metric_id(self):
        fake_metric_id = uuid.uuid4()
        result = self.app.get("/v1/metric/%s" % fake_metric_id, status=404)
        self.assertIn("Metric %s does not exist" % fake_metric_id, result.text)

    def test_post_metric_wrong_archive_policy(self):
        policy = str(uuid.uuid4())
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": policy},
                                    expect_errors=True,
                                    status=400)
        self.assertIn('Unknown archive policy %s' % policy, result.text)

    def test_list_metric(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"},
            status=201)
        metric = json.loads(result.text)
        result = self.app.get("/v1/metric")
        self.assertIn(metric['id'],
                      [r['id'] for r in json.loads(result.text)])
        result = self.app.get("/v1/metric?user_id=" + FakeMemcache.USER_ID)
        self.assertIn(metric['id'],
                      [r['id'] for r in json.loads(result.text)])

    def test_list_metric_filter_as_admin(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        with self.app.use_admin_user():
            result = self.app.get("/v1/metric?user_id=" + FakeMemcache.USER_ID)
        self.assertIn(metric['id'],
                      [r['id'] for r in json.loads(result.text)])

    def test_list_metric_invalid_user(self):
        result = self.app.get("/v1/metric?user_id=" + FakeMemcache.USER_ID_2,
                              status=403)
        self.assertIn("Insufficient privileges to filter by user/project",
                      result.text)

    def test_delete_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        result = self.app.delete("/v1/metric/" + metric['id'], status=204)

    def test_delete_metric_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        with self.app.use_another_user():
            self.app.delete("/v1/metric/" + metric['id'], status=403)

    def test_delete_metric_non_existent(self):
        e1 = str(uuid.uuid4())
        result = self.app.delete("/v1/metric/" + e1,
                                 expect_errors=True,
                                 status=404)
        self.assertIn(
            b"Metric " + e1.encode('ascii') + b" does not exist",
            result.body)

    def test_post_metric_bad_archives(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": 'foobar123'},
            expect_errors=True,
            status=400)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Unknown archive policy foobar123", result.body)

    def test_add_measure(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            status=204)

    def test_add_measure_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        with self.app.use_another_user():
            self.app.post_json(
                "/v1/metric/%s/measures" % metric['id'],
                params=[{"timestamp": '2013-01-01 23:23:23',
                         "value": 1234.2}],
                status=403)

    def test_add_multiple_measures_per_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"},
                                    status=201)
        metric = json.loads(result.text)
        for x in range(5):
            result = self.app.post_json(
                "/v1/metric/%s/measures" % metric['id'],
                params=[{"timestamp": '2013-01-01 23:23:2%d' % x,
                         "value": 1234.2 + x}],
                status=204)

    def test_add_measure_no_such_metric(self):
        e1 = str(uuid.uuid4())
        result = self.app.post_json(
            "/v1/metric/%s/measures" % e1,
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            expect_errors=True,
            status=404)
        self.assertIn(
            b"Metric " + e1.encode('ascii') + b" does not exist",
            result.body)

    def test_add_measures_back_window(self):
        ap_name = str(uuid.uuid4())
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": ap_name,
                        "back_window": 2,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                status=201)
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": ap_name})
        metric = json.loads(result.text)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:30:23',
                     "value": 1234.2}],
            status=204)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:29:23',
                     "value": 1234.2}],
            status=204)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:28:23',
                     "value": 1234.2}],
            status=204)
        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2012-01-01 23:27:23',
                     "value": 1234.2}],
            status=400)
        self.assertIn(
            b"The measure for 2012-01-01 23:27:23 is too old considering "
            b"the archive policy used by this metric. "
            b"It can only go back to 2013-01-01 23:28:00.",
            result.body)

        ret = self.app.get("/v1/metric/%s/measures" % metric['id'])
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T23:28:00.000000Z', 60.0, 1234.2],
             [u'2013-01-01T23:29:00.000000Z', 60.0, 1234.2],
             [u'2013-01-01T23:30:00.000000Z', 60.0, 1234.2]],
            result)

    def test_add_measures_too_old(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            status=204)

        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2012-01-01 23:23:23',
                     "value": 1234.2}],
            expect_errors=True,
            status=400)
        self.assertIn(
            b"The measure for 2012-01-01 23:23:23 is too old considering "
            b"the archive policy used by this metric. "
            b"It can only go back to 2013-01-01 00:00:00",
            result.body)

    def test_get_measure(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get("/v1/metric/%s/measures" % metric['id'], status=200)
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T00:00:00.000000Z', 86400.0, 1234.2],
             [u'2013-01-01T23:00:00.000000Z', 3600.0, 1234.2],
             [u'2013-01-01T23:20:00.000000Z', 300.0, 1234.2]],
            result)

    def test_get_measure_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        with self.app.use_another_user():
            self.app.get("/v1/metric/%s/measures" % metric['id'],
                         status=403)

    def test_get_measure_start(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?start=2013-01-01 23:23:20"
            % metric['id'],
            status=200)
        result = json.loads(ret.text)
        self.assertEqual([['2013-01-01T23:23:23.000000Z', 1.0, 1234.2]],
                         result)

    def test_get_measure_start_relative(self):
        """Make sure the timestamps can be relative to now."""
        # TODO(jd) Use a fixture as soon as there's one
        timeutils.set_time_override()
        self.addCleanup(timeutils.clear_time_override)
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": timeutils.isotime(),
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?start=-10 minutes"
            % metric['id'],
            status=200)
        result = json.loads(ret.text)
        now = timeutils.utcnow()
        self.assertEqual([
            [timeutils.isotime(now
                               - datetime.timedelta(
                                   seconds=now.second,
                                   microseconds=now.microsecond),
                               subsecond=True),
             60.0, 1234.2],
            [timeutils.isotime(now
                               - datetime.timedelta(
                                   microseconds=now.microsecond),
                               subsecond=True), 1.0, 1234.2]], result)

    def test_get_measure_stop(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        ret = self.app.get("/v1/metric/%s/measures"
                           "?stop=2013-01-01 12:00:01" % metric['id'],
                           status=200)
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T12:00:00.000000Z', 3600.0, 845.1],
             [u'2013-01-01T12:00:00.000000Z', 60.0, 845.1],
             [u'2013-01-01T12:00:00.000000Z', 1.0, 1234.2]],
            result)

    def test_get_measure_aggregation(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 123.2},
                                   {"timestamp": '2013-01-01 12:00:03',
                                    "value": 12345.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?aggregation=max" % metric['id'],
            status=200)
        result = json.loads(ret.text)
        self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 12345.2],
                          [u'2013-01-01T12:00:00.000000Z', 3600.0, 12345.2],
                          [u'2013-01-01T12:00:00.000000Z', 60.0, 12345.2]],
                         result)

    def test_get_moving_average(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 69},
                                   {"timestamp": '2013-01-01 12:00:20',
                                    "value": 42},
                                   {"timestamp": '2013-01-01 12:00:40',
                                    "value": 6},
                                   {"timestamp": '2013-01-01 12:01:00',
                                    "value": 44},
                                   {"timestamp": '2013-01-01 12:01:20',
                                    "value": 7}])

        path = "/v1/metric/%s/measures?aggregation=%s&window=%ds"
        ret = self.app.get(path % (metric['id'], 'moving-average', 120),
                           status=200)
        result = json.loads(ret.text)
        expected = [[u'2013-01-01T12:00:00.000000Z', 120.0, 32.25]]
        self.assertEqual(expected, result)
        ret = self.app.get(path % (metric['id'], 'moving-average', 90),
                           status=400)
        self.assertIn('No data available that is either full-res',
                      ret.text)
        path = "/v1/metric/%s/measures?aggregation=%s"
        ret = self.app.get(path % (metric['id'], 'moving-average'),
                           status=400)
        self.assertIn('Moving aggregate must have window specified',
                      ret.text)

    def test_get_moving_average_invalid_window(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 69},
                                   {"timestamp": '2013-01-01 12:00:20',
                                    "value": 42},
                                   {"timestamp": '2013-01-01 12:00:40',
                                    "value": 6},
                                   {"timestamp": '2013-01-01 12:01:00',
                                    "value": 44},
                                   {"timestamp": '2013-01-01 12:01:20',
                                    "value": 7}])

        path = "/v1/metric/%s/measures?aggregation=%s&window=foobar"
        ret = self.app.get(path % (metric['id'], 'moving-average'),
                           status=400)
        self.assertIn('Invalid value for window', ret.text)


class ResourceTest(RestTest):

    resource_scenarios = [
        ('generic', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000",
            },
            resource_type='generic')),
        ('instance', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000",
                # NOTE(jd) We test this one without user_id/project_id!
                # Just to test that use case. :)
                "host": "foo",
                "image_ref": "imageref!",
                "flavor_id": 123,
                "display_name": "myinstance",
                "server_group": "as_group",
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000",
                "host": "fooz",
                "image_ref": "imageref!z",
                "flavor_id": 1234,
                "display_name": "myinstancez",
                "server_group": "new_as_group",
            },
            resource_type='instance')),
        ('swift_account', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000",
            },
            resource_type='swift_account')),
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
        self.resource = self.attributes.copy()
        self.resource['created_by_user_id'] = FakeMemcache.USER_ID
        self.resource['created_by_project_id'] = FakeMemcache.PROJECT_ID
        self.resource['type'] = self.resource_type
        self.resource['ended_at'] = None
        self.resource['metrics'] = {}
        if 'user_id' not in self.resource:
            self.resource['user_id'] = None
        if 'project_id' not in self.resource:
            self.resource['project_id'] = None

    def test_post_resource(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=201)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/" + self.attributes['id'],
                         result.headers['Location'])
        self.assertEqual(resource, self.resource)

    def test_post_resource_with_invalid_metric(self):
        metric_id = str(uuid.uuid4())
        self.attributes['metrics'] = {"foo": metric_id}
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=400)
        self.assertIn("Metric %s does not exist" % metric_id,
                      result.text)

    def test_post_resource_with_metric_from_other_user(self):
        with self.app.use_another_user():
            metric = self.app.post_json(
                "/v1/metric",
                params={'archive_policy_name': "high"})
        metric_id = json.loads(metric.text)['id']
        self.attributes['metrics'] = {"foo": metric_id}
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=400)
        self.assertIn("Metric %s does not exist" % metric_id,
                      result.text)

    def test_post_resource_already_exist(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=201)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True,
            status=409)
        self.assertIn("Resource %s already exists" % self.attributes['id'],
                      result.text)

    def test_post_unix_timestamp(self):
        self.attributes['started_at'] = "1400580045.856219"
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=201)
        resource = json.loads(result.text)
        self.assertEqual(u"2014-05-20T10:00:45.856219",
                         resource['started_at'])

    def test_post_invalid_timestamp(self):
        self.attributes['started_at'] = "2014-01-01 02:02:02"
        self.attributes['ended_at'] = "2013-01-01 02:02:02"
        self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True,
            status=400)

    def test_get_resource(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual(self.resource, result)

    def test_get_resource_non_admin(self):
        with self.app.use_another_user():
            self.app.post_json("/v1/resource/" + self.resource_type,
                               params=self.attributes,
                               status=201)
            self.app.get("/v1/resource/"
                         + self.resource_type
                         + "/"
                         + self.attributes['id'],
                         status=200)

    def test_get_resource_unauthorized(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        with self.app.use_another_user():
            self.app.get("/v1/resource/"
                         + self.resource_type
                         + "/"
                         + self.attributes['id'],
                         status=403)

    def test_get_resource_named_metric(self):
        self.attributes['metrics'] = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.get("/v1/resource/"
                     + self.resource_type
                     + "/"
                     + self.attributes['id']
                     + "/metric/foo/measures",
                     status=200)

    def test_delete_resource_named_metric(self):
        self.attributes['metrics'] = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.delete("/v1/resource/"
                        + self.resource_type
                        + "/"
                        + self.attributes['id']
                        + "/metric/foo",
                        status=204)
        self.app.delete("/v1/resource/"
                        + self.resource_type
                        + "/"
                        + self.attributes['id']
                        + "/metric/foo/measures",
                        expect_errors=True,
                        status=404)

    def test_get_resource_unknown_named_metric(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.get("/v1/resource/"
                     + self.resource_type
                     + "/"
                     + self.attributes['id']
                     + "/metric/foo",
                     expect_errors=True,
                     status=404)

    def test_post_append_metrics_already_exists(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        metrics = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id'] + "/metric",
                           params=metrics, status=204)
        metrics = {'foo': {'archive_policy_name': "low"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id']
                           + "/metric",
                           params=metrics,
                           expect_errors=True,
                           status=409)

        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['metrics']['foo']))

    def test_post_append_metrics(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        metrics = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id'] + "/metric",
                           params=metrics, status=204)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['metrics']['foo']))

    def test_post_append_metrics_created_by_different_user(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            metric = self.app.post_json(
                "/v1/metric",
                params={'archive_policy_name': "high"})
        metric_id = json.loads(metric.text)['id']
        result = self.app.post_json("/v1/resource/" + self.resource_type
                                    + "/" + self.attributes['id'] + "/metric",
                                    params={str(uuid.uuid4()): metric_id},
                                    status=400)
        self.assertIn("Metric %s does not exist" % metric_id, result.text)

    def test_patch_resource_metrics(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        r = json.loads(result.text)
        new_metrics = {'foo': {'archive_policy_name': "medium"}}
        self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'metrics': new_metrics},
            status=204)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['metrics']['foo']))
        del result['metrics']
        del r['metrics']
        self.assertEqual(r, result)

    def test_patch_resource_existent_metrics_from_another_user(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            result = self.app.post_json(
                "/v1/metric",
                params={'archive_policy_name': "medium"})
        metric_id = json.loads(result.text)['id']
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'metrics': {'foo': metric_id}},
            status=400)
        self.assertIn("Metric %s does not exist" % metric_id, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual(result['metrics'], {})

    def test_patch_resource_non_existent_metrics(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'metrics': {'foo': e1}},
            expect_errors=True,
            status=400)
        self.assertIn("Metric %s does not exist" % e1, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual(result['metrics'], {})

    def test_patch_resource_attributes(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + self.attributes['id'],
            params=self.patchable_attributes,
            status=204)
        result = self.app.get("/v1/resource/" + self.resource_type
                              + "/" + self.attributes['id'])
        result = json.loads(result.text)
        for k, v in six.iteritems(self.patchable_attributes):
            self.assertEqual(v, result[k])

    def test_patch_resource_attributes_unauthorized(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        with self.app.use_another_user():
            self.app.patch_json(
                "/v1/resource/" + self.resource_type
                + "/" + self.attributes['id'],
                params=self.patchable_attributes,
                status=403)

    def test_patch_resource_ended_at_before_started_at(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'ended_at': "2000-05-05 23:23:23"},
            expect_errors=True,
            status=400)

    def test_patch_resource_no_partial_update(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'ended_at': "2044-05-05 23:23:23",
                    'metrics': {"foo": e1}},
            expect_errors=True,
            status=400)
        self.assertIn("Metric %s does not exist" % e1, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual(self.resource, result)

    def test_patch_resource_non_existent(self):
        self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params={},
            expect_errors=True,
            status=404)

    def test_patch_resource_non_existent_with_body(self):
        self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params=self.patchable_attributes,
            expect_errors=True,
            status=404)

    def test_patch_resource_unknown_field(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'foobar': 123},
            expect_errors=True,
            status=400)
        self.assertIn(b'Invalid input: extra keys not allowed @ data['
                      + repr(u'foobar').encode('ascii') + b"]",
                      result.body)

    def test_delete_resource(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.delete("/v1/resource/" + self.resource_type + "/"
                        + self.attributes['id'],
                        status=204)

    def test_delete_resource_unauthorized(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            self.app.delete("/v1/resource/" + self.resource_type + "/"
                            + self.attributes['id'],
                            status=403)

    def test_delete_resource_non_existent(self):
        result = self.app.delete("/v1/resource/" + self.resource_type + "/"
                                 + self.attributes['id'],
                                 status=404)
        self.assertIn(
            "Resource %s does not exist" % self.attributes['id'],
            result.text)

    def test_post_resource_invalid_uuid(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params={"id": "foobar"},
                                    expect_errors=True,
                                    status=400)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Invalid input: not a valid value "
                      b"for dictionary value @ data["
                      + repr(u'id').encode('ascii') + b"]",
                      result.body)

    def test_post_resource_with_metrics(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.attributes['metrics'] = {"foo": metric['id']}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.resource['metrics'] = self.attributes['metrics']
        self.assertEqual(resource, self.resource)

    def test_post_resource_with_null_metrics(self):
        self.attributes['metrics'] = {"foo": {"archive_policy_name": "low"}}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.assertEqual(resource["id"], self.attributes['id'])
        metric_id = uuid.UUID(resource['metrics']['foo'])
        result = self.app.get("/v1/metric/" + str(metric_id) + "/measures",
                              status=200)

    def test_list_resources_with_null_field(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "?ended_at=",
                              status=200)
        self.assertGreaterEqual(len(json.loads(result.text)), 1)

    def test_list_resources_by_unknown_field(self):
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"foo": "bar"},
                              expect_errors=True,
                              status=400)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn("Resource " + self.resource_type
                      + " has no foo attribute",
                      result.text)

    def test_list_resources_by_user(self):
        u1 = str(uuid.uuid4())
        self.attributes['user_id'] = u1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)
        result = self.app.get("/v1/resource/generic",
                              params={"user_id": u1},
                              status=200)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"user_id": u1},
                              status=200)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

    def test_list_resources_by_project(self):
        p1 = str(uuid.uuid4())
        self.attributes['project_id'] = p1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)
        result = self.app.get("/v1/resource/generic",
                              params={"project_id": p1},
                              status=200)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"project_id": p1},
                              status=200)
        resources = json.loads(result.text)
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
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })
        g = json.loads(result.text)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = json.loads(result.text)
        result = self.app.get("/v1/resource/generic", status=200)
        resources = json.loads(result.text)
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
        resources = json.loads(result.text)
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
            "/v1/resource/generic/",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })
        g = json.loads(result.text)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = json.loads(result.text)
        result = self.app.get(
            "/v1/resource/generic/",
            params={"started_after": "2014-01-01"},
            status=200)
        resources = json.loads(result.text)
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
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_with_bad_details(self):
        result = self.app.get("/v1/resource/generic?details=awesome",
                              expect_errors=True,
                              status=400)
        self.assertIn(
            b"Unable to parse details value in query: "
            b"Unrecognized value 'awesome', acceptable values are",
            result.body)

    def test_list_resources_with_bad_details_in_accept(self):
        result = self.app.get("/v1/resource/generic",
                              headers={
                                  "Accept": "application/json; details=foo",
                              },
                              expect_errors=True,
                              status=400)
        self.assertIn(
            b"Unable to parse details value in Accept: "
            b"Unrecognized value 'foo', acceptable values are",
            result.body)

    def _do_test_list_resources_with_detail(self, request):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })
        g = json.loads(result.text)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = json.loads(result.text)
        result = request()
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 2)

        i_found = False
        g_found = False
        for r in resources:
            if r['id'] == str(g['id']):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(i['id']):
                i_found = True
                # Check we got all the details
                self.assertEqual(i, r)
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        result = self.app.get("/v1/resource/" + self.resource_type)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_with_details(self):
        self._do_test_list_resources_with_detail(
            lambda: self.app.get("/v1/resource/generic?details=true"))

    def test_list_resources_with_details_via_accept(self):
        self._do_test_list_resources_with_detail(
            lambda: self.app.get(
                "/v1/resource/generic",
                headers={"Accept": "application/json; details=true"}))

    def test_get_res_named_metric_measure_aggregated_policies_invalid(self):
        # NOTE(sileht): This is a bit ugly, but this is a workaround for this
        # webob bug:
        #  https://github.com/Pylons/webob/issues/164
        binary_kwargs = {}
        if six.PY3:
            rest.LOGICAL_AND = '+'
            binary_kwargs['encoding'] = 'utf-8'

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric1 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric1['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 16}])

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name":
                                            "no_granularity_match"})
        metric2 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric2['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 4}])

        # NOTE(sileht): because the database is never cleaned between each test
        # we must ensure that the query will not match resources from an other
        # test, to achieve this we set a different server_group on each test.
        server_group = str(uuid.uuid4())
        if self.resource_type == 'instance':
            self.attributes['server_group'] = server_group

        self.attributes['metrics'] = {'foo': metric1['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        self.attributes['id'] = str(uuid.uuid4())
        self.attributes['metrics'] = {'foo': metric2['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/server_group=" + server_group
                              + urllib_parse.quote(rest.LOGICAL_AND)
                              + "display_name=myinstance"
                              + "/metric/foo/measures?aggregation=max",
                              expect_errors=True)
        self.assertEqual(400, result.status_code, result.body)
        if self.resource_type == 'instance':
            self.assertIn(b"One of the metric to aggregated doesn't have "
                          b"matching granularity",
                          result.body)

    def test_get_res_named_metric_measure_aggregation_query_invalid(self):
        # NOTE(sileht): This is a bit ugly, but this is a workaround for this
        # webob bug:
        #  https://github.com/Pylons/webob/issues/164
        binary_kwargs = {}
        if six.PY3:
            rest.LOGICAL_AND = '+'
            binary_kwargs['encoding'] = 'utf-8'

        invalid_query = ("server_group"
                         + urllib_parse.quote(rest.LOGICAL_AND)
                         + "display_name=myinstance")

        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + invalid_query
                              + "/metric/foo/measures?aggregation=max",
                              expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertIn(b'server_group'
                      + six.binary_type(rest.LOGICAL_AND, **binary_kwargs)
                      + b'display_name=myinstance',
                      result.body)

    def test_get_res_named_metric_measure_aggregation_nominal(self):
        # NOTE(sileht): This is a bit ugly, but this is a workaround for this
        # webob bug:
        #  https://github.com/Pylons/webob/issues/164
        if six.PY3:
            rest.LOGICAL_AND = '+'

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric1 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric1['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 8},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 16}])

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric2 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric2['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 0},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 4}])

        # NOTE(sileht): because the database is never cleaned between each test
        # we must ensure that the query will not match resources from an other
        # test, to achieve this we set a different server_group on each test.
        server_group = str(uuid.uuid4())
        if self.resource_type == 'instance':
            self.attributes['server_group'] = server_group

        self.attributes['metrics'] = {'foo': metric1['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        self.attributes['id'] = str(uuid.uuid4())
        self.attributes['metrics'] = {'foo': metric2['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/server_group=" + server_group
                              + urllib_parse.quote(rest.LOGICAL_AND)
                              + "display_name=myinstance"
                              + "/metric/foo/measures?aggregation=max",
                              expect_errors=True)

        if self.resource_type == 'instance':
            self.assertEqual(200, result.status_code, result.text)
            measures = json.loads(result.text)
            self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 16.0],
                              [u'2013-01-01T12:00:00.000000Z', 3600.0, 16.0],
                              [u'2013-01-01T12:00:00.000000Z', 60.0, 16.0]],
                             measures)
        else:
            self.assertEqual(400, result.status_code)

        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/server_group=" + server_group
                              + urllib_parse.quote(rest.LOGICAL_AND)
                              + "display_name=myinstance"
                              + "/metric/foo/measures?aggregation=min",
                              expect_errors=True)

        if self.resource_type == 'instance':
            self.assertEqual(200, result.status_code)
            measures = json.loads(result.text)
            self.assertEqual([['2013-01-01T00:00:00.000000Z', 86400.0, 0],
                              ['2013-01-01T12:00:00.000000Z', 3600.0, 0],
                              ['2013-01-01T12:00:00.000000Z', 60.0, 0]],
                             measures)
        else:
            self.assertEqual(400, result.status_code)


class GenericResourceTest(RestTest):
    def test_list_resources_tied_to_user(self):
        resource_id = str(uuid.uuid4())
        self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": resource_id,
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })

        with self.app.use_another_user():
            result = self.app.get("/v1/resource/generic")
            resources = json.loads(result.text)
            for resource in resources:
                if resource['id'] == resource_id:
                    self.fail("Resource found")


ResourceTest.generate_scenarios()
