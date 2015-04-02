# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import datetime
import uuid

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi.tests import base as tests_base


class TestIndexer(tests_base.TestCase):
    def test_get_driver(self):
        driver = indexer.get_driver(self.conf)
        self.assertIsInstance(driver, indexer.IndexerDriver)


class TestIndexerDriver(tests_base.TestCase):

    def test_create_archive_policy_already_exists(self):
        # NOTE(jd) This archive policy
        # is created by gnocchi.tests on setUp() :)
        self.assertRaises(indexer.ArchivePolicyAlreadyExists,
                          self.index.create_archive_policy,
                          archive_policy.ArchivePolicy("high", 0, {}))

    def test_get_archive_policy(self):
        ap = self.index.get_archive_policy("low")
        self.assertEqual(
            set(self.conf.archive_policy.default_aggregation_methods),
            set(ap['aggregation_methods']))
        del ap['aggregation_methods']
        self.assertEqual({
            'back_window': 0,
            'definition': [
                {u'granularity': 300, u'points': 12, u'timespan': 3600},
                {u'granularity': 3600, u'points': 24, u'timespan': 86400},
                {u'granularity': 86400, u'points': 30, u'timespan': 2592000}],
            'name': u'low'}, ap)

    def test_delete_archive_policy(self):
        name = str(uuid.uuid4())
        self.index.create_archive_policy(
            archive_policy.ArchivePolicy(name, 0, {}))
        self.index.delete_archive_policy(name)
        self.assertRaises(indexer.NoSuchArchivePolicy,
                          self.index.delete_archive_policy,
                          name)
        self.assertRaises(indexer.NoSuchArchivePolicy,
                          self.index.delete_archive_policy,
                          str(uuid.uuid4()))
        metric_id = uuid.uuid4()
        self.index.create_metric(metric_id, uuid.uuid4(),
                                 uuid.uuid4(), "low")
        self.assertRaises(indexer.ArchivePolicyInUse,
                          self.index.delete_archive_policy,
                          "low")
        self.index.delete_metric(metric_id)

    def test_create_metric(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        m = self.index.create_metric(r1, user, project, "low")
        self.assertEqual({"id": r1,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "archive_policy_name": "low",
                          "name": None,
                          "resource_id": None}, m)
        m2 = self.index.get_metrics([r1])
        self.assertEqual([m], m2)

    def test_create_resource(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        rc = self.index.create_resource('generic', r1, user, project)
        self.assertIsNotNone(rc['started_at'])
        self.assertIsNotNone(rc['lifetime_from'])
        del rc['started_at']
        self.assertEqual({"id": r1,
                          'lifetime_from': rc['lifetime_from'],
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "ended_at": None,
                          "type": "generic",
                          "metrics": {}},
                         rc)
        rg = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc['id'], rg['id'])
        self.assertEqual(rc['metrics'], rg['metrics'])

    def test_create_non_existent_metric(self):
        e = uuid.uuid4()
        try:
            self.index.create_resource(
                'generic', uuid.uuid4(), uuid.uuid4(), uuid.uuid4(),
                metrics={"foo": e})
        except indexer.NoSuchMetric as ex:
            self.assertEqual(e, ex.metric)
        else:
            self.fail("Exception not raised")

    def test_create_resource_already_exists(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('generic', r1, user, project)
        self.assertRaises(indexer.ResourceAlreadyExists,
                          self.index.create_resource,
                          'generic', r1, user, project)

    def _do_test_create_instance(self, server_group=None):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        kwargs = {'server_group': server_group} if server_group else {}

        rc = self.index.create_resource('instance', r1, user, project,
                                        flavor_id=1,
                                        image_ref="http://foo/bar",
                                        host="foo",
                                        display_name="lol", **kwargs)
        self.assertIsNotNone(rc['started_at'])
        self.assertIsNotNone(rc['lifetime_from'])
        del rc['started_at']
        self.assertEqual({"id": r1,
                          "lifetime_from": rc['lifetime_from'],
                          "type": "instance",
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "ended_at": None,
                          "display_name": "lol",
                          "server_group": server_group,
                          "host": "foo",
                          "image_ref": "http://foo/bar",
                          "flavor_id": 1,
                          "metrics": {}},
                         rc)
        rg = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc['id'], rg['id'])
        self.assertEqual(rc['lifetime_from'], rg['lifetime_from'])
        self.assertEqual(rc['metrics'], rg['metrics'])

    def test_create_instance(self):
        self._do_test_create_instance()

    def test_create_instance_with_server_group(self):
        self._do_test_create_instance('my_autoscaling_group')

    def test_delete_resource(self):
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, uuid.uuid4(), uuid.uuid4())

        class Boom(Exception):
            pass

        def delete_metrics(metrics):
            raise Boom

        self.assertRaises(Boom,
                          self.index.delete_resource,
                          r1, delete_metrics=delete_metrics)
        self.index.delete_resource(r1)
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)

    def test_delete_resource_non_existent(self):
        r1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)

    def test_create_resource_with_start_timestamp(self):
        r1 = uuid.uuid4()
        ts = datetime.datetime(2014, 1, 1, 23, 34, 23, 1234)
        user = uuid.uuid4()
        project = uuid.uuid4()
        rc = self.index.create_resource(
            'generic',
            r1, user, project,
            started_at=ts)
        self.assertEqual({"id": r1,
                          "lifetime_from": rc['lifetime_from'],
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "started_at": ts,
                          "ended_at": None,
                          "type": "generic",
                          "metrics": {}}, rc)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, r)

    def test_create_resource_with_metrics(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2,
                                 user, project,
                                 archive_policy_name="low")
        rc = self.index.create_resource('generic', r1, user, project,
                                        metrics={'foo': e1, 'bar': e2})
        self.assertIsNotNone(rc['started_at'])
        self.assertIsNotNone(rc['lifetime_from'])
        del rc['started_at']
        self.assertEqual({"id": r1,
                          "lifetime_from": rc['lifetime_from'],
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "ended_at": None,
                          "type": "generic",
                          "metrics": {'foo': str(e1), 'bar': str(e2)}}, rc)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": r1,
                          "lifetime_from": r['lifetime_from'],
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "type": "generic",
                          "ended_at": None,
                          "user_id": None,
                          "project_id": None,
                          "metrics": {'foo': str(e1), 'bar': str(e2)}}, r)

    def test_update_non_existent_resource_end_timestamp(self):
        r1 = uuid.uuid4()
        self.assertRaises(
            indexer.NoSuchResource,
            self.index.update_resource,
            'generic',
            r1,
            ended_at=datetime.datetime(2014, 1, 1, 2, 3, 4))

    def test_update_resource_end_timestamp(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('generic', r1, user, project)
        self.index.update_resource(
            'generic',
            r1,
            ended_at=datetime.datetime(2043, 1, 1, 2, 3, 4))
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": r1,
                          "lifetime_from": r['lifetime_from'],
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "ended_at": datetime.datetime(2043, 1, 1, 2, 3, 4),
                          "user_id": None,
                          "project_id": None,
                          "type": "generic",
                          "metrics": {}}, r)
        self.index.update_resource(
            'generic',
            r1,
            ended_at=None)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r['started_at'])
        self.assertIsNotNone(r['lifetime_from'])
        del r['started_at']
        self.assertEqual({"id": r1,
                          "lifetime_from": r['lifetime_from'],
                          "ended_at": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "type": "generic",
                          "metrics": {}}, r)

    def test_update_resource_metrics(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1})
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        rc = self.index.update_resource('generic', r1, metrics={'bar': e2})
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, r)

    def test_update_resource_metrics_append(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1})
        rc = self.index.update_resource('generic', r1, metrics={'bar': e2},
                                        append_metrics=True)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, r)
        self.assertIn('foo', rc['metrics'])
        self.assertIn('bar', rc['metrics'])

    def test_update_resource_metrics_append_fail(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1})

        self.assertRaises(indexer.NamedMetricAlreadyExists,
                          self.index.update_resource,
                          'generic', r1, metrics={'foo': e2},
                          append_metrics=True)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(str(e1), r['metrics']['foo'])

    def test_update_resource_attribute(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        rc = self.index.create_resource('instance', r1, user, project,
                                        flavor_id=1,
                                        image_ref="http://foo/bar",
                                        host="foo",
                                        display_name="lol")
        rc = self.index.update_resource('instance', r1, host="bar")
        r = self.index.get_resource('instance', r1, with_metrics=True)
        self.assertEqual(rc, r)

    def test_update_resource_unknown_attribute(self):
        r1 = uuid.uuid4()
        self.index.create_resource('instance', r1, uuid.uuid4(), uuid.uuid4(),
                                   flavor_id=1,
                                   image_ref="http://foo/bar",
                                   host="foo",
                                   display_name="lol")
        self.assertRaises(indexer.ResourceAttributeError,
                          self.index.update_resource,
                          'instance',
                          r1, foo="bar")

    def test_update_non_existent_metric(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_resource('generic', r1, uuid.uuid4(), uuid.uuid4())
        self.assertRaises(indexer.NoSuchMetric,
                          self.index.update_resource,
                          'generic',
                          r1, metrics={'bar': e1})

    def test_update_non_existent_resource(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_metric(e1, uuid.uuid4(), uuid.uuid4(),
                                 archive_policy_name="low")
        self.assertRaises(indexer.NoSuchResource,
                          self.index.update_resource,
                          'generic',
                          r1, metrics={'bar': e1})

    def test_create_resource_with_non_existent_metrics(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchMetric,
                          self.index.create_resource,
                          'generic',
                          r1, uuid.uuid4(), uuid.uuid4(),
                          metrics={'foo': e1})

    def test_delete_metric(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        rc = self.index.create_resource('generic', r1, user, project,
                                        metrics={'foo': e1, 'bar': e2})
        self.index.delete_metric(e1)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r['started_at'])
        self.assertIsNotNone(r['lifetime_from'])
        del r['started_at']
        self.assertEqual({"id": r1,
                          "lifetime_from": rc['lifetime_from'],
                          "ended_at": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "type": "generic",
                          "metrics": {'bar': str(e2)}}, r)

    def test_delete_instance(self):
        r1 = uuid.uuid4()
        created = self.index.create_resource('instance', r1,
                                             uuid.uuid4(), uuid.uuid4(),
                                             flavor_id=123,
                                             image_ref="foo",
                                             host="dwq",
                                             display_name="foobar")
        got = self.index.get_resource('instance', r1, with_metrics=True)
        self.assertEqual(created, got)
        self.index.delete_resource(r1)
        got = self.index.get_resource('instance', r1)
        self.assertIsNone(got)

    def test_list_resources_by_unknown_field(self):
        self.assertRaises(indexer.ResourceAttributeError,
                          self.index.list_resources,
                          'generic',
                          attribute_filter={"=": {"fern": "bar"}})

    def test_list_resources_by_user(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project,
                                       user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": user}})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": uuid.uuid4()}})
        self.assertEqual(0, len(resources))

    def test_list_resources_by_created_by_user(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"created_by_user_id": user}})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"created_by_user_id": uuid.uuid4()}})
        self.assertEqual(0, len(resources))

    def test_list_resources_by_user_with_details(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project,
                                       user, project)
        r2 = uuid.uuid4()
        i = self.index.create_resource('instance', r2,
                                       user, project,
                                       user, project,
                                       flavor_id=123,
                                       image_ref="foo",
                                       host="dwq",
                                       display_name="foobar")
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": user}},
            details=True,
        )
        self.assertEqual(2, len(resources))
        self.assertIn(g, resources)
        self.assertIn(i, resources)

    def test_list_resources_by_project(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project,
                                       user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"project_id": project}})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"project_id": uuid.uuid4()}})
        self.assertEqual(0, len(resources))

    def test_list_resources(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        g = self.index.create_resource('generic', r1,
                                       uuid.uuid4(), uuid.uuid4())
        r2 = uuid.uuid4()
        i = self.index.create_resource('instance', r2,
                                       uuid.uuid4(), uuid.uuid4(),
                                       flavor_id=123,
                                       image_ref="foo",
                                       host="dwq",
                                       display_name="foobar")
        resources = self.index.list_resources('generic')
        self.assertGreaterEqual(len(resources), 2)
        g_found = False
        i_found = False
        for r in resources:
            if r['id'] == r1:
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == r2:
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources('instance')
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == r2:
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_without_history(self):
        e = uuid.uuid4()
        rid = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        new_user = uuid.uuid4()
        new_project = uuid.uuid4()

        self.index.create_metric(e, user, project,
                                 archive_policy_name="low")

        self.index.create_resource('generic', rid, user, project,
                                   user, project,
                                   metrics={'foo': e})
        r2 = self.index.update_resource('generic', rid, user_id=new_user,
                                        project_id=new_project,
                                        append_metrics=True)

        self.assertEqual({'foo': str(e)}, r2['metrics'])
        self.assertEqual(new_user, r2['user_id'])
        self.assertEqual(new_project, r2['project_id'])
        resources = self.index.list_resources('generic', history=False,
                                              details=True)
        self.assertGreaterEqual(len(resources), 1)
        expected_resources = [r for r in resources
                              if r['id'] == rid]
        self.assertIn(r2, expected_resources)

    def test_list_resources_with_history(self):
        e = uuid.uuid4()
        rid = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        new_user = uuid.uuid4()
        new_project = uuid.uuid4()

        self.index.create_metric(e, user, project,
                                 archive_policy_name="low")

        r1 = self.index.create_resource('generic', rid, user, project,
                                        user, project,
                                        metrics={'foo': e})
        r2 = self.index.update_resource('generic', rid, user_id=new_user,
                                        project_id=new_project,
                                        append_metrics=True)

        r1['lifetime_to'] = r2['lifetime_from']
        self.assertEqual({'foo': str(e)}, r2['metrics'])
        self.assertEqual(new_user, r2['user_id'])
        self.assertEqual(new_project, r2['project_id'])
        resources = self.index.list_resources('generic', history=True,
                                              details=True)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual([r1, r2], [r for r in resources if r['id'] == rid])

    def test_list_resources_started_after_ended_before(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource(
            'generic', r1, user, project,
            started_at=datetime.datetime(2000, 1, 1, 23, 23, 23),
            ended_at=datetime.datetime(2000, 1, 3, 23, 23, 23))
        r2 = uuid.uuid4()
        i = self.index.create_resource(
            'instance', r2, user, project,
            flavor_id=123,
            image_ref="foo",
            host="dwq",
            display_name="foobar",
            started_at=datetime.datetime(2000, 1, 1, 23, 23, 23),
            ended_at=datetime.datetime(2000, 1, 4, 23, 23, 23))
        resources = self.index.list_resources(
            'generic',
            attribute_filter={
                "and":
                [{">=": {"started_at":
                         datetime.datetime(2000, 1, 1, 23, 23, 23)}},
                 {"<": {"ended_at":
                        datetime.datetime(2000, 1, 5, 23, 23, 23)}}]})
        self.assertGreaterEqual(len(resources), 2)
        g_found = False
        i_found = False
        for r in resources:
            if r['id'] == r1:
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == r2:
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(
            'instance',
            attribute_filter={
                ">=": {
                    "started_at": datetime.datetime(2000, 1, 1, 23, 23, 23)
                },
            })
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == r2:
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(
            'generic',
            attribute_filter={
                "<": {
                    "ended_at": datetime.datetime(1999, 1, 1, 23, 23, 23)
                },
            })
        self.assertEqual(0, len(resources))

    def test_get_metric(self):
        e1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")

        metric = self.index.get_metrics([e1])
        self.assertEqual([{"id": e1,
                           "archive_policy_name": "low",
                           "created_by_user_id": user,
                           "created_by_project_id": project,
                           "name": None,
                           "resource_id": None}],
                         metric)

    def test_get_metric_with_details(self):
        e1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")

        metric = self.index.get_metrics([e1], details=True)
        # Order of the list is random, use a set then
        metric[0]['archive_policy']['aggregation_methods'] = set(
            metric[0]['archive_policy']['aggregation_methods'])
        self.assertEqual([
            {"id": e1,
             "archive_policy": {
                 'aggregation_methods':
                 set(self.conf.archive_policy.default_aggregation_methods),
                 "back_window": 0,
                 "definition": [
                     {'granularity': 300,
                      'points': 12,
                      'timespan': 3600},
                     {'granularity': 3600,
                      'points': 24,
                      'timespan': 86400},
                     {'granularity': 86400,
                      'points': 30,
                      'timespan': 2592000}],
                 "name": "low"},
             "created_by_user_id": user,
             "created_by_project_id": project,
             "name": None,
             "resource_id": None}
        ], metric)

    def test_get_metric_with_bad_uuid(self):
        e1 = uuid.uuid4()
        self.assertEqual([], self.index.get_metrics([e1]))

    def test_get_metric_empty_list_uuids(self):
        self.assertEqual([], self.index.get_metrics([]))

    def test_get_metric_no_args(self):
        self.assertRaises(TypeError, self.index.get_metrics, *[])
