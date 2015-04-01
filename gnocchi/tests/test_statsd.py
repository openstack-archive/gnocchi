# -*- encoding: utf-8 -*-
#
# Copyright © 2015 eNovance
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

import mock
from oslo_utils import timeutils

from gnocchi import statsd
from gnocchi import storage
from gnocchi.tests import base as tests_base


class TestStatsd(tests_base.TestCase):

    STATSD_USER_ID = uuid.uuid4()
    STATSD_PROJECT_ID = uuid.uuid4()
    STATSD_ARCHIVE_POLICY_NAME = "medium"

    def setUp(self):
        super(TestStatsd, self).setUp()

        self.conf.set_override("resource_id",
                               uuid.uuid4(), "statsd")
        self.conf.set_override("user_id",
                               self.STATSD_USER_ID, "statsd")
        self.conf.set_override("project_id",
                               self.STATSD_PROJECT_ID, "statsd")
        self.conf.set_override("archive_policy_name",
                               self.STATSD_ARCHIVE_POLICY_NAME, "statsd")

        # NOTE(jd) Always use self.server.storage and self.server.indexer to
        # pick at the right storage/indexer used by the statsd server, and not
        # new instances from the base test class.
        self.stats = statsd.Stats(self.conf)
        self.server = statsd.StatsdServer(self.stats)

    def test_flush_empty(self):
        self.server.stats.flush()

    @mock.patch.object(timeutils, 'utcnow')
    def _test_gauge_or_ms(self, metric_type, utcnow):
        metric_name = "test_gauge_or_ms"
        metric_key = metric_name + "|" + metric_type
        utcnow.return_value = datetime.datetime(2015, 1, 7, 13, 58, 36)
        self.server.datagram_received(
            ("%s:1|%s" % (metric_name, metric_type)).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        r = self.stats.indexer.get_resource('generic',
                                            self.conf.statsd.resource_id,
                                            with_metrics=True)

        measures = self.stats.storage.get_measures(storage.Metric(
            r.get_metric(metric_key), None))
        self.assertEqual([(datetime.datetime(2015, 1, 7), 86400.0, 1.0),
                          (datetime.datetime(2015, 1, 7, 13), 3600.0, 1.0),
                          (datetime.datetime(2015, 1, 7, 13, 58), 60.0, 1.0)],
                         measures)

        utcnow.return_value = datetime.datetime(2015, 1, 7, 13, 59, 37)
        # This one is going to be ignored
        self.server.datagram_received(
            ("%s:45|%s" % (metric_name, metric_type)).encode('ascii'),
            ("127.0.0.1", 12345))
        self.server.datagram_received(
            ("%s:2|%s" % (metric_name, metric_type)).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        measures = self.stats.storage.get_measures(storage.Metric(
            r.get_metric(metric_key), None))
        self.assertEqual([(datetime.datetime(2015, 1, 7), 86400.0, 1.5),
                          (datetime.datetime(2015, 1, 7, 13), 3600.0, 1.5),
                          (datetime.datetime(2015, 1, 7, 13, 58), 60.0, 1.0),
                          (datetime.datetime(2015, 1, 7, 13, 59), 60.0, 2.0)],
                         measures)

    def test_gauge(self):
        self._test_gauge_or_ms("g")

    def test_ms(self):
        self._test_gauge_or_ms("ms")

    @mock.patch.object(timeutils, 'utcnow')
    def test_counter(self, utcnow):
        metric_name = "test_counter"
        metric_key = metric_name + "|c"
        utcnow.return_value = datetime.datetime(2015, 1, 7, 13, 58, 36)
        self.server.datagram_received(
            ("%s:1|c" % metric_name).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        r = self.stats.indexer.get_resource('generic',
                                            self.conf.statsd.resource_id,
                                            with_metrics=True)

        measures = self.stats.storage.get_measures(storage.Metric(
            r.get_metric(metric_key), None))
        self.assertEqual([(datetime.datetime(2015, 1, 7), 86400.0, 1.0),
                          (datetime.datetime(2015, 1, 7, 13), 3600.0, 1.0),
                          (datetime.datetime(2015, 1, 7, 13, 58), 60.0, 1.0)],
                         measures)

        utcnow.return_value = datetime.datetime(2015, 1, 7, 13, 59, 37)
        self.server.datagram_received(
            ("%s:45|c" % metric_name).encode('ascii'),
            ("127.0.0.1", 12345))
        self.server.datagram_received(
            ("%s:2|c|@0.2" % metric_name).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        measures = self.stats.storage.get_measures(storage.Metric(
            r.get_metric(metric_key), None))
        self.assertEqual([(datetime.datetime(2015, 1, 7), 86400.0, 28),
                          (datetime.datetime(2015, 1, 7, 13), 3600.0, 28),
                          (datetime.datetime(2015, 1, 7, 13, 58), 60.0, 1.0),
                          (datetime.datetime(2015, 1, 7, 13, 59), 60.0, 55.0)],
                         measures)
