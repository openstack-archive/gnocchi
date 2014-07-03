# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Openstack Foundation
#
# Authors: Ana Malagon <atmalagon@gmail.com>
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

import testscenarios

from gnocchi.aggregates import rolling_statistics
from gnocchi import aggregates
from gnocchi import storage
from gnocchi.storage import swift
from gnocchi import tests

from stevedore import extension

load_tests = testscenarios.load_tests_apply_scenarios


class TestAggregation(tests.TestCase):
    def _setup_entity(self, data, policy, spacing=1):
        self.conf.set_override('driver', 'swift', 'storage')
        driver = storage.get_driver(self.conf)
        self.assertIsInstance(driver, swift.SwiftStorage)

        self.storage.create_entity("foo", self.archive_policies[policy])

        start_time = datetime.datetime(2014, 1, 1, 12, 0, 0)
        sec = datetime.datetime.timedelta(seconds=spacing)

        measures = [storage.Measure(start_time + sec * n, data[n])
                    for n in range(len(data))]
        self.storage.add_measures('foo', measures)

    def _verify_aggregates(self, values, expected, start_time, spacing=1):

        self.assertEqual(len(expected), len(values))

        sec = datetime.datetime.timedelta(seconds=spacing)
        for i in range(len(values)):
            self.assertEqual(expected[i], values[start_time + i * sec])

    def _dict_of_functions(self):
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)

        self.custom_aggregates = dict((x.name, x.obj) for x in mgr)
        self.assertIsInstance(self.custom_aggregates['moving-average'],
                              rolling_statistics.RollingMean)
        self.assertIsInstance(self.custom_aggregates['moving-variance'],
                              rolling_statistics.RollingVariance)
        self.assertIsInstance(self.custom_aggregates['ewma'],
                              rolling_statistics.EWMA)
        return self.custom_aggregates

    def test_compute(self):

        self._setup_entity([69, 42, 4, 44], "high", spacing=1)
        aggregate_object = rolling_statistics.RollingMean
        values = aggregate_object.compute(self.indexer,
                                          self.storage, 'foo',
                                          '2014-01-01 12:00:01',
                                          '2014-01-01 12:00:04',
                                          window=2)
        self._verify_aggregates(values, [55.5, 23, 24], '2014-01-01 12:00:01',
                                spacing=1)
        self.storage.delete_entity("foo")

    def test_nans(self):
        self._setup_entity([69, 42, None, None], "high", spacing=1)
        aggregate_object = rolling_statistics.RollingMean
        values = aggregate_object.compute(self.indexer,
                                          self.storage, 'foo',
                                          '2014-01-01 12:00:01',
                                          '2014-01-01 12:00:04',
                                          window=2)
        self._verify_aggregates(values, [55.5, 42], '2014-01-01 12:00:01',
                                spacing=1)
        self.storage.delete_entity("foo")

    def test_window_multiple(self):
        self._setup_entity([69, 42, 4, 44], "medium", spacing=60)
        aggregate_object = rolling_statistics.RollingMean
        values = aggregate_object.compute(self.indexer,
                                          self.storage, 'foo',
                                          '2014-01-01 12:00:01',
                                          '2014-01-01 12:04:01',
                                          window=120)
        self._verify_aggregates(values, [55.5, 23, 24], '2014-01-01 12:00:01',
                                spacing=1)
        self.assertRaises(aggregates.CustomAggregationFailure,
                          rolling_statistics.RollingMean.compute,
                          self.indexer, self.storage,
                          'foo', '2014-01-01 12:00:01', '2014-01-01 12:04:01',
                          window=80)

        self.storage.delete_entity("foo")

    def test_window_specified(self):
        self._setup_entity([69, 42, 4, 44], "high", spacing=1)
        self.assertRaises(TypeError,
                          rolling_statistics.RollingMean.compute,
                          self.indexer, self.storage,
                          'foo', '2014-01-01 12:00:01', '2014-01-01 12:00:04',
                          window=None)

        self.storage.delete_entity("foo")
