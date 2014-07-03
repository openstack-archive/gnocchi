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
from gnocchi import storage
from gnocchi.storage import swift
from gnocchi import tests

from stevedore import extension

load_tests = testscenarios.load_tests_apply_scenarios


class TestAggregation(tests.TestCase):
    def test_dict(self):
        self.conf.set_override('driver', 'swift', 'storage')
        driver = storage.get_driver(self.conf)
        self.assertIsInstance(driver, swift.SwiftStorage)
        self.storage.create_entity("foo", [(1, 1)])
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)

        self.custom_aggregates = dict((x.name, x.obj) for x in mgr)
        self.assertIsInstance(self.custom_aggregates['moving-average'],
                              rolling_statistics.RollingMean)
        self.assertIsInstance(self.custom_aggregates['moving-variance'],
                              rolling_statistics.RollingVariance)
        self.assertIsInstance(self.custom_aggregates['ewma'],
                              rolling_statistics.EWMA)

    def test_compute(self):
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)
        self.custom_aggregates = dict((x.name, x.obj) for x in mgr)
        self.storage.create_entity("foo", [(1, 5)])
        self.storage.add_measures('foo', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 2), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 3), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 4), 44),
        ])

        values = self.custom_aggregates['moving-average'].compute(
            self.storage, 'foo', '2014-01-01 12:00:01', '2014-01-01 12:00:04',
            granularity=2, center='False')
        self.assertEqual(4, len(values))
        self.assertEqual(69, values[datetime.datetime(2014, 1, 1, 12, 0, 1)])
        self.assertEqual(55.5, values[datetime.datetime(2014, 1, 1, 12, 0, 2)])
        self.assertEqual(23, values[datetime.datetime(2014, 1, 1, 12, 0, 3)])
        self.assertEqual(24, values[datetime.datetime(2014, 1, 1, 12, 0, 4)])
        self.storage.delete_entity("foo")
        # TODO(atmalagon): test for case when center='True' (both for
        # granularity even and odd), and case when
        # granularity is larger than archive granularity.
