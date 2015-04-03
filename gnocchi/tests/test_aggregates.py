# -*- encoding: utf-8 -*-
#
# Copyright 2014-2015 Openstack Foundation
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

import pandas

from gnocchi import aggregates
from gnocchi.aggregates import moving_stats
from gnocchi import storage
from gnocchi.tests import base as tests_base


class TestAggregates(tests_base.TestCase):

    def test_extension_dict(self):
        self.assertIsInstance(self.custom_agg['moving-average'],
                              moving_stats.MovingAverage)

    def test_check_window_valid(self):
        for agg_method in self.custom_agg:
            window = '60s'
            agg_obj = self.custom_agg[agg_method]
            result = agg_obj.check_window_valid(window)
            self.assertEqual(60.0, result)

            window = '60'
            self.assertRaises(aggregates.CustomAggFailure,
                              agg_obj.check_window_valid,
                              window)

            window = None
            self.assertRaises(aggregates.CustomAggFailure,
                              agg_obj.check_window_valid,
                              window)

    def _test_create_metric_and_data(self, data, spacing):
        metric = storage.Metric(
            uuid.uuid4(), self.archive_policies['medium'])
        self.storage.create_metric(metric)
        start_time = datetime.datetime(2014, 1, 1, 12)
        incr = datetime.timedelta(seconds=spacing)
        measures = [storage.Measure(start_time + incr * n, val)
                    for n, val in enumerate(data)]
        self.storage.add_measures(metric, measures)
        return metric

    def test_retrieve_data(self):
        metric = self._test_create_metric_and_data([69, 42, 6, 44, 7],
                                                   spacing=20)
        for agg_method in self.custom_agg:
            agg_obj = self.custom_agg[agg_method]
            window = 90.0
            self.assertRaises(aggregates.CustomAggFailure,
                              agg_obj.retrieve_data,
                              self.storage, metric,
                              start=None, stop=None,
                              window=window)

            window = 120.0
            result = pandas.Series()
            grain, result = agg_obj.retrieve_data(self.storage, metric,
                                                  start=None, stop=None,
                                                  window=window)
            self.assertEqual(60.0, grain)
            self.assertEqual(39.0, result[datetime.datetime(2014, 1, 1, 12)])
            self.assertEqual(25.5,
                             result[datetime.datetime(2014, 1, 1, 12, 1)])
        self.storage.delete_metric(metric)

    def test_compute_moving_average(self):
        metric = self._test_create_metric_and_data([69, 42, 6, 44, 7],
                                                   spacing=20)
        agg_obj = self.custom_agg['moving-average']
        window = '120s'

        center = 'False'
        result = agg_obj.compute(self.storage, metric,
                                 start=None, stop=None,
                                 window=window, center=center)
        expected = [(datetime.datetime(2014, 1, 1, 12), 120.0, 32.25)]
        self.assertEqual(expected, result)

        center = 'True'
        result = agg_obj.compute(self.storage, metric,
                                 start=None, stop=None,
                                 window=window, center=center)

        expected = [(datetime.datetime(2014, 1, 1, 12, 1), 120.0, 28.875)]
        self.assertEqual(expected, result)
        # (FIXME) atmalagon: doing a centered average when
        # there are only two points in the retrieved data seems weird.
        # better to raise an error or return nan in this case?

        self.storage.delete_metric(metric)
