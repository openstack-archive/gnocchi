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
import datetime

import pandas
import testtools

from gnocchi import carbonara


class TestBoundTimeSerie(testtools.TestCase):
    def test_base(self):
        carbonara.BoundTimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 0, 4),
                                  datetime.datetime(2014, 1, 1, 12, 0, 9)],
                                 [3, 5, 6])

    def test_timespan(self):
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            timespan='5s')
        self.assertEqual(len(ts), 2)
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(len(ts), 3)

    def test_timespan_timelimit(self):
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            timespan='5s')
        self.assertEqual(len(ts), 2)
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(len(ts), 3)

        self.assertRaises(
            carbonara.NoDeloreanAvailable,
            ts.set_values,
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 42)],
        )


class TestAggregatedTimeSerie(testtools.TestCase):

    def test_base(self):
        carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

    def test_different_length_in_timestamps_and_data(self):
        self.assertRaises(ValueError,
                          carbonara.AggregatedTimeSerie,
                          [datetime.datetime(2014, 1, 1, 12, 0, 0),
                           datetime.datetime(2014, 1, 1, 12, 0, 4),
                           datetime.datetime(2014, 1, 1, 12, 0, 9)],
                          [3, 5])

    def test_max_size(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            max_size=2)
        self.assertEqual(2, len(ts))
        self.assertEqual(ts[0], 5)
        self.assertEqual(ts[1], 6)

    def test_down_sampling(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 7],
            sampling='5Min')
        self.assertEqual(1, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_down_sampling_with_max_size(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1],
            sampling='1Min',
            max_size=2)
        self.assertEqual(2, len(ts))
        self.assertEqual(6, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_down_sampling_with_max_size_and_method_max(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 70, 1],
            sampling='1Min',
            max_size=2,
            aggregation_method='max')
        self.assertEqual(2, len(ts))
        self.assertEqual(70, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_to_dict_from_dict(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1],
            sampling='1Min',
            max_size=2,
            aggregation_method='max')

        ts2 = carbonara.AggregatedTimeSerie.from_dict(ts.to_dict())
        self.assertEqual(ts, ts2)

    def test_serialize(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 2, 9),
             datetime.datetime(2014, 1, 1, 12, 3, 12)],
            [3, 5, 7, 100],
            sampling='1Min',
            block_size='1Min',
            max_size=10)
        s = ts.serialize()
        self.assertEqual(ts, carbonara.AggregatedTimeSerie.unserialize(s))

    def test_truncate_block_size(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 5),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 8, 5, 7, 1],
            max_size=5,
            block_size=pandas.tseries.offsets.Minute(1))
        self.assertEqual(5, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 19), 123)])
        self.assertEqual(4, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 1, 4)])
        self.assertEqual(7, ts[datetime.datetime(2014, 1, 1, 12, 1, 9)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 12)])
        self.assertEqual(123, ts[datetime.datetime(2014, 1, 1, 12, 3, 19)])
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 20), 124)])
        self.assertEqual(5, len(ts))


class TestTimeSerieArchive(testtools.TestCase):

    def test_fetch(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 60),
             (pandas.tseries.offsets.Minute(5), 24)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(5.5, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(8, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 10), 11)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(5.5, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(9, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_fetch_agg_max(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 60),
             (pandas.tseries.offsets.Minute(5), 24)],
            aggregation_method='max')

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(7, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(15, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 10), 110)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(7, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(110, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_serialize(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), None),
             (pandas.tseries.offsets.Minute(5), None)])
        tsc.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 1),
        ])

        self.assertEqual(tsc,
                         carbonara.TimeSerieArchive.unserialize(
                             tsc.serialize()))
