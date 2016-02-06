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
import math

from oslo_utils import timeutils
from oslotest import base
# TODO(jd) We shouldn't use pandas here
import pandas
import six

from gnocchi import carbonara


class TestBoundTimeSerie(base.BaseTestCase):
    @staticmethod
    def test_base():
        carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

    def test_block_size(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            block_size='5s')
        self.assertEqual(1, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(2, len(ts))

    def test_block_size_back_window(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            block_size='5s',
            back_window=1)
        self.assertEqual(3, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(3, len(ts))

    def test_block_size_unordered(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 9),
             datetime.datetime(2014, 1, 1, 12, 0, 5)],
            [10, 5, 23],
            block_size='5s')
        self.assertEqual(2, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 11), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 10), 4)])
        self.assertEqual(2, len(ts))

    def test_duplicate_timestamps(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 9),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [10, 5, 23])
        self.assertEqual(2, len(ts))
        self.assertEqual(10.0, ts[0])
        self.assertEqual(23.0, ts[1])

        ts.set_values([(datetime.datetime(2014, 1, 1, 13, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 9),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 8),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 7),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 4)])
        self.assertEqual(4, len(ts))
        self.assertEqual(10.0, ts[0])
        self.assertEqual(23.0, ts[1])
        self.assertEqual(3.0, ts[2])
        self.assertEqual(4.0, ts[3])


class TestAggregatedTimeSerie(base.BaseTestCase):
    @staticmethod
    def test_base():
        carbonara.AggregatedTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        carbonara.AggregatedTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6], sampling=3)
        carbonara.AggregatedTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6], sampling="4s")

    def test_fetch_basic(self):
        ts = carbonara.AggregatedTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            sampling="1s")
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12), 1, 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            ts.fetch())
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            ts.fetch(from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 4)))
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            ts.fetch(
                from_timestamp=timeutils.parse_isotime(
                    "2014-01-01 12:00:04")))
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            ts.fetch(
                from_timestamp=timeutils.parse_isotime(
                    "2014-01-01 13:00:04+01:00")))

    def test_bad_percentile(self):
        for bad_percentile in ('0pct', '100pct', '-1pct', '123pct'):
            self.assertRaises(carbonara.UnknownAggregationMethod,
                              carbonara.AggregatedTimeSerie,
                              sampling='1Min',
                              aggregation_method=bad_percentile)

    def test_74_percentile_serialized(self):
        ts = carbonara.AggregatedTimeSerie(sampling='1Min',
                                           aggregation_method='74pct')
        ts.update(carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)]))

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

        # Serialize and unserialize
        ts = carbonara.AggregatedTimeSerie.unserialize(ts.serialize())

        ts.update(carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)]))

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_95_percentile(self):
        ts = carbonara.AggregatedTimeSerie(sampling='1Min',
                                           aggregation_method='95pct')
        ts.update(carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)]))

        self.assertEqual(1, len(ts))
        self.assertEqual(5.9000000000000004,
                         ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_different_length_in_timestamps_and_data(self):
        self.assertRaises(ValueError,
                          carbonara.AggregatedTimeSerie.from_data,
                          [datetime.datetime(2014, 1, 1, 12, 0, 0),
                           datetime.datetime(2014, 1, 1, 12, 0, 4),
                           datetime.datetime(2014, 1, 1, 12, 0, 9)],
                          [3, 5])

    def test_max_size(self):
        ts = carbonara.AggregatedTimeSerie(
            max_size=2)
        ts.update(carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6]))
        self.assertEqual(2, len(ts))
        self.assertEqual(5, ts[0])
        self.assertEqual(6, ts[1])

    def test_down_sampling(self):
        ts = carbonara.AggregatedTimeSerie(sampling='5Min')
        ts.update(carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 7]))
        self.assertEqual(1, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_down_sampling_with_max_size(self):
        ts = carbonara.AggregatedTimeSerie(
            sampling='1Min',
            max_size=2)
        ts.update(carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1]))
        self.assertEqual(2, len(ts))
        self.assertEqual(6, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_down_sampling_with_max_size_and_method_max(self):
        ts = carbonara.AggregatedTimeSerie(
            sampling='1Min',
            max_size=2,
            aggregation_method='max')
        ts.update(carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 70, 1]))
        self.assertEqual(2, len(ts))
        self.assertEqual(70, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_to_dict_from_dict(self):
        ts = carbonara.AggregatedTimeSerie(
            sampling='1Min',
            max_size=2,
            aggregation_method='max')
        ts.update(carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1]))
        ts2 = carbonara.AggregatedTimeSerie.from_dict(ts.to_dict())
        self.assertEqual(ts, ts2)

    def test_aggregated_different_archive_no_overlap(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=60, max_size=50)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=50)
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.sampling)

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 11, 46, 4), 4)],
                        before_truncate_callback=tsc1.update)
        tsb2.set_values([(datetime.datetime(2014, 1, 1, 9, 1, 4), 4)],
                        before_truncate_callback=tsc2.update)

        dtfrom = datetime.datetime(2014, 1, 1, 11, 0, 0)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom)

    def test_aggregated_different_archive_no_overlap2(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=60, max_size=50)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=50)

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 0), 4)],
                        before_truncate_callback=tsc1.update)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1, tsc2])

    def test_aggregated_different_archive_overlap(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.sampling)

        # NOTE(sileht): minute 8 is missing in both and
        # minute 7 in tsc2 too, but it looks like we have
        # enough point to do the aggregation
        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 10),
            (datetime.datetime(2014, 1, 1, 12, 9, 0), 2),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 9, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 11, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 12, 0), 2),
        ], before_truncate_callback=tsc2.update)

        dtfrom = datetime.datetime(2014, 1, 1, 12, 0, 0)
        dtto = datetime.datetime(2014, 1, 1, 12, 10, 0)

        # By default we require 100% of point that overlap
        # so that fail
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom,
                          to_timestamp=dtto)

        # Retry with 80% and it works
        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1, tsc2], from_timestamp=dtfrom, to_timestamp=dtto,
            needed_percent_of_overlap=80.0)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 4.0),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 4.0),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 5.0),
            (pandas.Timestamp('2014-01-01 12:07:00'), 60.0, 10.0),
            (pandas.Timestamp('2014-01-01 12:09:00'), 60.0, 2.0),
        ], output)

    def test_aggregated_different_archive_overlap_edge_missing1(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.sampling)

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 9),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 1),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 7),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 3),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 13),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 24),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 16),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 12),
        ], before_truncate_callback=tsc2.update)

        # By default we require 100% of point that overlap
        # but we allow that the last datapoint is missing
        # of the precisest granularity
        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1, tsc2], aggregation='sum')

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 33.0),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 5.0),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 18.0),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 19.0),
        ], output)

    def test_aggregated_different_archive_overlap_edge_missing2(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.sampling)

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
        ], before_truncate_callback=tsc2.update)

        output = carbonara.AggregatedTimeSerie.aggregated([tsc1, tsc2])
        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 4.0),
        ], output)

    def test_fetch(self):
        ts = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 4), 4),
            (datetime.datetime(2014, 1, 1, 11, 47, 34), 8),
            (datetime.datetime(2014, 1, 1, 11, 50, 54), 50),
            (datetime.datetime(2014, 1, 1, 11, 54, 45), 4),
            (datetime.datetime(2014, 1, 1, 11, 56, 49), 4),
            (datetime.datetime(2014, 1, 1, 11, 57, 22), 6),
            (datetime.datetime(2014, 1, 1, 11, 58, 22), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 4, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 5, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 5, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 6, 0, 2), 3),
        ], before_truncate_callback=ts.update)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 6), 5),
        ], before_truncate_callback=ts.update)

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 54), 60.0, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 56), 60.0, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 57), 60.0, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 58), 60.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 4.0)
        ], ts.fetch())

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 4.0)
        ], ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

    def test_fetch_agg_pct(self):
        ts = carbonara.AggregatedTimeSerie(sampling=1, max_size=3600 * 24,
                                           aggregation_method='90pct')
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 0, 0, 123), 4),
                        (datetime.datetime(2014, 1, 1, 12, 0, 2), 4)],
                       before_truncate_callback=ts.update)

        result = ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        reference = [
            (pandas.Timestamp('2014-01-01 12:00:00'),
             1.0, 3.9),
            (pandas.Timestamp('2014-01-01 12:00:02'),
             1.0, 4)
        ]

        self.assertEqual(len(reference), len(result))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            self.assertEqual(ref[1], res[1])
            # Rounding \o/
            self.assertAlmostEqual(ref[2], res[2])

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 2, 113), 110)],
                       before_truncate_callback=ts.update)

        result = ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        reference = [
            (pandas.Timestamp('2014-01-01 12:00:00'),
             1.0, 3.9),
            (pandas.Timestamp('2014-01-01 12:00:02'),
             1.0, 99.4)
        ]

        self.assertEqual(len(reference), len(result))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            self.assertEqual(ref[1], res[1])
            # Rounding \o/
            self.assertAlmostEqual(ref[2], res[2])

    def test_fetch_nano(self):
        ts = carbonara.AggregatedTimeSerie(sampling=0.2, max_size=10)
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 200123), 4),
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 340000), 8),
            (datetime.datetime(2014, 1, 1, 11, 47, 0, 323154), 50),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 590903), 4),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 903291), 4),
        ], before_truncate_callback=ts.update)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 821312), 5),
        ], before_truncate_callback=ts.update)

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 200000), 0.2, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 47, 0, 200000), 0.2, 50.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 400000), 0.2, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 800000), 0.2, 4.5)
        ], ts.fetch())

    def test_fetch_agg_std(self):
        ts = carbonara.AggregatedTimeSerie(sampling=60, max_size=60,
                                           aggregation_method='std')
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)],
                       before_truncate_callback=ts.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:01:00'),
             60.0, 2.1213203435596424),
            (pandas.Timestamp('2014-01-01 12:02:00'),
             60.0, 9.8994949366116654),
        ], ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)],
                       before_truncate_callback=ts.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:01:00'),
             60.0, 2.1213203435596424),
            (pandas.Timestamp('2014-01-01 12:02:00'),
             60.0, 59.304300012730948),
        ], ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

    def test_fetch_agg_max(self):
        ts = carbonara.AggregatedTimeSerie(sampling=60, max_size=60,
                                           aggregation_method='max')
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)],
                       before_truncate_callback=ts.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'), 60.0, 3),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 7),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 15),
        ], ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)],
                       before_truncate_callback=ts.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'), 60.0, 3),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 7),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 110),
        ], ts.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

    def test_serialize(self):
        ts = carbonara.AggregatedTimeSerie(sampling=0.5)
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0, 1234), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 0, 321), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 4, 234), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9, 32), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 12, 532), 1),
        ], before_truncate_callback=ts.update)

        self.assertEqual(ts,
                         carbonara.AggregatedTimeSerie.unserialize(
                             ts.serialize()))

    def test_no_truncation(self):
        ts = carbonara.AggregatedTimeSerie(sampling=60)
        tsb = carbonara.BoundTimeSerie()

        for i in six.moves.range(1, 11):
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i), float(i))
            ], before_truncate_callback=ts.update)
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i + 1), float(i + 1))
            ], before_truncate_callback=ts.update)
            self.assertEqual(i, len(ts.fetch()))

    def test_back_window(self):
        """Back window testing.

        Test the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = carbonara.AggregatedTimeSerie(sampling=1, max_size=60)
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ], before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        try:
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
            ])
        except carbonara.NoDeloreanAvailable as e:
            self.assertEqual(
                six.text_type(e),
                u"2014-01-01 12:00:02.000099 is before 2014-01-01 12:00:03")
            self.assertEqual(datetime.datetime(2014, 1, 1, 12, 0, 2, 99),
                             e.bad_timestamp)
            self.assertEqual(datetime.datetime(2014, 1, 1, 12, 0, 3),
                             e.first_timestamp)
        else:
            self.fail("No exception raised")

    def test_back_window_ignore(self):
        """Back window testing.

        Test the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = carbonara.AggregatedTimeSerie(sampling=1, max_size=60)
        tsb = carbonara.BoundTimeSerie(block_size=ts.sampling)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ], before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
        ], ignore_too_old_timestamps=True, before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 9), 4.5),
        ], ignore_too_old_timestamps=True, before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 3.5),
            ],
            ts.fetch())

    def test_aggregated_nominal(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsc12 = carbonara.AggregatedTimeSerie(sampling=300, max_size=6)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc12.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=10)
        tsc22 = carbonara.AggregatedTimeSerie(sampling=300, max_size=6)
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc22.sampling)

        def ts1_update(ts):
            tsc1.update(ts)
            tsc12.update(ts)

        def ts2_update(ts):
            tsc2.update(ts)
            tsc22.update(ts)

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 4), 4),
            (datetime.datetime(2014, 1, 1, 11, 47, 34), 8),
            (datetime.datetime(2014, 1, 1, 11, 50, 54), 50),
            (datetime.datetime(2014, 1, 1, 11, 54, 45), 4),
            (datetime.datetime(2014, 1, 1, 11, 56, 49), 4),
            (datetime.datetime(2014, 1, 1, 11, 57, 22), 6),
            (datetime.datetime(2014, 1, 1, 11, 58, 22), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 4, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 5, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 5, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 3),
        ], before_truncate_callback=ts1_update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 4), 6),
            (datetime.datetime(2014, 1, 1, 11, 47, 34), 5),
            (datetime.datetime(2014, 1, 1, 11, 50, 54), 51),
            (datetime.datetime(2014, 1, 1, 11, 54, 45), 5),
            (datetime.datetime(2014, 1, 1, 11, 56, 49), 5),
            (datetime.datetime(2014, 1, 1, 11, 57, 22), 7),
            (datetime.datetime(2014, 1, 1, 11, 58, 22), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 8),
            (datetime.datetime(2014, 1, 1, 12, 2, 1), 10),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 2),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 4, 9), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 1), 10),
            (datetime.datetime(2014, 1, 1, 12, 5, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 1),
        ], before_truncate_callback=ts2_update)

        output = carbonara.AggregatedTimeSerie.aggregated([tsc1, tsc12,
                                                           tsc2, tsc22])
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 45), 300.0, 5.75),
            (datetime.datetime(2014, 1, 1, 11, 50), 300.0, 27.5),
            (datetime.datetime(2014, 1, 1, 11, 55), 300.0, 5.3333333333333339),
            (datetime.datetime(2014, 1, 1, 12, 0), 300.0, 6.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 5.1666666666666661),
            (datetime.datetime(2014, 1, 1, 11, 54), 60.0, 4.5),
            (datetime.datetime(2014, 1, 1, 11, 56), 60.0, 4.5),
            (datetime.datetime(2014, 1, 1, 11, 57), 60.0, 6.5),
            (datetime.datetime(2014, 1, 1, 11, 58), 60.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 6.0),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 4.5),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 6.75),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 2.0),
        ], output)

    def test_aggregated_partial_overlap(self):
        tsc1 = carbonara.AggregatedTimeSerie(sampling=1, max_size=86400)
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.sampling)
        tsc2 = carbonara.AggregatedTimeSerie(sampling=1, max_size=86400)
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.sampling)

        tsb1.set_values([
            (datetime.datetime(2015, 12, 3, 13, 19, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 20, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 21, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 22, 15), 1),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2015, 12, 3, 13, 21, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 22, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 23, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 24, 15), 10),
        ], before_truncate_callback=tsc2.update)

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1, tsc2], aggregation="sum")

        self.assertEqual([
            (pandas.Timestamp('2015-12-03 13:21:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:22:15'), 1.0, 11.0),
        ], output)

        dtfrom = datetime.datetime(2015, 12, 3, 13, 17, 0)
        dtto = datetime.datetime(2015, 12, 3, 13, 25, 0)

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1, tsc2], from_timestamp=dtfrom, to_timestamp=dtto,
            aggregation="sum", needed_percent_of_overlap=0)

        self.assertEqual([
            (pandas.Timestamp('2015-12-03 13:19:15'), 1.0, 1.0),
            (pandas.Timestamp('2015-12-03 13:20:15'), 1.0, 1.0),
            (pandas.Timestamp('2015-12-03 13:21:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:22:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:23:15'), 1.0, 10.0),
            (pandas.Timestamp('2015-12-03 13:24:15'), 1.0, 10.0),
        ], output)

        # By default we require 100% of point that overlap
        # so that fail if from or to is set
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1, tsc2], to_timestamp=dtto)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom)

        # Retry with 50% and it works
        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1, tsc2], from_timestamp=dtfrom,
            aggregation="sum",
            needed_percent_of_overlap=50.0)
        self.assertEqual([
            (pandas.Timestamp('2015-12-03 13:19:15'), 1.0, 1.0),
            (pandas.Timestamp('2015-12-03 13:20:15'), 1.0, 1.0),
            (pandas.Timestamp('2015-12-03 13:21:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:22:15'), 1.0, 11.0),
        ], output)

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1, tsc2], to_timestamp=dtto,
            aggregation="sum",
            needed_percent_of_overlap=50.0)
        self.assertEqual([
            (pandas.Timestamp('2015-12-03 13:21:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:22:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:23:15'), 1.0, 10.0),
            (pandas.Timestamp('2015-12-03 13:24:15'), 1.0, 10.0),
        ], output)

    def test_split_key(self):
        self.assertEqual(
            "1420128000.0",
            carbonara.AggregatedTimeSerie.get_split_key(
                datetime.datetime(2015, 1, 1, 23, 34), 5))
        self.assertEqual(
            "1420056000.0",
            carbonara.AggregatedTimeSerie.get_split_key(
                datetime.datetime(2015, 1, 1, 15, 3), 5))

    def test_split_key_datetime(self):
        self.assertEqual(
            datetime.datetime(2014, 5, 10),
            carbonara.AggregatedTimeSerie.get_split_key_datetime(
                datetime.datetime(2015, 1, 1, 15, 3), 3600))
        self.assertEqual(
            datetime.datetime(2014, 12, 29, 8),
            carbonara.AggregatedTimeSerie.get_split_key_datetime(
                datetime.datetime(2015, 1, 1, 15, 3), 58))

    def test_split(self):
        sampling = 5
        points = 100000
        ts = carbonara.TimeSerie.from_data(
            timestamps=map(datetime.datetime.utcfromtimestamp,
                           six.moves.range(points)),
            values=six.moves.range(points))
        agg = carbonara.AggregatedTimeSerie(sampling=sampling)
        agg.update(ts)

        grouped_points = list(agg.split())

        self.assertEqual(
            math.ceil((points / float(sampling))
                      / carbonara.AggregatedTimeSerie.POINTS_PER_SPLIT),
            len(grouped_points))
        self.assertEqual("0.0",
                         grouped_points[0][0])
        # 14400 × 5s = 20 hours
        self.assertEqual("72000.0",
                         grouped_points[1][0])
        self.assertEqual(carbonara.AggregatedTimeSerie.POINTS_PER_SPLIT,
                         len(grouped_points[0][1]))

    def test_from_timeseries(self):
        sampling = 5
        points = 100000
        ts = carbonara.TimeSerie.from_data(
            timestamps=map(datetime.datetime.utcfromtimestamp,
                           six.moves.range(points)),
            values=six.moves.range(points))
        agg = carbonara.AggregatedTimeSerie(sampling=sampling)
        agg.update(ts)

        split = [t[1] for t in list(agg.split())]

        self.assertEqual(agg,
                         carbonara.AggregatedTimeSerie.from_timeseries(
                             split,
                             sampling=agg.sampling,
                             max_size=agg.max_size,
                             aggregation_method=agg.aggregation_method))
