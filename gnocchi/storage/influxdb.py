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
from __future__ import absolute_import
import datetime
import logging
import operator

try:
    import influxdb
except ImportError:
    influxdb = None
import iso8601
from oslo_config import cfg
from oslo_utils import timeutils
import retrying

from gnocchi import storage
from gnocchi import utils


OPTS = [
    cfg.StrOpt('influxdb_host',
               default='localhost',
               help='InfluxDB host'),
    cfg.IntOpt('influxdb_port',
               default=8086,
               help='InfluxDB port'),
    cfg.StrOpt('influxdb_username',
               default='root',
               help='InfluxDB username'),
    cfg.StrOpt('influxdb_password',
               secret=True,
               help='InfluxDB password'),
    cfg.StrOpt('influxdb_database',
               default='gnocchi',
               help='InfluxDB database'),
    cfg.BoolOpt('influxdb_block_until_data_ingested',
                default=False,
                help='InfluxDB ingests data in asynchroneous ways. '
                'Set to True to wait data are ingested.'),
]


LOG = logging.getLogger(__name__)
START_EPOCH = datetime.datetime(1, 1, 1, tzinfo=iso8601.iso8601.UTC)


class InfluxDBStorage(storage.StorageDriver):

    def __init__(self, conf):
        if not influxdb:
            raise ImportError("Module influxdb could not be load")
        super(InfluxDBStorage, self).__init__(conf)
        self._block_until_data_ingested = (
            conf.influxdb_block_until_data_ingested)
        self.influx = influxdb.InfluxDBClient(conf.influxdb_host,
                                              conf.influxdb_port,
                                              conf.influxdb_username,
                                              conf.influxdb_password,
                                              conf.influxdb_database)
        self.database = conf.influxdb_database
        try:
            dbs = [db['name'] for db in self.influx.get_list_database()]
            if conf.influxdb_database not in dbs:
                self.influx.create_database(conf.influxdb_database)
        except influxdb.client.InfluxDBClientError as e:
            if "database already exists" in e.content:
                LOG.warning("InfluxDB database \"%s\" already exists",
                            self.database)
            else:
                LOG.warning('InfluxDB database creation failed: %s %s'
                            % (e.message, e.code), exc_info=True)
                raise

    @staticmethod
    def _get_metric_id(metric):
        return str(metric.id)

    def _metric_exists(self, metric):
        list_series = [s['name'] for s in self.influx.get_list_series()]
        return self._get_metric_id(metric) in list_series

    def _query(self, metric, query):
        try:
            return self.influx.query(query, database=self.database)
        except influxdb.client.InfluxDBClientError as e:
            if "measurement not found" in e.content:
                raise storage.MetricDoesNotExist(metric)
            # NOTE(ityaptin) If metric exists but doesn't have any measures
            # with `value` field influxdb client may rise exception
            # for aggregate query. It's not error in Gnocchi context and
            # we should to return empty list in this case.
            if "unknown field or tag name" in e.content:
                return {self._get_metric_id(metric): []}
            raise

    @retrying.retry(stop_max_delay=5000, wait_fixed=500,
                    retry_on_exception=utils.retry_if_retry_raised)
    def _wait_points_exists(self, metric_id, where):
        # NOTE(sileht): influxdb query returns even the data is not yet insert
        # in the asked series, the work is done in an async fashion, so a
        # immediate get_measures after an add_measures will not returns the
        # just inserted data. perhaps related:
        # https://github.com/influxdb/influxdb/issues/2450 This is a workaround
        # to wait that data appear in influxdb...
        if not self._block_until_data_ingested:
            return
        try:
            result = self.influx.query("SELECT * FROM \"%(metric_id)s\" WHERE "
                                       "%(where)s LIMIT 1" %
                                       dict(metric_id=metric_id, where=where),
                                       database=self.database)
        except influxdb.client.InfluxDBClientError as e:
            if "measurement not found" in e.content:
                raise utils.Retry
            raise

        result = list(result[metric_id])
        if not result:
            raise utils.Retry

    def create_metric(self, metric):
        if self._metric_exists(metric):
            raise storage.MetricAlreadyExists(metric)
        metric_id = self._get_metric_id(metric)
        self.influx.write_points([dict(measurement=metric_id,
                                       fields=dict(created=1))],
                                 time_precision='n',
                                 retention_policy="default",
                                 database=self.database)
        self._wait_points_exists(metric_id, "\"created\" = 1")

    def delete_metric(self, metric):
        if not self._metric_exists(metric):
            raise storage.MetricDoesNotExist(metric)
        metric_id = self._get_metric_id(metric)
        self._query(metric, "DROP MEASUREMENT \"%s\"" % metric_id)

    def add_measures(self, metric, measures):
        metric_id = self._get_metric_id(metric)
        points = [dict(measurement=metric_id,
                       time=self._timestamp_to_utc(m.timestamp).isoformat(),
                       fields=dict(value=float(m.value)))
                  for m in measures]
        self.influx.write_points(points=points, time_precision='n',
                                 database=self.database,
                                 retention_policy="default")
        self._wait_points_exists(metric_id, "time = '%(time)s' AND "
                                 "value = %(value)s" %
                                 dict(time=points[-1]['time'],
                                      value=points[-1]["fields"]["value"]))

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        if from_timestamp:
            from_timestamp = self._timestamp_to_utc(from_timestamp)
        if to_timestamp:
            to_timestamp = self._timestamp_to_utc(to_timestamp)

        if aggregation not in metric.archive_policy.aggregation_methods:
            raise storage.AggregationDoesNotExist(metric, aggregation)

        metric_id = self._get_metric_id(metric)

        result = self._query(metric, "select * from \"%(metric_id)s\"" %
                             dict(metric_id=metric_id))
        result = list(result[metric_id])
        if not result:
            raise storage.MetricDoesNotExist(metric)

        if not from_timestamp:
            first_measure_timestamp = self._timestamp_to_utc(
                timeutils.parse_isotime(result[0]['time']))

        query = ("SELECT %(aggregation)s(value) FROM \"%(metric_id)s\""
                 % dict(aggregation=aggregation,
                        metric_id=metric_id))

        # NOTE(jd) So this is totally suboptimal as we CANNOT limit the range
        # on time. InfluxDB is not smart enough yet to limit the result of the
        # time we want based on the GROUP BY result, not based on the time
        # value. If we do from_timestamp < t < to_timestamp, InfluxDB will
        # limit the datapoints to those, and then run the aggregate function.
        # What we want instead, is something like:
        # SELECT mean(value) FROM serie
        #  GROUP BY time(5s) as groupedtime
        #  WHERE from_timestamp <= groupedtime < to_timestamp
        # Since we cannot do that, we aggregate everything and then limit
        # the returned result.
        # see https://github.com/influxdb/influxdb/issues/1973
        # NOTE(sileht): But we have to set one time boundary to have the
        # request accept by influxdb.
        # see https://github.com/influxdb/influxdb/issues/2444
        #
        # That's good enough until we support continuous query or the like.

        results = []
        for definition in sorted(
                metric.archive_policy.definition,
                key=operator.attrgetter('granularity')):
            time_query = self._make_time_query(
                from_timestamp or first_measure_timestamp,
                to_timestamp,
                definition.granularity)
            subquery = (query +
                        " WHERE %(times)s GROUP BY time(%(granularity)ds) "
                        "fill(none) LIMIT %(points)d" %
                        dict(times=time_query,
                             granularity=definition.granularity,
                             points=definition.points))

            result = self._query(metric, subquery)

            subresults = []
            for point in result[metric_id]:
                timestamp = self._timestamp_to_utc(
                    timeutils.parse_isotime(point['time']))
                if (point[aggregation] is not None and
                    ((from_timestamp is None or timestamp >= from_timestamp)
                     and (to_timestamp is None or timestamp < to_timestamp))):
                    subresults.insert(0, (timestamp,
                                          definition.granularity,
                                          point[aggregation]))
            results.extend(subresults)

        return list(reversed(results))

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None,
                     aggregation='mean'):
        results = {}
        predicate = storage.MeasureQuery(query)

        for metric in metrics:
            measures = self.get_measures(metric, from_timestamp, to_timestamp,
                                         aggregation)
            results[metric] = [
                (timestamp, granularity, value)
                for timestamp, granularity, value in measures
                if predicate(value)]
        return results

    @staticmethod
    def _timestamp_to_utc(ts):
        return timeutils.normalize_time(ts).replace(tzinfo=iso8601.iso8601.UTC)

    def _make_time_query(self, from_timestamp, to_timestamp, granularity):
        if from_timestamp:
            from_timestamp = find_nearliest_stable_point(from_timestamp,
                                                         granularity)
            left_time = self._timestamp_to_utc(from_timestamp).isoformat()
        else:
            left_time = "now()"

        if to_timestamp and to_timestamp >= from_timestamp:
            right_time = self._timestamp_to_utc(to_timestamp).isoformat()
        else:
            right_time = None

        return ("time >= '%s'" % left_time) + (" and time < '%s'" % right_time
                                               if right_time else "")


def find_nearliest_stable_point(timestamp, granularity, next=False):
    seconds = timeutils.delta_seconds(START_EPOCH, timestamp)
    seconds = int(seconds - seconds % granularity)
    stable_point = START_EPOCH + datetime.timedelta(seconds=seconds)
    if next:
        stable_point += datetime.timedelta(seconds=granularity)
    return stable_point
