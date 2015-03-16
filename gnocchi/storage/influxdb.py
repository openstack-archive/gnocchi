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

import influxdb
from oslo_config import cfg
from oslo_utils import timeutils

from gnocchi import storage


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
               default='root',
               help='InfluxDB password'),
    cfg.StrOpt('influxdb_database',
               default='gnocchi',
               help='InfluxDB database'),
]


LOG = logging.getLogger(__name__)


class InfluxDBStorage(storage.StorageDriver):
    EPOCH = datetime.datetime(1970, 1, 1)

    def __init__(self, conf):
        super(InfluxDBStorage, self).__init__(conf)
        self.influx = influxdb.InfluxDBClient(conf.influxdb_host,
                                              conf.influxdb_port,
                                              conf.influxdb_username,
                                              conf.influxdb_password,
                                              conf.influxdb_database)
        try:
            dbs = self.influx.get_list_database()
            if conf.influxdb_database not in dbs:
                self.influx.create_database(conf.influxdb_database)
        except influxdb.client.InfluxDBClientError as e:
            LOG.warning('InfluxDB database creation failed: %s %d'
                        % (e.message, e.code), exc_info=True)
            raise

    def create_metric(self, metric):
        if metric.name in self.influx.get_list_series():
            raise storage.MetricAlreadyExists(metric)
        self.influx.write_points([dict(name=metric.name,
                                       fields=dict(exists=1))],
                                 time_precision='u')

    def _query(self, metric, query):
        try:
            return self.influx.query(query)
        except influxdb.client.InfluxDBClientError as e:
            if e.code == 400 and "Couldn't find series" in e.content:
                raise storage.MetricDoesNotExist(metric)
            raise

    def delete_metric(self, metric):
        if metric.name not in self.influx.get_list_series():
            raise storage.MetricDoesNotExist(metric)
        self.influx.delete_series(metric.name)

    def add_measures(self, metric, measures):
        points = [dict(name=metric.name,
                       timestamp=timeutils.isotime(
                           timeutils.normalize_time(m.timestamp), True),
                       fields=dict(value=m.value))
                  for m in measures]
        self.influx.write_points(points=points, time_precision='u')

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        if aggregation not in metric.archive_policy.aggregation_methods:
            raise storage.AggregationDoesNotExist(metric, aggregation)

        query = ("SELECT %(aggregation)s(value) FROM \"%(name)s\""
                 % dict(aggregation=aggregation,
                        name=metric.name))

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
        # That's good enough until we support continuous query or the like.

        results = []
        for definition in sorted(
                metric.archive_policy.definition,
                key=operator.attrgetter('granularity')):
            subquery = query + " GROUP BY time(%ds) LIMIT %d" % (
                definition.granularity, definition.points)

            result = self._query(metric, subquery)

            if not result:
                raise storage.MetricDoesNotExist(metric)

            for point in result[metric.name]:
                timestamp = timeutils.parse_isotime(point['time'])
                if ((from_timestamp is None or timestamp >= from_timestamp)
                   and (to_timestamp is None or timestamp < to_timestamp)):
                    results.append((timestamp,
                                    definition.granularity,
                                    point[aggregation]))

        return list(reversed(results))
