# -*- encoding: utf-8 -*-
#
# Copyright © 2016 The University of Melbourne
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
import re

try:
    import influxdb
except ImportError:
    influxdb = None
import iso8601
from oslo_config import cfg
from oslo_utils import timeutils

from gnocchi import carbonara
from gnocchi import indexer
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
    cfg.StrOpt('influxdb_default_retention_policy',
               default='autogen',
               help='InfluxDB default retention policy.'),
    cfg.BoolOpt('influxdb_disable_retention_policies',
                default=False,
                help='InfluxDB disable retention policies, '
                'essentially setting all retention policies to '
                'infinity'),
]


LOG = logging.getLogger(__name__)
START_EPOCH = datetime.datetime(1, 1, 1, tzinfo=iso8601.iso8601.UTC)

CQ_QUERY = """CREATE CONTINUOUS QUERY %(measure)s ON "%(database)s" BEGIN
SELECT %(aggregation_method)s AS value INTO
"%(database)s"."%(retention)s".%(measure)s FROM
"%(database)s"."%(parent_retention)s".%(parent_measure)s GROUP BY
time(%(granularity)ss), metric_id END"""


class InfluxDBStorage(storage.StorageDriver):

    def __init__(self, conf):
        if not influxdb:
            raise ImportError("Module influxdb could not be loaded")
        super(InfluxDBStorage, self).__init__(conf)
        self._block_until_data_ingested = (
            conf.influxdb_block_until_data_ingested)
        self.influx = influxdb.InfluxDBClient(conf.influxdb_host,
                                              conf.influxdb_port,
                                              conf.influxdb_username,
                                              conf.influxdb_password,
                                              conf.influxdb_database)
        self.database = conf.influxdb_database
        self.conf = conf._conf
        self.measurement_prefix = 'samples'
        try:
            self.influx.get_list_retention_policies(self.database)
        except influxdb.exceptions.InfluxDBClientError:
            LOG.warn("Coudn't setup influxDB CQs as DB doesn't exist")
        else:
            self.setup_archive_policies()

    def setup_archive_policies(self):
        index = indexer.get_driver(self.conf)
        archive_policies = index.list_archive_policies()

        for ap in archive_policies:
            self.setup_archive_policy(ap)

    def setup_archive_policy(self, ap, reset=False):
        start_measure = self._get_ap_measurement(ap)
        start_retention = self.conf.storage.influxdb_default_retention_policy
        ap_name = self._sanitize_cq_name(ap.name)
        for aggregation in ap.aggregation_methods:
            aggregation_method = self._get_aggregation_method(aggregation)
            for rule in sorted(ap.definition, key=lambda k: k['granularity']):

                retention = int(rule['timespan'])
                rp_name = 'rp_%s' % retention

                if self.conf.storage.influxdb_disable_retention_policies:
                    retention = 'INF'
                elif retention < 3600:
                    # Can't have a retention policy < 1 hour in influxDB
                    retention = 3600
                else:
                    retention = "%ss" % retention

                self.influx.create_retention_policy(name=rp_name,
                                                    duration=retention,
                                                    replication=1)
                granularity = int(rule['granularity'])
                measure = 'samples_%s_%s_%s' % (ap_name, aggregation,
                                                granularity)
                if reset:
                    try:
                        self.influx.query('DROP MEASUREMENT %s' % measure)
                    except influxdb.exceptions.InfluxDBClientError:
                        # Already gone so ignore
                        pass

                    self.influx.query('DROP CONTINUOUS QUERY %s ON %s' % (
                        measure, self.database))

                cq_query = CQ_QUERY % dict(
                    database=self.database,
                    retention=rp_name,
                    measure=measure,
                    aggregation=aggregation,
                    aggregation_method=aggregation_method,
                    granularity=granularity,
                    parent_retention=start_retention,
                    parent_measure=start_measure
                )
                LOG.debug(cq_query)
                try:
                    self.influx.query(cq_query)
                except influxdb.exceptions.InfluxDBClientError:
                    # Already exists so ignore
                    pass
        if reset:
            self.backfill_data(ap=ap)

    def create_db(self):
        self.influx.create_database(self.database)

    def drop_db(self):
        self.influx.drop_database(self.database)

    def _get_aggregation_method(self, aggregation):
        if aggregation == 'std':
            return 'STDDEV(value)'
        elif aggregation.endswith('pct'):
            return 'PERCENTILE(value,%s)' % aggregation.split('p')[0]
        else:
            return '%s(value)' % aggregation

    def _get_ap_measurement(self, ap):
        return "%s_%s" % (self.measurement_prefix,
                          self._sanitize_cq_name(ap.name))

    @staticmethod
    def _sanitize_cq_name(name):
        return name.replace('-', '')

    def earliest_time(self, measurement, rp=None, metrics=[]):
        where = ""
        if metrics:
            metrics_or = " OR ".join(
                ["metric_id = '%s'" % self._get_metric_id(metric)
                 for metric in metrics])
            where = "WHERE " + metrics_or
        if rp:
            measurement_select = "%s.%s" % (rp, measurement)
        else:
            measurement_select = measurement
        query = "SELECT * FROM %s %s ORDER BY time ASC LIMIT 1" % (
            measurement_select, where)
        LOG.debug(query)
        result = self.influx.query(query)
        if not result:
            return None
        return list(result[measurement])[0]['time']

    def backfill_data(self, ap):
        measurement = self._get_ap_measurement(ap)
        result = self.influx.query(
            "SELECT * FROM %s ORDER BY time ASC LIMIT 1" % measurement)
        if not result:
            return
        start_time = self.earliest_time(measurement)
        if not start_time:
            return
        cq_result = self.influx.query('SHOW CONTINUOUS QUERIES')
        cqs = list(cq_result[self.database])

        for cq in cqs:
            items = re.search(
                '.*BEGIN\s(?P<query>.*)\s(?P<query_end>GROUP BY.*)END.*',
                cq['query']).groupdict()
            cq_name = cq['name']
            if measurement and cq_name.startswith(measurement):
                query = items.get('query') + " WHERE time >= '%s' " % \
                    start_time + items.get('query_end')
                self.influx.query(query)

    def process_background_tasks(self, index, metrics, sync=True):
        # This is here solely for running tests and in normal operation
        # is never called.
        archive_policies = index.list_archive_policies()
        for ap in archive_policies:
            self.backfill_data(ap)

    @staticmethod
    def _get_metric_id(metric):
        return str(metric.id)

    def _query(self, metric, query):
        try:
            LOG.debug('INFLUX Query %s' % query)
            result = self.influx.query(query, database=self.database)
            LOG.debug('INFLUX Result %s' % result)
            return result
        except influxdb.client.InfluxDBClientError as e:
            # NOTE(ityaptin) If metric exists but doesn't have any measures
            # with `value` field influxdb client may raise exception for
            # (aggregate) query. It's not error in Gnocchi context and we
            # should to return empty list in this case.
            if ("unknown field or tag name" in e.content
               or "measurement not found" in e.content):
                return {self._get_metric_id(metric): []}
            raise

    @utils.retry
    def _wait_points_exists(self, measurement, metric_id, where):
        # NOTE(sileht): influxdb query returns even the data is not yet insert
        # in the asked series, the work is done in an async fashion, so a
        # immediate get_measures after an add_measures will not returns the
        # just inserted data. perhaps related:
        # https://github.com/influxdb/influxdb/issues/2450 This is a workaround
        # to wait that data appear in influxdb...
        if not self._block_until_data_ingested:
            return
        try:
            result = self.influx.query("SELECT * FROM \"%(measurment)s\" "
                                       "WHERE metric_id=\"%(metric_id)s\" AND "
                                       "%(where)s LIMIT 1" %
                                       dict(
                                           measurment=measurement,
                                           metric_id=metric_id,
                                           where=where),
                                       database=self.database)
        except influxdb.client.InfluxDBClientError as e:
            if "measurement not found" in e.content:
                raise utils.Retry
            raise

        result = list(result[metric_id])
        if not result:
            raise utils.Retry

    def delete_metric(self, metric, sync=None):
        metric_id = self._get_metric_id(metric)
        self._query(metric, "DROP MEASUREMENT \"%s\"" % metric_id)

    def add_measures(self, metric, measures):
        metric_id = self._get_metric_id(metric)
        measurement = self._get_ap_measurement(metric.archive_policy)
        points = [dict(measurement=measurement,
                       time=m.timestamp,
                       fields=dict(value=float(m.value)),
                       tags=dict(metric_id=metric_id))
                  for m in measures]
        rp = self.conf.storage.influxdb_default_retention_policy
        self.influx.write_points(points=points, time_precision='n',
                                 database=self.database, retention_policy=rp)

        self._wait_points_exists(metric_id, measurement,
                                 "time = '%(time)s' AND value = %(value)s" %
                                 dict(time=points[-1]['time'],
                                      value=points[-1]["fields"]["value"]))

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None):
        super(InfluxDBStorage, self).get_measures(
            metric, from_timestamp, to_timestamp, aggregation)

        if from_timestamp:
            from_timestamp = self._timestamp_to_utc(from_timestamp)
        if to_timestamp:
            to_timestamp = self._timestamp_to_utc(to_timestamp)

        metric_id = self._get_metric_id(metric)

        results = []
        defs = sorted(
            (d
             for d in metric.archive_policy.definition
             if granularity is None or granularity == d.granularity),
            key=operator.attrgetter('granularity'))

        if not defs:
            raise storage.GranularityDoesNotExist(metric, granularity)
        for definition in sorted(defs, key=lambda k: k['granularity'],
                                 reverse=True):
            time_query = self._make_time_query(
                from_timestamp,
                to_timestamp,
                definition.granularity)
            if time_query:
                time_query = " AND " + time_query
            ap_name = self._sanitize_cq_name(metric.archive_policy.name)
            measure = "samples_%(ap_name)s_%(aggregation)s_%(granularity)d" % \
                      dict(ap_name=ap_name, aggregation=aggregation,
                           granularity=definition.granularity)

            rp = "rp_%s" % int(definition.timespan)

            query = ("SELECT value as %(aggregation)s FROM "
                     "%(database)s.%(rp)s.%(measure)s WHERE "
                     "metric_id='%(metric_id)s' %(times)s fill(none) "
                     "ORDER BY time DESC LIMIT %(points)d"
                     % dict(database=self.database,
                            rp=rp,
                            measure=measure,
                            aggregation=aggregation,
                            metric_id=metric_id,
                            times=time_query,
                            points=definition.points))

            result = self._query(metric, query)

            subresults = []
            for point in result[measure]:
                timestamp = self._timestamp_to_utc(
                    timeutils.parse_isotime(point['time']))
                if point[aggregation] is not None:
                    subresults.insert(0, (timestamp,
                                          definition.granularity,
                                          point[aggregation]))
            results.extend(subresults)

        return list(results)

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean',
                     granularity=None):
        results = {}
        predicate = storage.MeasureQuery(query)

        for metric in metrics:
            measures = self.get_measures(metric, from_timestamp, to_timestamp,
                                         aggregation, granularity=None)
            results[metric] = [
                (timestamp, gran, value)
                for timestamp, gran, value in measures
                if predicate(value)]
        return results

    @staticmethod
    def _timestamp_to_utc(ts):
        return timeutils.normalize_time(ts).replace(tzinfo=iso8601.iso8601.UTC)

    def _make_time_query(self, from_timestamp, to_timestamp, granularity):
        if from_timestamp:
            from_timestamp = self._timestamp_to_utc(carbonara.round_timestamp(
                from_timestamp, granularity * 10e8))

            left_time = "time >= '%s'" % from_timestamp.isoformat()
        else:
            left_time = ""

        if from_timestamp and to_timestamp:
            left_time = left_time + " and "
        if to_timestamp:
            right_time = self._timestamp_to_utc(to_timestamp).isoformat()
        else:
            right_time = None

        if from_timestamp and to_timestamp and to_timestamp <= from_timestamp:
            right_time = None

        return ("%s" % left_time) + ("time < '%s'" % right_time
                                     if right_time else "")

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  reaggregation=None,
                                  granularity=None,
                                  needed_overlap=100.0):
        super(InfluxDBStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp, aggregation, reaggregation,
            granularity, needed_overlap)

        if reaggregation is None:
            reaggregation = aggregation

        archive_policies = set(
            [metric.archive_policy.name for metric in metrics])
        if len(archive_policies) != 1:
            raise storage.MetricUnaggregatable(metrics, 'No granularity match')

        archive_policy = metrics[0].archive_policy
        results = []
        defs = sorted(
            (d
             for d in archive_policy.definition
             if granularity is None or granularity == d.granularity),
            key=operator.attrgetter('granularity'), reverse=True)

        if not defs:
            raise storage.GranularityDoesNotExist(metric[0], granularity)

        for definition in defs:
            rp = "rp_%s" % int(definition.timespan)
            ap_name = self._sanitize_cq_name(archive_policy.name)
            measure = "samples_%(ap_name)s_%(aggregation)s_%(granularity)d" % \
                      dict(ap_name=ap_name, aggregation=aggregation,
                           granularity=definition.granularity)

            earliest_time = None
            if not from_timestamp:
                earliest_time = self.earliest_time(measure, rp, metrics)
                if not earliest_time:
                    continue
                earliest_time = self._timestamp_to_utc(
                    timeutils.parse_isotime(earliest_time))
            else:
                earliest_time = from_timestamp
            time_query = self._make_time_query(
                earliest_time,
                to_timestamp,
                definition.granularity)
            if time_query:
                time_query = " AND " + time_query

            i_aggregation = self._get_aggregation_method(reaggregation)
            metrics_or = " OR ".join(
                ["metric_id = '%s'" % self._get_metric_id(metric)
                 for metric in metrics])
            query = ("SELECT %(i_aggregation)s FROM "
                     "%(database)s.%(rp)s.%(measure)s WHERE "
                     "%(metrics)s %(times)s GROUP BY time(%(granularity)ds) "
                     "fill(none) ORDER BY time DESC LIMIT %(points)d"
                     % dict(database=self.database,
                            rp=rp,
                            measure=measure,
                            aggregation=aggregation,
                            i_aggregation=i_aggregation,
                            metrics=metrics_or,
                            times=time_query,
                            granularity=definition.granularity,
                            points=definition.points))

            LOG.debug(query)
            result = self._query(metric, query)

            subresults = []
            for point in result[measure]:
                timestamp = self._timestamp_to_utc(
                    timeutils.parse_isotime(point['time']))
                if point[reaggregation] is not None:
                    subresults.insert(0, (timestamp,
                                          definition.granularity,
                                          point[reaggregation]))
            results.extend(subresults)

        return list(results)

    def _add_measures(self, aggregation, archive_policy_def,
                      metric, timeserie):
        pass

    def _store_metric_measures(self, metric, aggregation, granularity, data):
        pass

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        pass

    def list_metric_with_measures_to_process(self, size, part, full=False):
        return []
