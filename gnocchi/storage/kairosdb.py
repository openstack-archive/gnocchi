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

import ast
import collections
import datetime
import json
import six
import sys
import time
import uuid

import logging
from oslo.config import cfg
import pyKairosDB

from gnocchi import exceptions as gnocchi_exc
from gnocchi import storage

OPTS = [
    cfg.StrOpt('kairos_host',
               default='localhost',
               help='KairosDB host.'),
    cfg.IntOpt('kairos_port',
               default='8080',
               help='KairosDB port.'),
    ]

cfg.CONF.register_opts(OPTS, group='storage')

LOG = logging.getLogger(__name__)

Point = collections.namedtuple('Point', ['timestamp', 'granularity', 'value'])


class NotImplementedAggregate(gnocchi_exc.NotImplementedError):
    """Error raised in case of unsupported aggregate function usage."""
    def __init__(self, aggregate, supported):
        super(NotImplementedAggregate, self).__init__(
            'No %s aggregation function is supported for the KairosDB. \
            Please use one from the list: %s' % (aggregate, supported)
        )


class KairosStorage(storage.StorageDriver):

    NATIVE_AGGREGATES = dict(
        mean='avg',
        median=None,
        std='dev',
        sum='sum',
        min='min',
        max='max',
        first=None,
        last=None,
    )

    def __init__(self, conf):
        self.kairos = pyKairosDB.connect(conf.kairos_host,
                                         conf.kairos_port)
        self.write_delay = conf.kairosdb_write_delay

    @staticmethod
    def _to_millisec(time_in_sec):
        """Returns time in milliseconds.

        Converts time in float time.time() in seconds format
        to integer milliseconds format.
        """
        return int(round(time_in_sec * 1000))

    @staticmethod
    def _to_hex(metric):
        return uuid.UUID(metric).hex

    @staticmethod
    def _to_epoch(dt):
        """Returns time in milliseconds since Epoch.

        Converts date and time in datetime.datetime() format
        to milliseconds.
        """
        return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

    @staticmethod
    def _get_available_aggregation_methods(metric):
        """Returns supported aggregation methods.

        Compares aggregation methods supported by KairosDB and
        aggregation methods supported by Gnocchi. Returns only
        methods which are present in both as a dictionary where
        the key is the name of the Gnocchi aggregation method
        and the value is the name of the aggregation method in
        KairosDB.
        """
        try:
            policy_methods = metric.archive_policy.aggregation_methods
        except NameError:
            policy_methods = dict()

        return dict((k, v) for k, v in
                    KairosStorage.NATIVE_AGGREGATES.iteritems()
                    if k in policy_methods and v)

    @staticmethod
    def read_query(query, option=None):
        if option == 'results':
            try:
                result = query['queries'][0]['results'][0]
            except KeyError as e:
                LOG.exception('Failed to read the query')
                LOG.warning('failure: %s' % e.message)
                raise
            return result

        if option == 'sample_size':
            try:
                sample_size = query['queries'][0]['sample_size']
            except KeyError as e:
                LOG.exception('Failed to read the query')
                LOG.warning('failure: %s' % e.message)
                raise
            return sample_size

    def _write(self, data):
        LOG.debug('sending data %s' % data)

        try:
            self.kairos.write_metrics(metric_list=data)

        except Exception as e:
            LOG.exception('Write failed')
            LOG.warning('failure: %s' % e.message)
            raise

    def _read(self, metric_name, start=0,
              end=None, tags=None,
              function=None,
              only_read_tags=False):
        return self.kairos.read_absolute(metric_names_list=[metric_name],
                                         start_time=start,
                                         end_time=end,
                                         tags=tags,
                                         query_modifying_function=function,
                                         only_read_tags=only_read_tags)

    def _get_policy_definitions(self, metric):
        """Returns metric policy information.

        Reads the metric policy information from the
        database and return definitions and aggregation
        methods without comparing them with the Gnocchi
        supported aggregation methods.
        """
        ans = self._read(metric_name='archives',
                         tags={'metric': self._to_hex(metric.name)})
        results = self.read_query(ans, option='results')
        sample_size = self.read_query(ans, option='sample_size')

        definitions = []

        if sample_size > 0:
            methods = results['tags']['aggregation_methods'][0]
            methods = methods.replace(',', ':').replace(';', ',')
            methods = ast.literal_eval(methods)

            for i in range(sample_size):
                granularity_points = results['tags']['granularity_points'][i]
                granularity, points = granularity_points.split('_')
                definitions.append({
                    'granularity': int(float(granularity)),
                    'points': float(points),
                    'back_window': results['values'][i][1],
                    'timespan': results['tags']['timespan'][i]
                })
        else:
            raise storage.MetricDoesNotExist(metric)
        return definitions, methods

    def _delete(self, metric_name, start=0, end=None, tags=None):
        if start is None:
            start = 0
        self.kairos.delete_datapoints(metric_names_list=[metric_name],
                                      start_time=start, end_time=end,
                                      tags=tags)

    def delete_datapoints(self, metric, start=0,
                          end=None, tags=None):
        """Deletes data points from the database.

        Deletes data points that satisfy specified
        parameters: start and end time and tags from the
        database.
        """
        self._delete(metric_name=self._to_hex(metric.name),
                     start=start, end=end, tags=tags)

    def delete_metric(self, metric, start=0, end=None):
        """Deletes metric.

        Deletes only metric information from the database.
        Data points are not affected.
        """
        self._delete(metric_name='archives', start=start, end=end,
                     tags={'metric': self._to_hex(metric.name)})

    @staticmethod
    def _get_modifying_function(aggregation, granularity):
        """Returns aggregation function.

        Accepts aggregation method and granularity which
        will be added to the query.
        """
        def func(query):
            if query is not None:
                query['metrics'][0]['aggregators'] = [{'name': aggregation}]
                query['metrics'][0]['aggregators'][0]['sampling'] = {
                    'value': granularity, 'unit': 'seconds'}
            else:
                LOG.exception("The query is empty, nothing to modify")
                raise
        return func

    def get_measures(self, metric, start=0, end=None,
                     aggregation=None, tags=None):
        """Returns measures.

        Returns measurements of the specified metric
        which satisfy the requested parameters: start and
        end time and tags. If aggregation is None, returns
        raw data.
        """
        if not start:
            start = 0
        # KairosDB requires starting time parameter
        definitions, methods = self._get_policy_definitions(metric)
        if aggregation:
            try:
                aggregation = methods.get(aggregation)
            except KeyError:
                raise NotImplementedAggregate(
                    aggregation,
                    [k for k in self.NATIVE_AGGREGATES.keys()
                     if self.NATIVE_AGGREGATES[k] is not None]
                )
        datapoints = []

        for definition in definitions:
            granularity = definition['granularity']
            func = None
            if aggregation:
                func = self._get_modifying_function(aggregation, granularity)
            ans = self._read(metric_name=self._to_hex(metric.name),
                             start=start, end=end, tags=tags,
                             function=func)
            dps = ans['queries'][0]['results'][0]['values']
            points = []
            for dp in dps:
                points.append(
                    Point(
                        timestamp=datetime.datetime.utcfromtimestamp(
                            float(dp[0])),
                        granularity=granularity, value=dp[1]))
            datapoints.extend(points)

        return datapoints

    def add_measures(self, metric, measures):
        """Adds measures.

        Writes both raw and preaggregated (downsampled)
        measures of specified metric to the database.

        Data is downsampled according to metric archive policy.
        """
        data = []
        earliest_ts = sys.maxsize
        latest_ts = 0

        LOG.debug("Post measures to metric %s" %
                  self._to_hex(metric.name))

        for measure in measures:
            ts = self._to_epoch(measure.timestamp)
            data.append({'name': self._to_hex(metric.name),
                         'timestamp': self._to_epoch(measure.timestamp),
                         'value': measure.value,
                         'tags': {'resource': 'resource_id'}})
            earliest_ts = min(earliest_ts, ts)
            latest_ts = max(latest_ts, ts)
        self._write(data)

    def _prepare_downsampling(self, earliest_ts, latest_ts, metric):
        definitions, methods = self._get_policy_definitions(metric)
        for definition in definitions:
            timespan = definition['timespan']
            for aggregation, supported_name in six.iteritems(methods):
                self._write_downsampled_data(
                    metric, aggregation, definition,
                    self._get_stable_point(earliest_ts, timespan,
                                           position='previous'),
                    self._get_stable_point(latest_ts, timespan,
                                           position='next'))

    def _write_downsampled_data(self, metric, aggregation, definition,
                                from_timestamp, to_timestamp):
        downsampled = self.get_measures(metric, aggregation=aggregation,
                                        start=from_timestamp, end=to_timestamp)
        data = [{'name': self._to_hex(metric.name),
                 'timestamp': self._to_epoch(timestamp),
                 'value': value,
                 'tags': {'resource': 'resource_id',
                          'granularity': definition['granularity'],
                          'aggregation': aggregation}} for
                timestamp, granularity, value in downsampled]
        self._write(data)
        time.sleep(self.write_delay)

    @staticmethod
    def _get_stable_point(timestamp, timespan, position='next'):
        shift = 0
        if position == 'next':
            shift = 1
        try:
            timestamp = int(float(timestamp))
            timespan = int(float(timespan))
        except TypeError:
            LOG.error("Invalid type of timestamp or timespan")
            return 0
        else:
            return timestamp - (timestamp % timespan) + shift*timespan

    def create_metric(self, metric):
        """Create a metric.

        Creates a metric in db for all metrics' archives from 'archive_policy'
        parameter with the current time from epoch as a timestamp and metric as
        a tag.
        """
        available_methods = json.dumps(
            self._get_available_aggregation_methods(metric),
            separators=(';', ','))

        # we need to change separators for JSON because current version of
        # KairosDB 0.9.5 and earlier versions don't support colon ':' as a
        # value in tags
        data = []
        metric_time = self._to_millisec(time.time())
        for definition in metric.archive_policy.definition:
            data.append({'name': 'archives',
                         'timestamp': metric_time,
                         'value': metric.archive_policy.back_window,
                         'tags': {'metric': self._to_hex(metric.name),
                                  'timespan': float(definition.timespan),
                                  'granularity_points': '%s_%s' % (
                                      float(definition.granularity),
                                      definition.points),
                                  'aggregation_methods': available_methods}})
        self._write(data)
