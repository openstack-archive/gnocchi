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
import sys
import time
import uuid

import logging
from oslo.config import cfg
import pyKairosDB

from gnocchi import exceptions as gnocchi_exc
from gnocchi import storage

OPTIONS = [
    cfg.StrOpt('kairos_host',
               default='192.168.0.116',
               help='KairosDB host.'),
    cfg.IntOpt('kairos_port',
               default='8080',
               help='KairosDB port.')]

cfg.CONF.register_opts(OPTIONS, group='storage')

LOG = logging.getLogger(__name__)

Point = collections.namedtuple('Point', ['timestamp', 'granularity', 'value'])


class NotImplementedAggregate(gnocchi_exc.NotImplementedError):
    """Error raised in case of unsupported aggregate function usage. """

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
    def _get_available_aggregation_methods(policy_methods):
        return dict((k, v) for k, v in
                    KairosStorage.NATIVE_AGGREGATES.iteritems()
                    if k in policy_methods and v)

    def _write(self, data):
        LOG.debug('sending data %s' % data)

        try:
            self.kairos.write_metrics(metric_list=data)

        except Exception as e:
            LOG.exception('write failed')
            LOG.warning('failure: %s' % e.message)

    def _read(self, metric, start=0,
              end=None, tags={},
              function=None,
              only_read_tags=False):
        return self.kairos.read_absolute(metric_names_list=[metric],
                                         start_time=start,
                                         end_time=end,
                                         tags=tags,
                                         query_modifying_function=function,
                                         only_read_tags=only_read_tags)

    def _get_policy_definitions(self, metric):
        """Get archive policy definitions and aggregation methods.

        :type metric: string
        :param metric: The name of the metric to query (one name at a time)

        :type start_time: float
        :param start_time: The float representing the number of seconds since
        the epoch that this query starts at.

        :type end_time: float
        :param end_time: The float representing the number of seconds since
        the epoch that this query ends at.
        """
        ans = self._read(metric='archives',
                         tags={'metric': self._to_hex(metric)})
        results = ans['queries'][0]['results'][0]
        sample_size = ans['queries'][0]['sample_size']

        definitions = []
        agg_methods = {}

        if sample_size > 0:
            agg_methods = results['tags']['aggregation_methods'][0]
            agg_methods = agg_methods.replace(',', ':').replace(';', ',')
            agg_methods = ast.literal_eval(agg_methods)

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
        return definitions, agg_methods

    def delete_metric(self, metric):
        pass

    @staticmethod
    def _get_modifying_function(aggregation, granularity):

        def func(query):
            if query is not None:
                query['metrics'][0]['aggregators'] = [{'name': aggregation}]
                query['metrics'][0]['aggregators'][0]['sampling'] = {
                    'value': granularity, 'unit': 'seconds'}
            else:
                LOG.exception("The query is empty, nothing to modify")
                raise
        return func

    def get_measures(self,
                     storage_metric,
                     start=0,
                     end=None,
                     aggregation=None,
                     tags={}):
        metric = self._to_hex(storage_metric.name)
        if not start:
            start = 0
        # KairosDB requires starting time parameter
        definitions, agg_methods = self._get_policy_definitions(metric)
        aggregation = agg_methods.get(aggregation)

        if aggregation is None:
            raise NotImplementedAggregate(
                aggregation,
                [k for k in self.NATIVE_AGGREGATES.keys()
                 if self.NATIVE_AGGREGATES[k] is not None])
            aggregation = agg_methods.get('mean')

        datapoints = []

        for definition in definitions:
            granularity = definition['granularity']
            func = self._get_modifying_function(aggregation, granularity)
            ans = self._read(metric=metric, start=start,
                             end=end, tags=tags,
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

    def add_measures(self, storage_metric, measures):
        data = []
        earliest_ts = sys.maxsize
        latest_ts = 0
        metric = self._to_hex(storage_metric.name)

        LOG.debug("Post measures to metric %s" % metric)

        for measure in measures:
            ts = self._to_epoch(measure.timestamp)
            data.append({'name': metric,
                         'timestamp': ts,
                         'value': measure.value,
                         'tags': {'resource': 'resource_id'}})
            earliest_ts = min(earliest_ts, ts)
            latest_ts = max(latest_ts, ts)
        self._write(data)
        time.sleep(2)
        self._read(metric)

    def create_metric(self, storage_metric):
        """Create a metric.

        Creates a metric in db for all metrics' archives from 'archive_policy'
        parameter with the current time from epoch as a timestamp and metric as
        a tag.

        :param metric: The metric key.
        :param archive_policy: The archive definition(s) configuration to use.
                               A list of (seconds, points) that indicates how
                               many points to keep every seconds interval.
        """
        metric = storage_metric.name
        archive_policy = storage_metric.archive_policy
        try:
            policy_aggregation_methods = archive_policy.aggregation_methods
        except NameError:
            policy_aggregation_methods = dict()

        available_methods = self._get_available_aggregation_methods(
            policy_aggregation_methods)

        available_methods = json.dumps(available_methods,
                                       separators=(';', ','))
        # we need to change separators for JSON because current version of
        # KairosDB 0.9.5 and earlier versions don't support colon ':' as a
        # value in tags
        data = []
        metric_time = self._to_millisec(time.time())
        for definition in archive_policy.definition:
            data.append({'name': 'archives',
                         'timestamp': metric_time,
                         'value': archive_policy.back_window,
                         'tags': {'metric': self._to_hex(metric),
                                  'timespan': float(definition.timespan),
                                  'granularity_points': '%s_%s' % (
                                      float(definition.granularity),
                                      definition.points),
                                  'aggregation_methods': available_methods}})
        self._write(data)
        time.sleep(2)
        # metric = self._to_hex(metric)
        # self._read(metric='archives',
        #            tags={'metric': metric},
        #            only_read_tags=True)
        # self._get_policy_definitions(metric)
