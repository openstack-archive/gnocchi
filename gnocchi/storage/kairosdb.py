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
               help='KairosDB port.'),
    ]

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
        return int(round(time_in_sec * 1000))

    @staticmethod
    def _to_hex(metric):
        return uuid.UUID(metric).hex

    @staticmethod
    def _to_epoch(dt):
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

    def _read(self, metric, start_time=1, end_time=None):
        metric_name = '%s-archives' % metric
        return self.kairos.read_absolute([metric_name], start_time, end_time)

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
        ans = self._read(metric)
        results = ans['queries'][0]['results'][0]
        sample_size = ans['queries'][0]['sample_size']

        definitions = []
        aggregation_methods = {}

        if results:
            aggregation_methods = results['tags']['aggregation_methods'][0]
            aggregation_methods = aggregation_methods.replace(',', ':')
            aggregation_methods = aggregation_methods.replace(';', ',')
            aggregation_methods = ast.literal_eval(aggregation_methods)

        for i in range(sample_size):
            granularity_points = results['tags']['granularity_points'][i]
            granularity, points = granularity_points.split('_')
            definitions.append({
                'granularity': float(granularity),
                'points': float(points),
                'back_window': results['values'][i][1],
                'timespan': results['tags']['timespan'][i]
            })
        return definitions, aggregation_methods

    def get_measures(self, metric):
        pass

    def delete_metric(self, metric):
        pass

    def add_measures(self, metric, measures):
        data = []
        earliest_ts = sys.maxsize
        latest_ts = 0
        metric = self._to_hex(metric)

        LOG.debug("Post measures to metric %s" % metric)

        for measure in measures:
            ts = self._to_epoch(measure.timestamp)
            data.append({'name': metric,
                         'timestamp': measure.timestamp,
                         'value': measure.value,
                         'tags': {'resource': 'resource_id'}})
            earliest_ts = min(earliest_ts, ts)
            latest_ts = max(latest_ts, ts)

        print(earliest_ts, latest_ts)
        print(data)
        self._write(data)

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
            data.append({'name': '%s-archives' % metric,
                         'timestamp': metric_time,
                         'value': archive_policy.back_window,
                         'tags': {'metric': self._to_hex(metric),
                                  'timespan': float(definition.timespan),
                                  'granularity_points': '%s_%s' % (
                                      float(definition.granularity),
                                      definition.points),
                                  'aggregation_methods': available_methods}})
        print(data)
        self._write(data)
        time.sleep(2)
        self._get_policy_definitions(metric)
        self.get_measures(metric)
        # self.add_measures(metric, measures)
