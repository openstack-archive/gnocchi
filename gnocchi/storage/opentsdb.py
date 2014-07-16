# Copyright 2014: Mirantis Inc.
# All Rights Reserved.
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

import collections
import datetime
import sys
import threading
import time

from oslo.config import cfg
try:
    import opentsdbclient
    import opentsdbclient.client as opentsdb_cl
except ImportError:
    opentsdbclient = None
    opentsdb_cl = None
import six

from gnocchi import exceptions as gnocchi_exc
from gnocchi.openstack.common import log
from gnocchi import storage

Point = collections.namedtuple('Point', ['timestamp', 'granularity', 'value'])

OPENTSDB_OPTS = [
    cfg.IntOpt('opentsdb_port',
               default=4242,
               help='The port for the OpenTSDB server.',
               ),
    cfg.StrOpt('opentsdb_host',
               default='127.0.0.1',
               help='The listen IP for the OpenTSDB REST API interface.',
               ),
    cfg.IntOpt('opentsdb_flush_interval',
               default=1000,
               help='How often, in milliseconds, to flush the data point '
                    'storage write buffer. Should be more or equal the '
                    'according configuration option set in the OpenTSDB.'
               ),
    cfg.IntOpt('send_queue_max_size',
               default=5,
               help='Batch size to be used foe meters sending. This is '
                    'needed, as python-opentsdbclient supports the '
                    'opportunity of message batching, and default batch size '
                    'is 1000 for it - that is too big value for testing at '
                    'least.')
]

cfg.CONF.register_opts(OPENTSDB_OPTS, group="storage")

LOG = log.getLogger(__name__)


class OpenTSDBClientError(Exception):
    """Error raised when no OpenTSDB python client is provided."""

    def __init__(self, msg=None):
        if not msg:
            msg = 'Some error while OpenTSDB usage occurred.'
        super(OpenTSDBClientError, self).__init__(msg)


class NotImplementedAggregate(gnocchi_exc.NotImplementedError):
    """Error raised in case of unsupported aggregation function usage."""

    def __init__(self, aggregate, supported):
        super(NotImplementedAggregate, self).__init__(
            'No %s aggregation function is supported for the OpenTSDB. '
            'Please use one from the list: %s' % (aggregate, supported))


class OpenTSDBStorage(storage.StorageDriver):
    # default OpenTSDB aggregation functions are the following:
    # 'min', 'mimmin', 'max', 'mimmax', 'dev', 'sum', 'avg', 'zimsum'
    # so there is no analog for the median, first and last
    # for more info go here:
    # http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html
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
        if opentsdb_cl is None or opentsdbclient is None:
            raise OpenTSDBClientError('No python-opentsdbclient package found.'
                                      ' Please install it and try again.')
        host = (conf.opentsdb_host, conf.opentsdb_port)
        self.rest_client = opentsdb_cl.get_client([host], protocol='rest')
        self.socket_client = opentsdb_cl.get_client(
            [host], protocol='socket',
            send_queue_max_size=conf.send_queue_max_size)
        self.flush_interval = conf.opentsdb_flush_interval

    @staticmethod
    def _to_epoch(dt):
        return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

    def create_metric(self, metric, back_window, archives):
        """Create an metric.

        For all entities archives are stored in 'archives' metric with current
        time from epoch as a timestamp and metric as a tag.

        :param metric: The metric key.
        :param back_window: number of maximum granularity windows to support
                            writing data for
        :param archives: The archive(s) configuration to use.
                         A list of (seconds, points) that indicates how many
                         points to keep every seconds interval in archives.
        """
        data = []
        for archive in archives:
            # we can store only one *numeric* value in the value data, so
            # let's write to the value back_window param, but keep granularity
            # and points in the tag
            data.append({'metric': 'archives',
                         'timestamp': int(time.time()),
                         'value': back_window,
                         'tags': {'metric': metric,
                                  'timespan': float(archive['timespan']),
                                  'granularity_points': '%s_%s' % (
                                      float(archive['granularity']),
                                      archive['points'])}})
        # note(dbelova): by default OpenTSDB client batches meters to be
        # sent (default batch size is 1000), so you definitely need to specify
        # parameter send_queue_max_size of OpenTSDB client instance.
        self.socket_client.put_meter(data)

    def _get_archives(self, metric):
        # we grab all the data regardless to the time it was created
        # as archives table is quite synthetic one
        query = ('start=0&m=sum:archives{metric=%s,granularity_points=*}'
                 % metric)
        res = self.rest_client.get_query(query)
        res = self.rest_client.process_response(res)

        archives = []
        for dp in res:
            # as we store granularity and points in tags let's grab it
            granularity, points = (
                dp['tags']['granularity_points'].split('_'))
            archive_dps = dp['dps']
            archives.append({'granularity': float(granularity),
                             'points': points,
                             'back_window': archive_dps[archive_dps.keys()[0]],
                             'timespan': float(dp['tags']['timespan'])})
        return archives

    def delete_metric(self, metric):
        """Delete an metric.

        Does not work for the OpenTSDB - we may implement table
        dropping directly for the underlaying HBase, but that's discussible.

        :param metric: The metric key.
        """
        raise NotImplementedError('Entity deletion is not supported for '
                                  'OpenTSDB backend.')

    def add_measures(self, metric, measures):
        """Add a measure to an metric.

        :param metric: The metric measured.
        :param measures: The actual measures.
        """
        data = []
        earliest_ts = sys.maxsize
        latest_ts = 0

        archives = self._get_archives(metric)

        # note(dbelova): we'll request only data in maximum needed time
        # interval. Earliest point will be earliest measure timestamp come
        # without maximum timespan being deducted.
        # WARNING: it makes sense to downsample not actually big time intervals
        # (measured in hours or days I would say).
        # Otherwise it'll mean every time we'll write new measures we'll need
        # to make OpenTSDB calculating downsampling on amazingly big amount of
        # data. I believe OpenTSDB can survive and function with huge load,
        # but exact benchmarks need to be done separately.
        for measure in measures:
            ts = self._to_epoch(measure.timestamp)
            data.append({'metric': metric, 'timestamp': ts,
                         'value': measure.value,
                         'tags': {'resource': 'resource_id'}})
            earliest_ts = min(earliest_ts, ts)
            latest_ts = max(latest_ts, ts)

        # write raw data
        # I suppose this should be done separately and firstly, even if
        # downsampled data won't be written for some reason, we'll have raw one
        # stored
        self.socket_client.put_meter(data)

        # todo(dbelova): check somewhere before earliest TS
        # due to the back_window

        # TODO(dbelova): that's not a good idea to create new and new threads
        # in real life, where usually we'll receive one measure in the
        # add_measures -> let's leave this code for now as POC, but in future
        # we need background process of downsampling with periodic job, not
        # just-in-time downsampling
        time.sleep(self.flush_interval)
        threads = []
        for archive in archives:
            for aggr, supported in six.iteritems(self.NATIVE_AGGREGATES):
                if supported:
                    t = threading.Thread(
                        target=self._write_downsampled_data,
                        args=(metric, supported, archive,
                              earliest_ts - int(archive['timespan']),
                              latest_ts))
                    t.start()
                    threads.append(t)
        for t in threads:
            t.join()

    def _write_downsampled_data(self, metric, aggregation, archive, from_ts,
                                to_ts):
        data = []
        query = self.compose_query(metric, archive['granularity'],
                                   from_ts, to_ts, aggregation)
        downsampled = self.rest_client.get_query(query)
        dps = self.rest_client.process_response(downsampled)[0]['dps']
        dps = [{'metric': '%s_%s_%s' % (metric, archive['granularity'],
                                        aggregation),
                'timestamp': int(k), 'value': v,
                'tags': {'resource': 'resource_id'}}
               for k, v in six.iteritems(dps)]

        data.extend(dps)
        # write downsampled data for the aggregation function and archive
        # passed.
        # todo(dbelova): Should we just return list and then dump this as one
        # request to OpenTSDB?
        self.socket_client.put_meter(data)

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='avg'):
        """Get measures for an metric.

        :param metric: The metric measured.
        :param from_timestamp: The timestamp to get the measure from.
        :param to_timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """

        if from_timestamp is None:
            # OpenTSDB does not support querying without start point, so
            # we need to define some time from epoch - 0 seconds will mean
            # the very beginning here
            from_timestamp = 0

        aggregation = self.NATIVE_AGGREGATES[aggregation]
        if aggregation is None:
            raise NotImplementedAggregate(
                aggregation,
                [k for k in self.NATIVE_AGGREGATES.keys()
                 if self.NATIVE_AGGREGATES[k] is not None])

        datapoints = []

        for archive in self._get_archives(metric):
            # we grab data from downsampled one
            query = self.compose_query(
                "%s_%s_%s" % (metric, archive['granularity'], aggregation),
                from_timestamp=from_timestamp, to_timestamp=to_timestamp,
                aggregation=aggregation)
            resp = self.rest_client.get_query(query)
            try:
                data = self.rest_client.process_response(resp)
                dps = data[0]['dps']
                dps = [
                    Point(timestamp=datetime.datetime.utcfromtimestamp(
                        float(k)), granularity=archive['granularity'], value=v)
                    for k, v in six.iteritems(dps)]
                datapoints.extend(dps)
            except opentsdbclient.OpenTSDBError as e:
                msg = e.msg
                msg = msg.get('message', '') if type(msg) == dict else msg
                if "No such name for \'metrics\'" in msg:
                    LOG.warn('No measures were added to %s metric for now.' %
                             metric)
                else:
                    raise

        return datapoints

    @staticmethod
    def compose_query(metric, granularity=None, from_timestamp=None,
                      to_timestamp=None, aggregation='avg'):
        """Returns query to get the measures with needed archive.

        :param metric: The metric measured.
        :param granularity: Time period to be downsampled (if needed)
        :param from_timestamp: The timestamp to get the measure from.
        :param to_timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """

        # actually we're creating something like the following query:
        # start=%(from)s&end=%(to)s
        #           &m=%(aggr)s:%(period)s-%(downsample)s:%(metric)s
        query = 'start=%s' % from_timestamp

        if to_timestamp is not None:
            query += '&end=%s' % to_timestamp

        # NOTE(dbelova): Actually the 'aggregation' term we use in Gnocchi
        # is 'downsampling' for OpenTSDB. Aggregation combines with the
        # aggregation function datapoints for one metrics (metric) but
        # actually for different timeseries (that's also defined by tags,
        # for instance). So combination will be found through all available
        # for this concrete timestamp timeseries - if that's not specified
        # - and that's done for every single timestamp. Downsampling means
        # actually reducing number of datapoints without keeping different
        # timestamps presented - new datapoint is created that uses
        # downsampling function for *all* datapoints inside needed
        # downsampling period.

        # aggregation is required for the OpenTSDB as there is no way to
        # predict if it'll be only one timeseries presented
        query += '&m=%s:' % aggregation

        # next step is to add the downsampling inside the to-from period
        # here we need to use granularity from the archive
        # NOTE(dbelova): we use the same aggregation function for the
        # aggregation and downsampling in OpenTSDB terms, although that
        # might be changed if needed
        # in Gnocchi we store period as seconds
        if granularity is not None:
            query += '%ss-%s:' % (granularity, aggregation)

        # and finally metric name
        query += '%s' % metric
        return query
