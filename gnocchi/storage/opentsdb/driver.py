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
import six
import time

from oslo.config import cfg

from gnocchi import storage
from gnocchi.storage import opentsdb
from gnocchi.storage.opentsdb import client
from gnocchi.openstack.common import log

Point = collections.namedtuple('Point', ['timestamp', 'value'])

cfg.CONF.import_opt('opentsdb_port', 'gnocchi.storage.opentsdb.client',
                    group='storage')
cfg.CONF.import_opt('opentsdb_host', 'gnocchi.storage.opentsdb.client',
                    group='storage')

LOG = log.getLogger(__name__)


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

    def _to_epoch(self, dt):
        return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

    def create_entity(self, entity, archives):
        """Create an entity.

        For all entities archives are stored in 'archives' metric with current
        time from epoch as a timestamp and entity as a tag.

        :param entity: The entity key.
        :param archives: The archive(s) configuration to use.
                         A list of (seconds, points) that indicates how many
                         points to keep every seconds interval in archives.
        """
        data = []
        for archive in archives:
            # we can store only one *numeric* value in the value data, so
            # let's write to the value just something, but keep granularity
            # and retention in the tag
            data.append({'metric': 'archives',
                         'timestamp': int(time.time()),
                         'value': 0,
                         'tags': {'entity': entity,
                                  'granularity_retention': '%s_%s' % (
                                      archive[0], archive[1])}})
        res_list = client.put_meter(data)

    def _get_archives(self, entity):
        # we grab all the data regardless to the time it was created
        # as archives table is quite synthetic one
        query = ('start=0&m=sum:archives{entity=%s,granularity_retention=*}'
                 % entity)
        res = client.get_query(query)
        res = client.process_response(res)

        archives = []
        for dp in res:
            # as we store granularity and retention in tags let's grab it
            granularity, retention = (
                dp['tags']['granularity_retention'].split('_'))
            archives.append((granularity, retention))
        return archives

    def delete_entity(self, entity):
        """Delete an entity.

        Does not work for the OpenTSDB - we may implement table
        dropping directly for the underlaying HBase, but that's discussible.

        :param entity: The entity key.
        """
        pass

    def add_measures(self, entity, measures):
        """Add a measure to an entity.

        :param entity: The entity measured.
        :param measures: The actual measures.
        """
        data = []
        for measure in measures:
            data.append({'metric': entity,
                         'timestamp': self._to_epoch(measure.timestamp),
                         'value': measure.value,
                         'tags': {'resource': 'resource_id'}})
        res_list = client.put_meter(data)

    def get_measures(self, entity, from_timestamp=None, to_timestamp=None,
                     aggregation='avg'):
        """Get measures for an entity.

        :param entity: The entity measured.
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
            raise opentsdb.OpenTSDBError(
                'No %s aggregation function is supported for the OpenTSDB. '
                'Please use one from the list: %s' %
                (aggregation,
                 [k for k in self.NATIVE_AGGREGATES.keys()
                  if self.NATIVE_AGGREGATES[k] is not None]))

        def compose_query(archive):
            """Returns query to get the measures with needed archive.

            :param archive: tuple of (<granularity>, <retention>) to be used
            """

            # actually we're creating something like the following query:
            # start=%(from)s&end=%(to)s
            #           &m=%(aggr)s:%(period)s-%(downsample)s:%(entity)s
            query = 'start=%s' % from_timestamp

            if to_timestamp is not None:
                query += '&end=%s' % to_timestamp

            # NOTE(dbelova): Actually the 'aggregation' term we use in Gnocchi
            # is 'downsampling' for OpenTSDB. Aggregation combines with the
            # aggregation function datapoints for one metrics (entity) but
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
            period = archive[0]
            # in Gnocchi we store period as seconds
            query += '%ss-%s:' % (period, aggregation)

            # and finally metric name
            query += '%s' % entity
            return query

        datapoints = []

        for archive in self._get_archives(entity):
            query = compose_query(archive)
            resp = client.get_query(query)
            data = client.process_response(resp)
            dps = data[0]['dps']
            dps = [
                Point(timestamp=datetime.datetime.utcfromtimestamp(float(k)),
                      value=v)
                for k, v in six.iteritems(dps)]
            datapoints.extend(dps)

        return dict((dp.timestamp, dp.value) for dp in datapoints)
