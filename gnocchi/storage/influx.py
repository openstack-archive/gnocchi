# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Red Hat, Inc.
#
# Authors: Eoghan Glynn <eglynn@redhat.com>
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

from gnocchi.openstack.common import timeutils
import logging
from oslo.config import cfg

import influxdb

from gnocchi import storage

OPTIONS = [
    cfg.StrOpt('influx_host',
               default='localhost',
               help='InfluxDB host.'),
    cfg.IntOpt('influx_port',
               default=8086,
               help='InfluxDB port.'),
    cfg.StrOpt('influx_user',
               default='root',
               help='InfluxDB user name.'),
    cfg.StrOpt('influx_password',
               default='root',
               help='InfluxDB password.'),
    cfg.StrOpt('influx_database',
               default='gnocchi',
               help='InfluxDB database.'),
]

cfg.CONF.register_opts(OPTIONS, group='storage')

LOG = logging.getLogger(__name__)

Archive = collections.namedtuple('Archive', ['granularity', 'retention'])
Format = collections.namedtuple('Fromat', ['timestamp', 'value'])
Point = collections.namedtuple('Point', ['timestamp', 'value'])


class InfluxStorage(storage.StorageDriver):
    """A simple storage driver for InfluxDB.

    Archive (granularity, retention) pairs are stored in psuedo-timeseries
    called <entity_id>-archive.

    Entity data are stored in true timeseries called <entity_id>-archive.

    Granularities coarser than 1s are aggregated by mean.

    Retention by datapoint count is not currently honored.

    Timestamp granularity is limited to 1s, i.e. millisecond precision is
    discarded.

    Related timeseries queries for multi-archive entities are not currently
    batched.
    """

    EPOCH = datetime.datetime(1970, 1, 1)

    def __init__(self, conf):
        self.influx = influxdb.client.InfluxDBClient(conf.influx_host,
                                                     conf.influx_port,
                                                     conf.influx_user,
                                                     conf.influx_password,
                                                     conf.influx_database)

        try:
            dbs = self.influx.get_database_list()
            names = [db['name'] for db in dbs]
            if conf.influx_database not in names:
                self.influx.create_database(conf.influx_database)
        except influxdb.client.InfluxDBClientError as e:
            LOG.exception('database creation failed')
            LOG.warning('failure: %s %d' % (e.message, e.code))
            raise

    def _write(self, data, entity):
        LOG.debug('sending data %s' % data)

        try:
            self.influx.write_points([data])
        except influxdb.client.InfluxDBClientError as e:
            LOG.exception('write failed')
            LOG.warning('failure: %s %d' % (e.message, e.code))
            if e.code == 404:
                raise storage.EntityDoesNotExist(entity)
            raise

    def _query(self, query, entity):
        LOG.debug('sending query %s' % query)

        try:
            return self.influx.query(query)
        except influxdb.client.InfluxDBClientError as e:
            LOG.exception('query failed')
            LOG.warning('failure: %s %d' % (e.message, e.code))
            if e.code == 404:
                raise storage.EntityDoesNotExist(entity)
            raise

    def create_entity(self, entity, archives):
        """Create an entity.

        :param entity: The entity key.
        :param archive: The archive configuration to use.
                        A list of (seconds, points) that indicates how many
                        points to keep every seconds interval in archives.
        """
        name = '%s-archives' % entity
        columns = ['granularity', 'retention']

        data = dict(name=name,
                    columns=columns,
                    points=[[g, r] for g, r in archives])

        self._write(data, entity)

    def delete_entity(self, entity):
        """Delete an entity.

        :param entity: The entity key.
        """
        self._query('drop series %s-data' % entity, entity)
        self._query('drop series %s-archive' % entity, entity)

    @staticmethod
    def _as_seconds(timestamp):
        naive = timeutils.normalize_time(timestamp)
        # TODO(eglynn): millisecond precision is lost
        return int((naive - InfluxStorage.EPOCH).total_seconds())

    def add_measures(self, entity, measures):
        """Add a measure to an entity.

        :param entity: The entity measured.
        :param measures: The actual measures.
        """
        name = '%s-data' % entity
        columns = ['time', 'value']

        data = dict(name=name,
                    columns=columns,
                    points=[[self._as_seconds(m.timestamp), m.value]
                            for m in measures])

        self._write(data, entity)

    def _get_archives(self, entity):
        name = '%s-archives' % entity

        query = 'select * from %s;' % name

        data = self._query(query, entity)
        if data:
            gi = data[0]['columns'].index('granularity')
            ri = data[0]['columns'].index('retention')

            return [Archive(granularity=p[gi], retention=p[ri])
                    for p in data[0]['points']]

        return [Archive(granularity=1, retention=sys.maxint)]

    def get_measures(self, entity, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        """Get measures for an entity.

        :param entity: The entity measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """

        def _select(archive):
            params = dict(name='%s-data' % entity,
                          target='*', group_by='', limit='')

            if archive.granularity > 1:
                #TODO(eglynn): ensure aggregate function names match
                params['target'] = '%s(value)' % aggregation
                params['group_by'] = \
                    'group by time(%ds)' % archive.granularity

            if archive.retention < sys.maxint:
                params['limit'] = 'limit %s' % archive.retention

            if from_timestamp and to_timestamp:
                f = self._as_seconds(timeutils.parse_isotime(from_timestamp))
                t = self._as_seconds(timeutils.parse_isotime(to_timestamp))
                #TODO(eglynn): influx doesn't support '>=' for time comparison
                #   "Cannot use time with '>='"
                params['where'] = 'where time > %ds and time < %ds' % (f, t)
            elif from_timestamp:
                f = self._as_seconds(timeutils.parse_isotime(from_timestamp))
                params['where'] = 'where time > %ds' % f
            elif to_timestamp:
                t = self._as_seconds(timeutils.parse_isotime(to_timestamp))
                params['where'] = 'where time < %ds' % t
            else:
                params['where'] = ''

            select = ('select %(target)s from %(name)s'
                      ' %(group_by)s %(where)s %(limit)s;')
            return select % params

        def _format(archive):
            value = 'value' if archive.granularity == 1 else aggregation
            return Format(timestamp='time', value=value)

        # TODO(eglynn): batch up per-archive queries

        points = []
        for archive in self._get_archives(entity):
            query = _select(archive)

            data = self._query(query, entity)

            LOG.debug('retrived data %s' % data)

            # data format returned by influx:
            #
            # Unaggregated case:
            #   [{"name": entity_id,
            #     "columns": ["time","sequence_number","value"],
            #     "points": [[epoch_seconds, sequence-number, value]
            #
            # Aggregated case:
            #   [{"name":entity_id,
            #     "columns": ["time", aggregate],
            #     "points":  [[epoch_seconds, value]]}]

            if data:
                format = _format(archive)
                ti = data[0]['columns'].index(format.timestamp)
                vi = data[0]['columns'].index(format.value)

                points.extend([Point(timestamp=p[ti], value=p[vi])
                               for p in data[0]['points']])

        def _as_string(timestamp):
            return datetime.datetime.utcfromtimestamp(timestamp)

        # TODO(eglynn): returning a dict keyed by timestamp has two
        #               unfortunate side-effects:
        #
        #               * loses datapoint ordering
        #               * finer-grain datapoints mask coarser-grain

        return dict((_as_string(p.timestamp), p.value) for p in points)
