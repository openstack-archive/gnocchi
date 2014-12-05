# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
import random
import threading
import uuid

import pandas
import six.moves.queue
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage


class CarbonaraBasedStorageToozLockMixin(object):
    def _create_coordinator(self):
        self.coord = coordination.get_coordinator(
            self.conf.coordination_url,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()
        # NOTE(jd) So this is a (smart?) optimization: since we're going to
        # lock for each of this aggregation type, if we are using running
        # Gnocchi with multiple processses, let's randomize what we iter
        # over so there are less chances we fight for the same lock!

    def __del__(self):
        if hasattr(self, 'coord'):
            self.coord.stop()

    def _get_lock_ctx(self, metric, aggregation):
        if not hasattr(self, 'coord'):
            self._create_coordinator()
        lock_name = (b"gnocchi-" + metric.encode('ascii')
                     + b"-" + aggregation.encode('ascii'))
        return self.coord.get_lock(lock_name)


class CarbonaraBasedStorage(storage.StorageDriver):
    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self.aggregation_types = list(storage.AGGREGATION_TYPES)
        random.shuffle(self.aggregation_types)

    @staticmethod
    def _create_metric_container(metric):
        pass

    def create_metric(self, metric, back_window, archive_policy):
        self._create_metric_container(metric)
        for aggregation in self.aggregation_types:
            # TODO(jd) Having the TimeSerieArchive.full_res_timeserie duped in
            # each archive isn't the most efficient way of doing things. We
            # may want to store it as its own object.
            # TODO(jd) We should not use Pandas here
            # – abstraction layer violation!
            archive = carbonara.TimeSerieArchive.from_definitions(
                [(pandas.tseries.offsets.Second(v['granularity']), v['points'])
                 for v in archive_policy],
                back_window=back_window,
                aggregation_method=aggregation)
            self._store_metric_measures(metric, aggregation,
                                        archive.serialize())

    @staticmethod
    def _get_measures(metric, aggregation):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, aggregation, data):
        raise NotImplementedError

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        contents = self._get_measures(metric, aggregation)
        archive = carbonara.TimeSerieArchive.unserialize(contents)
        return archive.fetch(from_timestamp, to_timestamp)

    def _add_measures(self, aggregation, metric, measures, exceptions):
        try:
            with self._get_lock_ctx(metric, aggregation):
                contents = self._get_measures(metric, aggregation)
                archive = carbonara.TimeSerieArchive.unserialize(contents)
                try:
                    archive.set_values([(m.timestamp, m.value)
                                        for m in measures])
                except carbonara.NoDeloreanAvailable as e:
                    raise storage.NoDeloreanAvailable(e.first_timestamp,
                                                      e.bad_timestamp)
                self._store_metric_measures(metric, aggregation,
                                            archive.serialize())
        except Exception as e:
            raise
            exceptions.put(e)
            return

    def add_measures(self, metric, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        threads = []
        exceptions = six.moves.queue.Queue()
        for aggregation in self.aggregation_types:
            self._add_measures(aggregation, metric, measures, exceptions)
        return
        if True:
            t = threading.Thread(target=self._add_measures,
                                 args=(aggregation, metric,
                                       measures, exceptions))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if not exceptions.empty():
            # Only raise the first one, not much choice
            raise exceptions.get()
