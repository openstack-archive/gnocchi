# -*- encoding: utf-8 -*-
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
from collections import defaultdict
import contextlib
import datetime
import uuid

import six

from gnocchi.storage.common import swift
from gnocchi.storage.incoming import _carbonara

swclient = swift.swclient
swift_utils = swift.swift_utils


class SwiftStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
        self.swift = swift.get_connection(conf)

    def _sack(self, sack):
        return self.SACK_PREFIX % sack

    def upgrade(self, index):
        super(SwiftStorage, self).upgrade(index)
        for i in six.moves.range(self.NUM_SACKS):
            self.swift.put_container(self._sack(i))

    def _store_new_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.swift.put_object(
            self._sack(self.compute_sack(metric.id)),
            six.text_type(metric.id) + "/" + six.text_type(uuid.uuid4()) + now,
            data)

    def _build_report(self, details):
        metric_details = defaultdict(int)
        nb_metrics = 0
        measures = 0
        for i in six.moves.range(self.NUM_SACKS):
            if details:
                headers, files = self.swift.get_container(self._sack(i),
                                                          full_listing=True)
                for f in files:
                    metric, __ = f['name'].split("/", 1)
                    metric_details[metric] += 1
            else:
                headers, files = self.swift.get_container(self._sack(i),
                                                          delimiter='/',
                                                          full_listing=True)
                nb_metrics += len(files)
            measures += int(headers.get('x-container-object-count'))
        return (nb_metrics or len(metric_details), measures,
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, sack):
        headers, files = self.swift.get_container(self._sack(sack),
                                                  delimiter='/',
                                                  full_listing=True)
        return set(f['subdir'][:-1] for f in files if 'subdir' in f)

    def _list_measure_files_for_metric_id(self, sack, metric_id):
        headers, files = self.swift.get_container(
            self._sack(sack), path=six.text_type(metric_id),
            full_listing=True)
        return files

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        sack = self.compute_sack(metric_id)
        files = self._list_measure_files_for_metric_id(sack, metric_id)
        swift.bulk_delete(self.swift, self._sack(sack), files)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        sack = self.compute_sack(metric.id)
        files = self._list_measure_files_for_metric_id(sack, metric.id)

        measures = []
        for f in files:
            headers, data = self.swift.get_object(
                self._sack(sack), f['name'])
            measures.extend(self._unserialize_measures(f['name'], data))

        yield measures

        # Now clean objects
        swift.bulk_delete(self.swift, self._sack(sack), files)
