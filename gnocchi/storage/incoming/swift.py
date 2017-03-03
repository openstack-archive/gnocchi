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
        
    def upgrade(self, index):
        super(SwiftStorage, self).upgrade(index)
        buckets = index.get_storage_state().buckets
        for i in range(buckets):
            self.swift.put_container('measure%s' % i)

    def _store_new_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.swift.put_object(
            'measure%s' % metric.bucket,
            six.text_type(metric.id) + "/" + six.text_type(uuid.uuid4()) + now,
            data)

    def _build_report(self, buckets, details):
        metric_details = defaultdict(int)
        nb_metrics = 0
        measures = 0
        for i in range(buckets):
            if details:
                headers, files = self.swift.get_container('measure%s' % i,
                                                          full_listing=True)
                for f in files:
                    metric, __ = f['name'].split("/", 1)
                    metric_details[metric] += 1
            else:
                headers, files = self.swift.get_container('measure%s' % i,
                                                          delimiter='/',
                                                          full_listing=True)
                nb_metrics += len(files)
            measures += int(headers.get('x-container-object-count'))
        return (nb_metrics or len(metric_details), measures,
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, bucket):
        headers, files = self.swift.get_container('measure%s' % bucket,
                                                  delimiter='/',
                                                  full_listing=True)
        return set(f['subdir'][:-1] for f in files if 'subdir' in f)

    def _list_measure_files_for_metric_id(self, bucket, metric_id):
        headers, files = self.swift.get_container(
            'measure%s' % bucket, path=six.text_type(metric_id),
            full_listing=True)
        return files

    def delete_unprocessed_measures_for_metric(self, metric):
        files = self._list_measure_files_for_metric_id(metric.bucket,
                                                       metric.id)
        swift.bulk_delete(self.swift, 'measure%s' % metric.bucket, files)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        files = self._list_measure_files_for_metric_id(metric.bucket,
                                                       metric.id)

        measures = []
        for f in files:
            headers, data = self.swift.get_object(
                'measure%s' % metric.bucket, f['name'])
            measures.extend(self._unserialize_measures(f['name'], data))

        yield measures

        # Now clean objects
        swift.bulk_delete(self.swift, 'measure%s' % metric.bucket, files)
