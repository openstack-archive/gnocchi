# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat
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
import contextlib
import datetime
import os
import uuid

import six

from gnocchi.storage.common import redis
from gnocchi.storage.incoming import _carbonara


class RedisStorage(_carbonara.CarbonaraBasedStorage):

    def __init__(self, conf):
        super(RedisStorage, self).__init__(conf)
        self._client = redis.get_client(conf)

    def _build_measure_path(self, metric_id, random_id=None):
        path = os.path.join(self.MEASURE_PREFIX, six.text_type(metric_id))
        if random_id:
            if random_id is True:
                now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
                random_id = six.text_type(uuid.uuid4()) + now
            return os.path.join(path, random_id)
        return path

    def _store_new_measures(self, metric, data):
        self._client.set(self._build_measure_path(metric.id, True), data)

    def _build_report(self, details):
        metric_details = {}
        match = os.path.join(self.MEASURE_PREFIX, "*")
        for metric in self._client.scan_iter(match=match):
            metric_details[metric] = len(
                self._list_measures_container_for_metric_id(metric))
        return (len(metric_details.keys()), sum(metric_details.values()),
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, size, part, full=False):
        match = os.path.join(self.MEASURE_PREFIX, "*")
        measures = list(self._client.scan_iter(match=match))
        if full:
            return set(measures)
        return set(measures[size * part:size * (part + 1)])

    def _list_measures_container_for_metric_id(self, metric_id):
        match = os.path.join(self._build_measure_path(metric_id), "*")
        return list(self._client.scan_iter(match=match))

    def _delete_measures_keys_for_metric_id(self, metric_id, keys):
        self._client.delete(self._build_measure_path(metric_id), *keys)

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        keys = self._list_measures_container_for_metric_id(metric_id)
        self._delete_measures_keys_for_metric_id(metric_id, keys)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        keys = self._list_measures_container_for_metric_id(metric.id)
        measures = []
        for k in keys:
            data = self._client.get(k)
            measures.extend(self._unserialize_measures(k, data))

        yield measures

        self._delete_measures_keys_for_metric_id(metric.id, keys)
