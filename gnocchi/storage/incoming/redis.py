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
import collections
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

    def _build_measure_path(self, sack, metric_id, random_id=None):
        path = os.path.join(self.SACK_PREFIX % sack, six.text_type(metric_id))
        if random_id:
            if random_id is True:
                now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
                random_id = six.text_type(uuid.uuid4()) + now
            return os.path.join(path, random_id)
        return path

    def _store_new_measures(self, metric, data):
        sack = self.compute_sack(metric.id)
        path = self._build_measure_path(sack, metric.id, True)
        self._client.set(path.encode("utf8"), data)

    def _build_report(self, details):
        metric_details = collections.defaultdict(int)
        match = os.path.join(self.SACK_PREFIX % "*", "*")
        for key in self._client.scan_iter(match=match.encode('utf8')):
            metric = key.decode('utf8').split(os.path.sep)[1]
            metric_details[metric] += 1
        return (len(metric_details.keys()), sum(metric_details.values()),
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, sack):
        match = os.path.join(self.SACK_PREFIX % sack, "*")
        keys = self._client.scan_iter(match=match.encode('utf8'))
        return set([k.decode('utf8').split(os.path.sep)[1] for k in keys])

    def _list_measures_container_for_metric_id(self, sack, metric_id):
        match = os.path.join(self._build_measure_path(sack, metric_id), "*")
        return list(self._client.scan_iter(match=match.encode("utf8")))

    def delete_unprocessed_measures_for_metric(self, metric):
        sack = self.compute_sack(metric.id)
        keys = self._list_measures_container_for_metric_id(sack, metric.id)
        if keys:
            self._client.delete(*keys)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        sack = self.compute_sack(metric.id)
        keys = self._list_measures_container_for_metric_id(sack, metric.id)
        measures = []
        for k in keys:
            data = self._client.get(k)
            sp_key = k.decode('utf8').split("/")[-1]
            measures.extend(self._unserialize_measures(sp_key, data))

        yield measures

        if keys:
            self._client.delete(*keys)
