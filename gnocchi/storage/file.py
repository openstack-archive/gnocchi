# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Objectif Libre
#
# Authors: Stéphane Albert
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
import errno
import os
import shutil

from oslo.config import cfg

from gnocchi import storage
from gnocchi.storage import _carbonara


OPTS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi',
               help='Path used to store gnocchi data files.'),
]

cfg.CONF.register_opts(OPTS, group="storage")


class FileStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(FileStorage, self).__init__(conf)
        self.basepath = conf.file_basepath
        self._lock = _carbonara.CarbonaraBasedStorageToozLock(conf)

    def _create_metric_container(self, metric):
        path = os.path.join(self.basepath, metric)
        try:
            os.mkdir(path, 0o750)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise storage.MetricAlreadyExists(metric)
            raise

    def _store_metric_measures(self, metric, aggregation, data):
        path = os.path.join(self.basepath, metric, aggregation)
        with open(path, 'wb') as aggregation_file:
            aggregation_file.write(data)

    def delete_metric(self, metric):
        path = os.path.join(self.basepath, metric)
        try:
            shutil.rmtree(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            raise

    def _get_measures(self, metric, aggregation):
        path = os.path.join(self.basepath, metric, aggregation)
        try:
            with open(path, 'rb') as aggregation_file:
                return aggregation_file.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            raise
