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
import os
import random
import shutil
import uuid

from oslo.config import cfg
import pandas
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage


OPTIONS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi/',
               help='Path used to store gnocchi data files.'),
    cfg.BoolOpt('file_use_coordination_driver',
                help='Use coordination driver in case of a shared filesystem.',
                default=False),
    cfg.StrOpt('file_coordination_driver',
               help='File based storage coordination driver',
               default='memcached'),
]

cfg.CONF.register_opts(OPTIONS, group="storage")


class FileStorage(storage.StorageDriver):
    def __init__(self, conf):
        self.basepath = conf.file_basepath
        self.aggregation_types = list(storage.AGGREGATION_TYPES)
        self.coord = None
        if conf.use_coordination_driver:
            self.coord = coordination.get_coordinator(
                conf.file_coordination_driver,
                str(uuid.uuid4()).encode('ascii'))
            self.coord.start()
            # NOTE(jd) So this is a (smart?) optimization: since we're going to
            # lock for each of this aggregation type, if we are using running
            # Gnocchi with multiple processses, let's randomize what we iter
            # over so there are less chances we fight for the same lock!
            random.shuffle(self.aggregation_types)

    def create_entity(self, entity, archive):
        path = os.path.join(self.basepath, entity)
        try:
            os.mkdir(path, 0o750)
        except OSError:
            raise storage.EntityAlreadyExists(entity)
        for aggregation in self.aggregation_types:
            # TODO(jd) Having the TimeSerieArchive.timeserie duplicated in
            # each archive isn't the most efficient way of doing things. We
            # may want to store it as its own object.
            tsc = carbonara.TimeSerieArchive.from_definitions(
                [(pandas.tseries.offsets.Second(second), size)
                 for second, size in archive],
                aggregation_method=aggregation)
            aggregation_path = os.path.join(path, aggregation)
            aggregation_file = open(aggregation_path, 'wb')
            aggregation_file.write(tsc.serialize())
            aggregation_file.close()

    def delete_entity(self, entity):
        path = os.path.join(self.basepath, entity)
        try:
            shutil.rmtree(path)
        except:
            raise storage.EntityDoesNotExist(entity)

    def add_measures(self, entity, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        entity_path = os.path.join(self.basepath, entity)
        for aggregation in self.aggregation_types:
            if self.coord:
                lock = self.coord.get_lock(b"gnocchi-" + entity.encode('ascii')
                                           + b"-" + aggregation.encode('ascii'))
                lock.acquire()
            try:
                aggregation_path = os.path.join(entity_path, aggregation)
                aggregation_file = open(aggregation_path, 'rb')
                contents = aggregation_file.read()
                aggregation_file.close()
            except IOError:
                raise storage.EntityDoesNotExist(entity)
            else:
                tsc = carbonara.TimeSerieArchive.unserialize(contents)
                tsc.set_values([(m.timestamp, m.value) for m in measures])
                aggregation_file = open(aggregation_path, 'wb')
                aggregation_file.write(tsc.serialize())
                aggregation_file.close()
            finally:
                if self.coord:
                    lock.release()

    def get_measures(self, entity, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        path = os.path.join(self.basepath, entity, aggregation)

        try:
            contents = open(path).read()
        except IOError:
            raise storage.EntityDoesNotExist(entity)
        tsc = carbonara.TimeSerieArchive.unserialize(contents)
        return dict(tsc.fetch(from_timestamp, to_timestamp))
