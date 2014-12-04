# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Mehdi Abaakouk <mehdi.abaakouk@enovance.com>
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

from oslo.config import cfg
from oslo.utils import importutils

from gnocchi.openstack.common import log
from gnocchi import storage
from gnocchi.storage import _carbonara

LOG = log.getLogger(__name__)

# NOTE(sileht): rados module is not available on pypi
rados = importutils.try_import('rados')

OPTS = [
    cfg.StrOpt('ceph_pool',
               default='gnocchi',
               help='Ceph pool name to use.'),
    cfg.StrOpt('ceph_username',
               default=None,
               help='Ceph username (ie: client.admin).'),
    cfg.StrOpt('ceph_keyring',
               default=None,
               help='Ceph keyring path.'),
    cfg.StrOpt('ceph_conffile',
               default='/etc/ceph/ceph.conf',
               help='Ceph configuration file.'),
]

cfg.CONF.register_opts(OPTS, group="storage")


class CephStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(CephStorage, self).__init__(conf)
        self.conf = conf
        self.pool = conf.ceph_pool

    @contextlib.contextmanager
    def _get_ioctx(self):
        options = {}
        if self.conf.ceph_keyring:
            options['keyring'] = self.conf.ceph_keyring

        r = rados.Rados(conffile=self.conf.ceph_conffile,
                        rados_id=self.conf.ceph_username,
                        conf=options)
        r.connect()
        try:
            ctx = r.open_ioctx(self.pool)
            try:
                yield ctx
            finally:
                ctx.close()
        finally:
            r.shutdown()

    @staticmethod
    def _get_object_name(metric, aggregation):
        return "gnocchi_%s_%s" % (metric, aggregation)

    def _create_metric_container(self, metric):
        with self._get_ioctx() as ioctx:
            aggregation = self.aggregation_types[0]
            name = self._get_object_name(metric, aggregation)
            try:
                ioctx.stat(name)
            except rados.ObjectNotFound:
                pass
            else:
                raise storage.MetricAlreadyExists(metric)

    def _store_metric_measures(self, metric, aggregation, data):
        with self._get_ioctx() as ioctx:
            name = self._get_object_name(metric, aggregation)
            ioctx.write_full(name, data)

    def delete_metric(self, metric):
        with self._get_ioctx() as ioctx:
            try:
                for aggregation in self.aggregation_types:
                    name = self._get_object_name(metric, aggregation)
                    ioctx.remove_object(name)
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)

    def _get_measures(self, metric, aggregation):
        try:
            with self._get_ioctx() as ioctx:
                name = self._get_object_name(metric, aggregation)
                offset = 0
                content = b''
                while True:
                    data = ioctx.read(name, offset=offset)
                    if not data:
                        break
                    content += data
                    offset += len(content)
                return content
        except rados.ObjectNotFound:
            raise storage.MetricDoesNotExist(metric)
