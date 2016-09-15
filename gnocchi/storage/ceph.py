# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import errno
import itertools
import uuid

from oslo_config import cfg
from oslo_log import log
from oslo_utils import importutils

from gnocchi import storage
from gnocchi.storage import _carbonara


LOG = log.getLogger(__name__)

for RADOS_MODULE_NAME in ('cradox', 'rados'):
    rados = importutils.try_import(RADOS_MODULE_NAME)
    if rados is not None:
        break
else:
    RADOS_MODULE_NAME = None

if rados is not None and hasattr(rados, 'run_in_thread'):
    rados.run_in_thread = lambda target, args, timeout=None: target(*args)
    LOG.info("rados.run_in_thread is monkeypatched.")


OPTS = [
    cfg.StrOpt('ceph_pool',
               default='gnocchi',
               help='Ceph pool name to use.'),
    cfg.StrOpt('ceph_username',
               help='Ceph username (ie: admin without "client." prefix).'),
    cfg.StrOpt('ceph_secret', help='Ceph key', secret=True),
    cfg.StrOpt('ceph_keyring', help='Ceph keyring path.'),
    cfg.StrOpt('ceph_conffile',
               default='/etc/ceph/ceph.conf',
               help='Ceph configuration file.'),
]


class CephStorage(_carbonara.CarbonaraBasedStorage):

    WRITE_FULL = False

    def __init__(self, conf):
        super(CephStorage, self).__init__(conf)
        self.pool = conf.ceph_pool
        options = {}
        if conf.ceph_keyring:
            options['keyring'] = conf.ceph_keyring
        if conf.ceph_secret:
            options['key'] = conf.ceph_secret

        if not rados:
            raise ImportError("No module named 'rados' nor 'cradox'")

        if (not hasattr(rados, 'WriteOp') or not hasattr(
                getattr(rados, 'WriteOp'), 'truncate')):
            raise ImportError("Your rados python module does not support "
                              "required features for Gnocchi. "
                              "Install 'cradox' (recommended) "
                              "or upgrade 'python-rados' >= 12.0.0")

        LOG.info("Ceph storage backend use '%s' python library" %
                 RADOS_MODULE_NAME)

        # NOTE(sileht): librados handles reconnection itself,
        # by default if a call timeout (30sec), it raises
        # a rados.Timeout exception, and librados
        # still continues to reconnect on the next call
        self.rados = rados.Rados(conffile=conf.ceph_conffile,
                                 rados_id=conf.ceph_username,
                                 conf=options)
        self.rados.connect()
        self.ioctx = self.rados.open_ioctx(self.pool)

        # NOTE(sileht): constants can't be class attributes because
        # they rely on presence of rados module

        # NOTE(sileht): OMAP_XXXX_FLAGS ARE for "MEASURE_PREFIX" object only
        # We don't use any flags for other object

        # NOTE(sileht): We allow to read the measure object on
        # outdated replicats, that safe for us, we will
        # get the new stuffs on next metricd pass.
        self.OMAP_READ_FLAGS = (rados.LIBRADOS_OPERATION_BALANCE_READS |
                                rados.LIBRADOS_OPERATION_SKIPRWLOCKS)

        # NOTE(sileht): That should be safe to manipulate the omap keys
        # with any OSDs at the same times, each osd should replicate the
        # new key to others and same thing for deletion.
        # I wonder how ceph handle rm_omap and set_omap run at same time
        # on the same key. I assume the operation are timestamped so that will
        # be same. If not, they are still one acceptable race here, a rm_omap
        # can finish before all replicats of set_omap are done, but we don't
        # care, if that occurs next metricd run, will just remove it again, no
        # object with the measure have already been delected by previous, so
        # we are safe and good.
        self.OMAP_WRITE_FLAGS = rados.LIBRADOS_OPERATION_SKIPRWLOCKS

        # NOTE(sileht): we currently just wait for the expected number of
        # operation to complete, perhaps we can store more to get better
        # debugging information if need in the futur
        self._pending_operations = []

    def _wait_async_completion(self):
        while self._pending_operations:
            op = self._pending_operations.pop()
            op.wait_for_complete()

    def stop(self):
        self.ioctx.aio_flush()
        self.ioctx.close()
        self.rados.shutdown()
        super(CephStorage, self).stop()

    def upgrade(self, index):
        super(CephStorage, self).upgrade(index)

        # Move names stored in xattrs to omap
        try:
            xattrs = tuple(k for k, v in
                           self.ioctx.get_xattrs(self.MEASURE_PREFIX))
        except rados.ObjectNotFound:
            return
        with rados.WriteOpCtx() as op:
            self.ioctx.set_omap(op, xattrs, tuple([""] * len(xattrs)))
            self.ioctx.operate_write_op(op, self.MEASURE_PREFIX,
                                        flags=self.OMAP_WRITE_FLAGS)

        for xattr in xattrs:
            self.ioctx.rm_xattr(self.MEASURE_PREFIX, xattr)

    def _check_for_metric_upgrade(self, metric):
        lock = self._lock(metric.id)
        with lock:
            container = "gnocchi_%s_container" % metric.id
            try:
                xattrs = tuple(k for k, v in self.ioctx.get_xattrs(container))
            except rados.ObjectNotFound:
                pass
            else:
                with rados.WriteOpCtx() as op:
                    self.ioctx.set_omap(op, xattrs, tuple([""] * len(xattrs)))
                    self.ioctx.operate_write_op(op, container)
                for xattr in xattrs:
                    self.ioctx.rm_xattr(container, xattr)
        super(CephStorage, self)._check_for_metric_upgrade(metric)

    def _store_new_measures(self, metric, data):
        # NOTE(sileht): list all objects in a pool is too slow with
        # many objects (2min for 20000 objects in 50osds cluster),
        # and enforce us to iterrate over all objects
        # So we create an object MEASURE_PREFIX, that have as
        # omap the list of objects to process (not xattr because
        # it doesn't allow to configure the locking behavior)
        name = "_".join((
            self.MEASURE_PREFIX,
            str(metric.id),
            str(uuid.uuid4()),
            datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")))

        self.ioctx.write_full(name, data)

        with rados.WriteOpCtx() as op:
            self.ioctx.set_omap(op, (name,), ("",))
            self.ioctx.operate_write_op(op, self.MEASURE_PREFIX,
                                        flags=self.OMAP_WRITE_FLAGS)

    def _build_report(self, details):
        names = self._list_object_names_to_process()
        metrics = set()
        count = 0
        metric_details = defaultdict(int)
        for name in names:
            count += 1
            metric = name.split("_")[1]
            metrics.add(metric)
            if details:
                metric_details[metric] += 1
        return len(metrics), count, metric_details if details else None

    def _list_object_names_to_process(self, prefix=""):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", prefix, -1)
            try:
                self.ioctx.operate_read_op(
                    op, self.MEASURE_PREFIX, flag=self.OMAP_READ_FLAGS)
            except rados.ObjectNotFound:
                # API have still written nothing
                return ()
            # NOTE(sileht): after reading the libradospy, I'm
            # not sure that ret will have the correct value
            # get_omap_vals transforms the C int to python int
            # before operate_read_op is called, I dunno if the int
            # content is copied during this transformation or if
            # this is a pointer to the C int, I think it's copied...
            if ret == errno.ENOENT:
                return ()
            return (k for k, v in omaps)

    def _pending_measures_to_process_count(self, metric_id):
        object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
        return len(list(self._list_object_names_to_process(object_prefix)))

    def list_metric_with_measures_to_process(self, size, part, full=False):
        names = self._list_object_names_to_process()
        if full:
            objs_it = names
        else:
            objs_it = itertools.islice(names, size * part, size * (part + 1))
        return set([name.split("_")[1] for name in objs_it])

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
        object_names = self._list_object_names_to_process(object_prefix)
        # Now clean objects and omap
        with rados.WriteOpCtx() as op:
            # NOTE(sileht): come on Ceph, no return code
            # for this operation ?!!
            self.ioctx.remove_omap_keys(op, tuple(object_names))
            self.ioctx.operate_write_op(op, self.MEASURE_PREFIX,
                                        flags=self.OMAP_WRITE_FLAGS)

        for n in object_names:
            self.ioctx.aio_remove(n)

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        object_prefix = self.MEASURE_PREFIX + "_" + str(metric.id)
        object_names = list(self._list_object_names_to_process(object_prefix))

        measures = []
        for n in object_names:
            data = self._get_object_content(n)
            measures.extend(self._unserialize_measures(data))

        yield measures

        # Now clean objects and omap
        with rados.WriteOpCtx() as op:
            # NOTE(sileht): come on Ceph, no return code
            # for this operation ?!!
            self.ioctx.remove_omap_keys(op, tuple(object_names))
            self._pending_operations.append(
                self.ioctx.operate_aio_write_op(op, self.MEASURE_PREFIX,
                                                flags=self.OMAP_WRITE_FLAGS)
            )

        for n in object_names:
            self._pending_operations.append(
                self.ioctx.aio_remove(n)
            )

    @staticmethod
    def _get_object_name(metric, timestamp_key, aggregation, granularity,
                         version=3):
        name = str("gnocchi_%s_%s_%s_%s" % (
            metric.id, timestamp_key, aggregation, granularity))
        return name + '_v%s' % version if version else name

    def _object_exists(self, name):
        try:
            self.ioctx.stat(name)
            return True
        except rados.ObjectNotFound:
            return False

    def _create_metric(self, metric):
        name = "gnocchi_%s_container" % metric.id
        if self._object_exists(name):
            raise storage.MetricAlreadyExists(metric)
        else:
            self.ioctx.write_full(name, "metric created")

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity, version)
        with rados.WriteOpCtx() as op:
            if offset is None:
                op.write_full(data)
            else:
                op.write(data, offset=offset)
            self._pending_operations.append(
                self.ioctx.operate_aio_write_op(op, name)
            )
        with rados.WriteOpCtx() as op:
            self.ioctx.set_omap(op, (name,), ("",))
            self._pending_operations.append(
                self.ioctx.operate_aio_write_op(
                    op, "gnocchi_%s_container" % metric.id)
            )

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity, version)
        with rados.WriteOpCtx() as op:
            self.ioctx.remove_omap_keys(op, (name,))
            self.ioctx.operate_write_op(op, "gnocchi_%s_container" % metric.id)
        self.ioctx.aio_remove(name)

    def _delete_metric(self, metric):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
            try:
                self.ioctx.operate_read_op(
                    op, "gnocchi_%s_container" % metric.id)
            except rados.ObjectNotFound:
                return
            if ret == errno.ENOENT:
                return
            for name, _ in omaps:
                self.ioctx.aio_remove(name)
        for name in ('container', 'none'):
            self.ioctx.aio_remove("gnocchi_%s_%s" % (metric.id, name))

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        try:
            name = self._get_object_name(metric, timestamp_key,
                                         aggregation, granularity, version)
            return self._get_object_content(name)
        except rados.ObjectNotFound:
            if self._object_exists("gnocchi_%s_container" % metric.id):
                raise storage.AggregationDoesNotExist(metric, aggregation)
            else:
                raise storage.MetricDoesNotExist(metric)

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
            try:
                self.ioctx.operate_read_op(
                    op, "gnocchi_%s_container" % metric.id)
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)
            if ret == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            keys = set()
            for name, value in omaps:
                meta = name.split('_')
                if (aggregation == meta[3] and granularity == float(meta[4])
                        and self._version_check(name, version)):
                    keys.add(meta[2])
            return keys

    def _get_unaggregated_timeserie(self, metric):
        try:
            return self._get_object_content("gnocchi_%s_none" % metric.id)
        except rados.ObjectNotFound:
            raise storage.MetricDoesNotExist(metric)

    def _store_unaggregated_timeserie(self, metric, data):
        with rados.WriteOpCtx() as op:
            op.write_full(data)
            self._pending_operations.append(
                self.ioctx.operate_aio_write_op(
                    op, "gnocchi_%s_none" % metric.id)
            )

    def _get_object_content(self, name):
        offset = 0
        content = b''
        while True:
            data = self.ioctx.read(name, offset=offset)
            if not data:
                break
            content += data
            offset += len(data)
        return content
