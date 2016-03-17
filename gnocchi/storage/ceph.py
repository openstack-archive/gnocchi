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
import itertools
import logging
import uuid
import time

from oslo_config import cfg
from oslo_utils import importutils

from gnocchi import storage
from gnocchi.storage import _carbonara


LOG = logging.getLogger(__name__)

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

        LOG.info("Ceph storage backend use '%s' python library" %
                 rados)

        # NOTE(sileht): librados handles reconnection itself,
        # by default if a call timeout (30sec), it raises
        # a rados.Timeout exception, and librados
        # still continues to reconnect on the next call
        self.rados = rados.Rados(conffile=conf.ceph_conffile,
                                 rados_id=conf.ceph_username,
                                 conf=options)
        self.rados.connect()

        self.lock_name = "measures"
        self.MEASURE_DONE_PREFIX = self.MEASURE_PREFIX + "_done"

    def cleanup(self):
        with self._get_ioctx() as ioctx:
            try:
                ioctx.unlock(self.MEASURE_PREFIX, self.lock_name, 'del')
            except Exception:
                pass

    @contextlib.contextmanager
    def _measures_lock_allow_concurency(self, ioctx, cookie):
        while True:
            try:
                ioctx.lock_shared(self.MEASURE_PREFIX, self.lock_name, cookie,
                                  cookie)
                break
            except rados.ObjectBusy:
                time.sleep(0.1)
                LOG.debug("waiting for shared lock for %s" % cookie)

        yield
        ioctx.unlock(self.MEASURE_PREFIX, self.lock_name, cookie)

    @contextlib.contextmanager
    def _measures_lock_no_concurency(self, ioctx, cookie):
        # NOTE(sileht): perhaps we should prioritize this one
        # because when we call _list_metric_with_measures_to_process if
        # other workers continue to grab the shared lock, in worse case
        # this will wait until all workers tried to acquire the exclusive one
        while True:
            try:
                ioctx.lock_exclusive(self.MEASURE_PREFIX, self.lock_name,
                                     cookie)
                break
            except rados.ObjectBusy:
                LOG.debug("waiting for exclusive lock for %s" % cookie)
                time.sleep(0.1)
        yield
        ioctx.unlock(self.MEASURE_PREFIX, self.lock_name, cookie)

    def _store_measures(self, metric, data):
        # NOTE(sileht): list all objects in a pool is too slow with
        # many objects (2min for 20000 objects in 50osds cluster),
        # and enforce us to iterrate over all objects
        # So we create an object MEASURE_PREFIX, that have as
        # xattr the list of objects to process
        name = "_".join((
            self.MEASURE_PREFIX,
            str(metric.id),
            str(uuid.uuid4()),
            datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")))
        with self._get_ioctx() as ioctx:
            ioctx.write_full(name, data)
            with self._measures_lock_allow_concurency(ioctx,
                                                      "_store_measures"):
                ioctx.append(self.MEASURE_PREFIX, name + "\n")

    def _build_report(self, details):
        with self._get_ioctx() as ioctx:
            with self._measures_lock_allow_concurency(ioctx, "_build_report"):
                names = self._list_object_names_to_process(ioctx)
                if not names:
                    return 0, 0, {} if details else None
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

    def _list_object_names_to_process(self, ioctx, prefix=None):
        try:
            data = self._get_object_content(ioctx, self.MEASURE_PREFIX)
            names = data.split()[:-1]
        except rados.ObjectNotFound:
            return ()
        try:
            data = self._get_object_content(ioctx, self.MEASURE_DONE_PREFIX)
            done = data.split()[:-1]
        except rados.ObjectNotFound:
            done = []
        names = set(names) - set(done)
        if prefix is None:
            return names
        return (name for name in names if name.startswith(prefix))

    def _pending_measures_to_process_count(self, metric_id):
        with self._get_ioctx() as ioctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
            with self._measures_lock_allow_concurency(
                    ioctx, "_pending_measures_to_process_count "):
                return len(list(self._list_object_names_to_process(
                    ioctx, object_prefix)))

    def _list_metric_with_measures_to_process(self, block_size, full=False):
        with self._get_ioctx() as ioctx:
            with self._measures_lock_no_concurency(
                    ioctx, "_list_metric_with_measures_to_process"):
                names = self._list_object_names_to_process(ioctx)

                # Cleanup measures and measures_done object
                if names:
                    ioctx.write_full(self.MEASURE_PREFIX,
                                     "\n".join(names) + "\n")
                else:
                    ioctx.trunc(self.MEASURE_PREFIX, 0)
                ioctx.trunc(self.MEASURE_DONE_PREFIX, 0)
            if not names:
                return []
        metrics = set()
        if full:
            objs_it = names
        else:
            objs_it = itertools.islice(
                names, block_size * self.partition, None)
        objs_it = list(objs_it)
        print objs_it
        for name in objs_it:
            metrics.add(name.split("_")[1])
            if full is False and len(metrics) >= block_size:
                break
        return metrics

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        with self._get_ioctx() as ctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
            with self._measures_lock_allow_concurency(
                    ctx, "_delete_unprocessed_measures_for_metric_id"):
                object_names = self._list_object_names_to_process(
                    ctx, object_prefix)
                for n in object_names:
                    try:
                        ctx.append(self.MEASURE_DONE_PREFIX, n + '\n')
                    finally:
                        ctx.aio_remove(n)

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        with self._get_ioctx() as ctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric.id)
            with self._measures_lock_allow_concurency(
                    ctx, "_process_measure_for_metric_read"):
                object_names = list(self._list_object_names_to_process(
                    ctx, object_prefix))

            measures = []
            for n in object_names:
                try:
                    data = self._get_object_content(ctx, n)
                except rados.ObjectNotFound:
                    # WHAT ???!!!
                    LOG.exception("%s doesn't exists anymore" % n)
                    continue
                measures.extend(self._unserialize_measures(data))

            yield measures

            # Now clean objects and xattrs
            with self._measures_lock_allow_concurency(
                    ctx, "_process_measure_for_metric_write"):
                for n in object_names:
                    try:
                        ctx.append(self.MEASURE_DONE_PREFIX, n + '\n')
                    finally:
                        ctx.aio_remove(n)

    def _get_ioctx(self):
        return self.rados.open_ioctx(self.pool)

    @staticmethod
    def _get_object_name(metric, timestamp_key, aggregation, granularity):
        return str("gnocchi_%s_%s_%s_%s" % (
            metric.id, timestamp_key, aggregation, granularity))

    @staticmethod
    def _object_exists(ioctx, name):
        try:
            ioctx.stat(name)
            return True
        except rados.ObjectNotFound:
            return False

    def _create_metric(self, metric):
        name = "gnocchi_%s_container" % metric.id
        with self._get_ioctx() as ioctx:
            if self._object_exists(ioctx, name):
                raise storage.MetricAlreadyExists(metric)
            else:
                ioctx.write_full(name, "metric created")

    def _store_metric_measures(self, metric, timestamp_key,
                               aggregation, granularity, data):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity)
        with self._get_ioctx() as ioctx:
            ioctx.write_full(name, data)
            ioctx.set_xattr("gnocchi_%s_container" % metric.id, name, "")

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity)
        with self._get_ioctx() as ioctx:
            ioctx.rm_xattr("gnocchi_%s_container" % metric.id, name)
            ioctx.remove_object(name)

    def _delete_metric(self, metric):
        with self._get_ioctx() as ioctx:
            try:
                xattrs = ioctx.get_xattrs("gnocchi_%s_container" % metric.id)
            except rados.ObjectNotFound:
                pass
            else:
                for xattr, _ in xattrs:
                    ioctx.aio_remove(xattr)
            for name in ('container', 'none'):
                ioctx.aio_remove("gnocchi_%s_%s" % (metric.id, name))

    def _get_measures(self, metric, timestamp_key, aggregation, granularity):
        try:
            with self._get_ioctx() as ioctx:
                name = self._get_object_name(metric, timestamp_key,
                                             aggregation, granularity)
                return self._get_object_content(ioctx, name)
        except rados.ObjectNotFound:
            with self._get_ioctx() as ioctx:
                if self._object_exists(
                        ioctx, "gnocchi_%s_container" % metric.id):
                    raise storage.AggregationDoesNotExist(metric, aggregation)
                else:
                    raise storage.MetricDoesNotExist(metric)

    def _list_split_keys_for_metric(self, metric, aggregation, granularity):
        with self._get_ioctx() as ioctx:
            try:
                xattrs = ioctx.get_xattrs("gnocchi_%s_container" % metric.id)
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)
            keys = []
            for xattr, value in xattrs:
                _, metric_id, key, agg, g = xattr.split('_', 4)
                if aggregation == agg and granularity == float(g):
                    keys.append(key)

        return keys

    def _get_unaggregated_timeserie(self, metric):
        try:
            with self._get_ioctx() as ioctx:
                return self._get_object_content(
                    ioctx, "gnocchi_%s_none" % metric.id)
        except rados.ObjectNotFound:
            raise storage.MetricDoesNotExist(metric)

    def _store_unaggregated_timeserie(self, metric, data):
        with self._get_ioctx() as ioctx:
            ioctx.write_full("gnocchi_%s_none" % metric.id, data)

    @staticmethod
    def _get_object_content(ioctx, name):
        offset = 0
        content = b''
        while True:
            data = ioctx.read(name, offset=offset)
            if not data:
                break
            content += data
            offset += len(data)
        return content

    # The following methods deal with Gnocchi <= 1.3 archives
    def _get_metric_archive(self, metric, aggregation):
        """Retrieve data in the place we used to store TimeSerieArchive."""
        try:
            with self._get_ioctx() as ioctx:
                return self._get_object_content(
                    ioctx, str("gnocchi_%s_%s" % (metric.id, aggregation)))
        except rados.ObjectNotFound:
            raise storage.AggregationDoesNotExist(metric, aggregation)

    def _store_metric_archive(self, metric, aggregation, data):
        """Stores data in the place we used to store TimeSerieArchive."""
        with self._get_ioctx() as ioctx:
            ioctx.write_full(
                str("gnocchi_%s_%s" % (metric.id, aggregation)), data)

    def _delete_metric_archives(self, metric):
        with self._get_ioctx() as ioctx:
            for aggregation in metric.archive_policy.aggregation_methods:
                try:
                    ioctx.remove_object(
                        str("gnocchi_%s_%s" % (metric.id, aggregation)))
                except rados.ObjectNotFound:
                    pass
