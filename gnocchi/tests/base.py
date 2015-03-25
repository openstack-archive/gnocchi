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
import functools
import os
import uuid

import fixtures
from oslotest import base
from oslotest import mockpatch
import six
from stevedore import extension
from swiftclient import exceptions as swexc
from testtools import testcase
from tooz import coordination

from gnocchi import archive_policy
from gnocchi import exceptions
from gnocchi import indexer
from gnocchi import service
from gnocchi import storage


class SkipNotImplementedMeta(type):
    def __new__(cls, name, bases, local):
        for attr in local:
            value = local[attr]
            if callable(value) and (
                    attr.startswith('test_') or attr == 'setUp'):
                local[attr] = _skip_decorator(value)
        return type.__new__(cls, name, bases, local)


def _skip_decorator(func):
    @functools.wraps(func)
    def skip_if_not_implemented(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except exceptions.NotImplementedError as e:
            raise testcase.TestSkipped(six.text_type(e))
    return skip_if_not_implemented


class FakeRadosModule(object):
    class ObjectNotFound(Exception):
        pass

    class ioctx(object):
        def __init__(self, kvs):
            self.kvs = kvs
            self.librados = self
            self.io = self

        def __enter__(self):
            return self

        @staticmethod
        def __exit__(exc_type, exc_value, traceback):
            pass

        def rados_lock_exclusive(self, ctx, name, lock, locker, desc, timeval,
                                 flags):
            # Locking a not existing object create an empty one
            # so, do the same in test
            key = name.value.decode('ascii')
            if key not in self.kvs:
                self.kvs[key] = ""
            return 0

        def rados_unlock(self, ctx, name, lock, locker):
            # Locking a not existing object create an empty one
            # so, do the same in test
            key = name.value.decode('ascii')
            if key not in self.kvs:
                self.kvs[key] = ""
            return 0

        @staticmethod
        def close():
            pass

        @staticmethod
        def _validate_key(name):
            if not isinstance(name, str):
                raise TypeError("key is not a 'str' object")

        def write_full(self, key, value):
            self._validate_key(key)
            self.kvs[key] = value

        def stat(self, key):
            self._validate_key(key)
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            else:
                return (1024, "timestamp")

        def read(self, key, length=8192, offset=0):
            self._validate_key(key)
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            else:
                return self.kvs[key][offset:offset+length]

        def remove_object(self, key):
            self._validate_key(key)
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            del self.kvs[key]

    class FakeRados(object):
        def __init__(self, kvs):
            self.kvs = kvs

        @staticmethod
        def connect():
            pass

        @staticmethod
        def shutdown():
            pass

        def open_ioctx(self, pool):
            return FakeRadosModule.ioctx(self.kvs)

    def __init__(self):
        self.kvs = {}

    def Rados(self, *args, **kwargs):
        return FakeRadosModule.FakeRados(self.kvs)

    @staticmethod
    def run_in_thread(method, args):
        return method(*args)

    @staticmethod
    def make_ex(ret, reason):
        raise Exception(reason)


class FakeSwiftClient(object):
    def __init__(self, *args, **kwargs):
        self.kvs = {}

    def put_container(self, container, response_dict=None):
        if response_dict is not None:
            if container in self.kvs:
                response_dict['status'] = 204
            else:
                response_dict['status'] = 201
        self.kvs[container] = {}

    def put_object(self, container, key, obj):
        if hasattr(obj, "seek"):
            obj.seek(0)
            obj = obj.read()
            # TODO(jd) Maybe we should reset the seek(), but well…
        self.kvs[container][key] = obj

    def get_object(self, container, key):
        try:
            return {}, self.kvs[container][key]
        except KeyError:
            raise swexc.ClientException("No such container/object",
                                        http_status=404)

    def delete_object(self, container, obj):
        try:
            del self.kvs[container][obj]
        except KeyError:
            raise swexc.ClientException("No such container/object",
                                        http_status=404)

    def delete_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)
        del self.kvs[container]

    def head_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)


@six.add_metaclass(SkipNotImplementedMeta)
class TestCase(base.BaseTestCase):

    ARCHIVE_POLICIES = {
        'low': archive_policy.ArchivePolicy(
            "low",
            0,
            [
                # 5 minutes resolution for an hour
                archive_policy.ArchivePolicyItem(
                    granularity=300, points=12),
                # 1 hour resolution for a day
                archive_policy.ArchivePolicyItem(
                    granularity=3600, points=24),
                # 1 day resolution for a month
                archive_policy.ArchivePolicyItem(
                    granularity=3600 * 24, points=30),
            ],
        ),
        'medium': archive_policy.ArchivePolicy(
            "medium",
            0,
            [
                # 1 minute resolution for an hour
                archive_policy.ArchivePolicyItem(
                    granularity=60, points=60),
                # 1 hour resolution for a week
                archive_policy.ArchivePolicyItem(
                    granularity=3600, points=7 * 24),
                # 1 day resolution for a year
                archive_policy.ArchivePolicyItem(
                    granularity=3600 * 24, points=365),
            ],
        ),
        'high': archive_policy.ArchivePolicy(
            "high",
            0,
            [
                # 1 second resolution for a day
                archive_policy.ArchivePolicyItem(
                    granularity=1, points=3600 * 24),
                # 1 minute resolution for a month
                archive_policy.ArchivePolicyItem(
                    granularity=60, points=60 * 24 * 30),
                # 1 hour resolution for a year
                archive_policy.ArchivePolicyItem(
                    granularity=3600, points=365 * 24),
            ],
        ),
        'no_granularity_match': archive_policy.ArchivePolicy(
            "no_granularity_match",
            0,
            [
                # 2 second resolution for a day
                archive_policy.ArchivePolicyItem(
                    granularity=2, points=3600 * 24),
                ],
        ),
    }

    @staticmethod
    def path_get(project_file=None):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..',
                                            )
                               )
        if project_file:
            return os.path.join(root, project_file)
        return root

    def setUp(self):
        super(TestCase, self).setUp()
        self.conf = service.prepare_service([])
        self.conf.set_override('policy_file',
                               self.path_get('etc/gnocchi/policy.json'),
                               group="oslo_policy")

        self.conf.set_override(
            'url',
            os.environ.get("GNOCCHI_TEST_INDEXER_URL", "null://"),
            'indexer')

        self.index = indexer.get_driver(self.conf)
        self.index.connect()

        self.conf.set_override('coordination_url',
                               os.getenv("GNOCCHI_COORDINATION_URL", "ipc://"),
                               'storage')

        # NOTE(jd) So, some driver, at least SQLAlchemy, can't create all
        # their tables in a single transaction even with the
        # checkfirst=True, so what we do here is we force the upgrade code
        # path to be sequential to avoid race conditions as the tests run in
        # parallel.
        self.coord = coordination.get_coordinator(
            os.getenv("GNOCCHI_COORDINATION_URL", "ipc://"),
            str(uuid.uuid4()).encode('ascii'))

        with self.coord.get_lock(b"gnocchi-tests-db-lock"):
            self.index.upgrade()

        self.archive_policies = self.ARCHIVE_POLICIES
        # Used in gnocchi.gendoc
        if not getattr(self, "skip_archive_policies_creation", False):
            for name, ap in six.iteritems(self.ARCHIVE_POLICIES):
                # Create basic archive policies
                try:
                    self.index.create_archive_policy(ap)
                except indexer.ArchivePolicyAlreadyExists:
                    pass

        self.useFixture(mockpatch.Patch(
            'swiftclient.client.Connection',
            FakeSwiftClient))

        self.useFixture(mockpatch.Patch('gnocchi.storage.ceph.rados',
                                        FakeRadosModule()))

        self.conf.set_override(
            'driver',
            os.getenv("GNOCCHI_TEST_STORAGE_DRIVER", "null"),
            'storage')

        if self.conf.storage.driver == 'file':
            tempdir = self.useFixture(fixtures.TempDir())
            self.conf.set_override('file_basepath',
                                   tempdir.path,
                                   'storage')

        self.storage = storage.get_driver(self.conf)

        self.mgr = extension.ExtensionManager('gnocchi.aggregates',
                                              invoke_on_load=True)
        self.custom_agg = dict((x.name, x.obj) for x in self.mgr)

    def tearDown(self):
        self.index.disconnect()
        super(TestCase, self).tearDown()
