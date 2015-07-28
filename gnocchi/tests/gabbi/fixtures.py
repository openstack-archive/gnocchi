#
# Copyright 2015 Red Hat. All Rights Reserved.
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
"""Fixtures for use with gabbi tests."""

import datetime
import os
import shutil
import tempfile
import threading
import time
from unittest import case
import uuid
import warnings

from gabbi import fixture
from oslo_config import cfg
import oslo_messaging
import sqlalchemy.engine.url as sqlalchemy_url
import sqlalchemy_utils

from gnocchi import indexer
from gnocchi.rest import app
from gnocchi import service
from gnocchi import storage


# NOTE(chdent): Hack to restore semblance of global configuration to
# pass to the WSGI app used per test suite. CONF is the olso
# configuration, PECAN_CONF is the pecan application configuration of
# which the critical part is a reference to the current indexer.
CONF = None
PECAN_CONF = None


def setup_app():
    global CONF
    global PECAN_CONF
    return app.setup_app(config=PECAN_CONF, cfg=CONF)


class ConfigFixture(fixture.GabbiFixture):
    """Establish the relevant configuration fixture, per test file.

    Each test file gets its own oslo config and its own indexer and storage
    instance. The indexer is based on the current database url. The storage
    uses a temporary directory.

    To use this fixture in a gabbit add::

        fixtures:
            - ConfigFixture
    """

    def __init__(self):
        self.conf = None
        self.db_url = None
        self.tmp_dir = None

    def start_fixture(self):
        """Create necessary temp files and do the config dance."""

        global CONF
        global PECAN_CONF

        PECAN_CONF = {}
        PECAN_CONF.update(app.PECAN_CONFIG)

        data_tmp_dir = tempfile.mkdtemp(prefix='gnocchi')

        conf = service.prepare_service([])

        CONF = self.conf = conf
        self.tmp_dir = data_tmp_dir

        # Use the indexer set in the conf, unless we have set an
        # override via the environment.
        if 'GNOCCHI_TEST_INDEXER_URL' in os.environ:
            conf.set_override('url',
                              os.environ.get("GNOCCHI_TEST_INDEXER_URL"),
                              'indexer')

        # TODO(jd) It would be cool if Gabbi was able to use the null://
        # indexer, but this makes the API returns a lot of 501 error, which
        # Gabbi does not want to see, so let's just disable it.
        if conf.indexer.url is None or conf.indexer.url == "null://":
            raise case.SkipTest("No indexer configured")

        # Use the presence of DEVSTACK_GATE_TEMPEST as a semaphore
        # to signal we are not in a gate driven functional test
        # and thus should override conf settings.
        if 'DEVSTACK_GATE_TEMPEST' not in os.environ:
            conf.set_override('driver', 'file', 'storage')
            self.conf.set_override(
                'coordination_url',
                os.getenv("GNOCCHI_COORDINATION_URL", "ipc://"),
                'storage')
            conf.set_override('policy_file',
                              os.path.abspath('etc/gnocchi/policy.json'),
                              group="oslo_policy")
            conf.set_override('file_basepath', data_tmp_dir, 'storage')

        # NOTE(jd) All of that is still very SQL centric but we only support
        # SQL for now so let's say it's good enough.
        url = sqlalchemy_url.make_url(conf.indexer.url)

        url.database = url.database + str(uuid.uuid4()).replace('-', '')
        db_url = str(url)
        conf.set_override('url', db_url, 'indexer')
        sqlalchemy_utils.create_database(db_url)

        index = indexer.get_driver(conf)
        index.connect()
        index.upgrade()

        PECAN_CONF['indexer'] = index

        conf.set_override('pecan_debug', False, 'api')

        # Turn off any middleware.
        conf.set_override('middlewares', [], 'api')

        self.index = index

        # start up a thread to async process measures
        self.metricd_thread = MetricdThread(index, storage.get_driver(conf))
        self.metricd_thread.start()

    def stop_fixture(self):
        """Clean up the config fixture and storage artifacts."""
        if hasattr(self, 'metricd_thread'):
            self.metricd_thread.stop()
            self.metricd_thread.join()

        if hasattr(self, 'index'):
            self.index.disconnect()

        if not self.conf.indexer.url.startswith("null://"):
            # Swallow noise from missing tables when dropping
            # database.
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore',
                                        module='sqlalchemy.engine.default')
                sqlalchemy_utils.drop_database(self.conf.indexer.url)
        if self.tmp_dir:
            shutil.rmtree(self.tmp_dir)

        self.conf.reset()


class MetricdThread(threading.Thread):
    """Run metricd in a naive thread to process measures."""

    def __init__(self, index, storer, name='metricd'):
        super(MetricdThread, self).__init__(name=name)
        self.index = index
        self.storage = storer
        self.flag = True

    def run(self):
        while self.flag:
            self.storage.process_measures(self.index)
            time.sleep(0.1)

    def stop(self):
        self.flag = False


class CeilometerSamplesInjection(fixture.GabbiFixture):
    def start_fixture(self):
        url = os.getenv('CEILOMETER_OSLO_MESSAGING_URL')
        if not url:
            raise case.SkipTest("oslo.messaging url of ceilometer not "
                                "configured")
        conf = cfg.ConfigOpts()
        conf([], project='ceilometer-injector')
        self.transport = oslo_messaging.get_transport(conf, url)

        notifier = oslo_messaging.Notifier(
            self.transport, driver="messagingv2", topic="metering",
            publisher_id='telemetry.publisher.gnocchi-inject')

        samples = [{
            'source': 'blackhole',
            'counter_name': 'cpu_util',
            'counter_type': 'gauge',
            'counter_unit': None,
            'counter_volume': 5,
            'user_id': '43f4a2d3-0acb-44e7-9bf6-3a4dc9276022',
            'project_id': '7cd1dc39-637f-4c61-beda-070819eca00d',
            'resource_id': '992a4076-24d1-4c70-aa0f-33ad5dd5fc72',
            'timestamp': datetime.datetime(2014, 1, 1, 12, 0, 0).isoformat(),
            'resource_metadata': {'host': 'compute01',
                                  'image_ref_url': 'http://glance/somewhere',
                                  'display_name': 'my_wonderful_vm',
                                  'instance_flavor_id': 'sobig',
                                  'user_metadata': {
                                      'server_group': 'frontend_stack'}},
            'message_id': 'f9b64f66-6290-4736-b488-13ecf65d4556',
            'message_signature': '',
        }]
        notifier.sample({}, event_type='metering', payload=samples)

    def stop_fixture(self):
        self.transport.cleanup()
