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

import os
import shutil
import tempfile
from unittest import case
import uuid
import warnings

from gabbi import fixture
from oslo.config import fixture as fixture_config
import sqlalchemy.engine.url as sqlalchemy_url
import sqlalchemy_utils

from gnocchi import indexer


class ConfigFixture(fixture.GabbiFixture):
    """Establish the relevant configuration fixture, per test file.

    Each test file gets its own oslo config and its own indexer and storage
    instance. The indexer is based on the current database url. The storage
    uses a temporary directory.

    To use this fixture in a gabbit add::

        fixtures:
            - ConfigFixture
    """

    def start_fixture(self):
        """Do the config dance."""
        fixture = fixture_config.Config()
        data_tmp_dir = tempfile.mkdtemp(prefix='gnocchi')
        coordination_dir = os.path.join(data_tmp_dir, 'tooz')
        # Let exceptions rise and kill the fixture.
        os.mkdir(coordination_dir)
        coordination_url = 'file://%s' % coordination_dir
        conf = fixture.conf
        self.conf = conf
        conf([], project='gnocchi', validate_default_values=True)
        conf.import_opt('file_basepath', 'gnocchi.storage.file',
                        group='storage')
        conf.set_override('policy_file',
                          os.path.abspath('etc/gnocchi/policy.json'))
        conf.set_override('file_basepath', data_tmp_dir, 'storage')
        conf.set_override('driver', 'file', 'storage')
        conf.set_override('coordination_url', coordination_url, 'storage')
        conf.set_override('driver', 'sqlalchemy', 'indexer')
        conf.set_override('pecan_debug', False, 'api')

        # Get the indexer so we have some configuration to override
        index = indexer.get_driver(conf)

        # TODO(chdent): Fair bit of duplication with gnocchi.test.base.
        db_url = os.environ.get('GNOCCHI_TEST_MYSQL_URL', os.environ.get(
            'GNOCCHI_TEST_PGSQL_URL'))
        if db_url is None:
            raise case.SkipTest("No database connection configured")

        url = sqlalchemy_url.make_url(db_url)
        url.database = url.database + str(uuid.uuid4()).replace('-', '')
        self.db_url = str(url)
        conf.set_override('connection', self.db_url, 'database')
        sqlalchemy_utils.create_database(self.db_url)
        conf.set_override('middlewares', [], 'api')

        index.connect()
        index.upgrade()
        self.tmp_dir = data_tmp_dir

    def stop_fixture(self):
        """Clean up the config fixture."""
        self.conf.reset()
        # Swallow noise from missing tables.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', module='sqlalchemy.engine.default')
            sqlalchemy_utils.drop_database(self.db_url)
        shutil.rmtree(self.tmp_dir)
