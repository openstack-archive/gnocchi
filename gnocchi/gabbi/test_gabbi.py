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

"""A test module to exercise the Gnocchi API with gabbi."""

import os
import tempfile
import uuid


from gabbi import driver
from oslo.config import fixture as fixture_config
import sqlalchemy.engine.url as sqlalchemy_url
import sqlalchemy_utils
from testtools import testcase

from gnocchi import indexer
from gnocchi.rest import app


TESTS_DIR = 'gabbits'


def setup_test_environment():
    CONF = fixture_config.Config().conf
    CONF([], project='gnocchi')
    CONF.import_opt('file_basepath', 'gnocchi.storage.file',
                    group='storage')
    CONF.set_override('policy_file',
                      os.path.abspath('etc/gnocchi/policy.json'))
    CONF.set_override('file_basepath', tempfile.mkdtemp(), 'storage')
    CONF.set_override('driver', 'file', 'storage')
    CONF.set_override('driver', 'sqlalchemy', 'indexer')

    # Get the indexer so we have some configuration to override
    index = indexer.get_driver(CONF)

    # NOTE(chdent): Fair bit of duplication with gnocchi.test.base.
    db_url = os.environ.get('GNOCCHI_TEST_MYSQL_URL', os.environ.get(
        'GNOCCHI_TEST_PGSQL_URL'))
    if db_url is None:
        raise testcase.TestSkipped("No database connection configured")

    url = sqlalchemy_url.make_url(db_url)
    url.database = url.database + str(uuid.uuid4()).replace('-', '')
    db_url = str(url)
    CONF.set_override('connection', db_url, 'database')
    sqlalchemy_utils.create_database(db_url)
    CONF.set_override('middlewares', [], 'api')

    index.connect()
    index.upgrade()


def load_tests(loader, tests, pattern):
    """Provide a TestSuite to the discovery process."""
    setup_test_environment()
    test_dir = os.path.join(os.path.dirname(__file__), TESTS_DIR)
    return driver.build_tests(test_dir, loader, host=None,
                              intercept=app.setup_app)
