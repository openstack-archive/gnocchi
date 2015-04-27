# Copyright 2015 eNovance
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
from alembic import command
import mock
from oslo_db.sqlalchemy import test_migrations

from gnocchi.indexer import sqlalchemy_base
from gnocchi.tests import base


class ModelsMigrationsSync(base.TestCase,
                           test_migrations.ModelsMigrationsSync):

    no_upgrade = True

    def setUp(self):
        super(ModelsMigrationsSync, self).setUp()
        self.db = mock.Mock()

    @staticmethod
    def get_metadata():
        return sqlalchemy_base.Base.metadata

    def get_engine(self):
        return self.index.engine_facade.get_engine()

    def db_sync(self, engine):
        cfg = self.index._get_alembic_config()
        cfg.conf = self.conf
        command.upgrade(cfg, "head")
