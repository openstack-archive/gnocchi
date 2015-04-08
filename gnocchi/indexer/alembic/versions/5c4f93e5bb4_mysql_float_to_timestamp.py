# Copyright 2016 OpenStack Foundation
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
#

"""mysql_float_to_timestamp

Revision ID: 5c4f93e5bb4
Revises: 7e6f9d542f8b
Create Date: 2016-07-25 15:36:36.469847

"""

from alembic import op

from gnocchi.indexer import sqlalchemy_base

# revision identifiers, used by Alembic.
revision = '5c4f93e5bb4'
down_revision = '7e6f9d542f8b'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind and bind.engine.name == "mysql":
        op.alter_column("resource", "started_at",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=False)
        op.alter_column("resource", "ended_at",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=True)
        op.alter_column("resource", "revision_start",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=False)
        op.alter_column("resource_history", "started_at",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=False)
        op.alter_column("resource_history", "ended_at",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=True)
        op.alter_column("resource_history", "revision_start",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=False)
        op.alter_column("resource_history", "revision_end",
                        type_=sqlalchemy_base.TimestampUTC(),
                        nullable=False)
