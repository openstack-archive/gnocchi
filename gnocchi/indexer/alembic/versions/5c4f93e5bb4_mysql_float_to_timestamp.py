# -*- encoding: utf-8 -*-
#
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
import sqlalchemy as sa
from sqlalchemy.sql import func

from gnocchi.indexer import sqlalchemy_base

# revision identifiers, used by Alembic.
revision = '5c4f93e5bb4'
down_revision = '7e6f9d542f8b'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind and bind.engine.name == "mysql":
        # NOTE(jd) So that crappy engine that is MySQL does not have "ALTER
        # TABLE … USING …". We need to copy everything and convert…
        for table_name in ("resource", "resource_history"):
            for column in ("started_at", "ended_at",
                           "revision_start", "revision_end"):

                if column == "revision_end" and table_name == "resource":
                    continue

                op.add_column(table_name,
                              sa.Column(column + "_ts",
                                        sqlalchemy_base.TimestampUTC(),
                                        nullable=False))
                t = sa.sql.table(table_name)
                op.execute(t.update().values(
                    **{column + "_ts": func.from_unixtime(column)}))
                op.drop_column(table_name, column)
                op.alter_column(table_name,
                                column + "_ts", new_column_name=column)
