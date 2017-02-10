# Copyright 2017 OpenStack Foundation
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

"""Make sure resource.original_resource_id is NOT NULL

Revision ID: 1e1a63d3d186
Revises: 397987e38570
Create Date: 2017-01-26 19:33:35.209688

"""

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils


# revision identifiers, used by Alembic.
revision = '1e1a63d3d186'
down_revision = '397987e38570'
branch_labels = None
depends_on = None


def upgrade():
    for table_name in ('resource', 'resource_history'):
        table = sa.Table(table_name, sa.MetaData(),
                         sa.Column('id',
                                   sqlalchemy_utils.types.uuid.UUIDType(),
                                   nullable=False),
                         sa.Column('original_resource_id', sa.String(255)))
        op.execute(table.update().where(
            table.c.original_resource_id.is_(None)).values(
                {'original_resource_id': sa.cast(table.c.id, sa.String)}))
        op.alter_column(table_name, "original_resource_id", nullable=False,
                        existing_type=sa.String(255),
                        existing_nullable=True)
