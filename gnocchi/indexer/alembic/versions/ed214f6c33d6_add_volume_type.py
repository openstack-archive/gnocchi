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

"""add volume_type

Revision ID: ed214f6c33d6
Revises: 7e6f9d542f8b
Create Date: 2016-09-06 14:03:12.097910

"""

import json
import sqlalchemy as sa

from alembic import op

from gnocchi.indexer import sqlalchemy_legacy_resources as legacy


# revision identifiers, used by Alembic.
revision = 'ed214f6c33d6'
down_revision = '7e6f9d542f8b'
branch_labels = None
depends_on = None


def upgrade():
    # Add volume_type column to volume and volume_history table
    try:
        op.add_column('volume', sa.Column('volume_type',
                                          sa.String(length=255),
                                          nullable=True))
        op.add_column('volume_history', sa.Column('volume_type',
                                                  sa.String(length=255),
                                                  nullable=True))
    except sa.exc.NoSuchTableError:
        return

    # Update attributes of volume in resource_type table
    try:
        resource_type = sa.Table(
            'resource_type', sa.MetaData(),
            sa.Column('name', sa.String(255), nullable=False),
            sa.Column('tablename', sa.String(18), nullable=False),
            sa.Column('attributes', sa.Text, nullable=False)
        )
        attributes = legacy.ceilometer_resources.get('volume')
        text_attributes = json.dumps(attributes)
        op.execute(resource_type.update().where(
            resource_type.c.name == 'volume'
        ).values({resource_type.c.attributes: text_attributes}))
    except Exception:
        raise
