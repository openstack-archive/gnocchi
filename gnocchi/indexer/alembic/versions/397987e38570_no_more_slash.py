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

"""no-more-slash

Revision ID: 397987e38570
Revises: 5c4f93e5bb4
Create Date: 2017-01-11 16:32:40.421758

"""
import uuid

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils

from gnocchi import utils

# revision identifiers, used by Alembic.
revision = '397987e38570'
down_revision = '5c4f93e5bb4'
branch_labels = None
depends_on = None

resourcehelper = sa.Table(
    'resource',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=False),
    sa.Column('original_resource_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True))
)

resourcehistoryhelper = sa.Table(
    'resource_history',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=False),
    sa.Column('original_resource_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True))
)

metrichelper = sa.Table(
    'metric',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=False),
    sa.Column('name', sa.String(255))

)


def upgrade():
    connection = op.get_bind()
    for tablehelper in [resourcehelper, resourcehistoryhelper]:
        for resource in connection.execute(tablehelper.select().where(
                tablehelper.c.original_resource_id.like('%/%'))):
            new_original_resource_id = resource.original_resource_id.replace(
                '/', '_')
            new_id = uuid.uuid5(utils.RESOURCE_ID_NAMESPACE,
                                new_original_resource_id)
            connection.execute(
                tablehelper.update().where(
                    tablehelper.c.id == resource.id
                ).values(
                    id=new_id,
                    original_resource_id=new_original_resource_id
                )
            )

    for metric in connection.execute(metrichelper.select().where(
            metrichelper.c.name.like("%/%"))):
        connection.execute(
            metrichelper.update().where(
                metrichelper.c.id == metric.id
            ).values(
                name=metric.name.replace('/', '_'),
            )
        )
