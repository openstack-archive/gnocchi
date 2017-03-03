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

"""add bucket info

Revision ID: 9420813ce89d
Revises: 1e1a63d3d186
Create Date: 2017-03-03 16:13:54.106040

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9420813ce89d'
down_revision = '1e1a63d3d186'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('metric', sa.Column('bucket', sa.Integer, nullable=True))
    metric = sa.Table('metric', sa.MetaData(), sa.Column('bucket', sa.Integer))
    op.execute(metric.update().values(bucket=0))
    op.alter_column('metric', 'bucket', nullable=False, existing_nullable=True)

    op.create_table('storage_state',
                    sa.Column('buckets', sa.Integer, nullable=False))
    state = sa.Table('storage_state', sa.MetaData(),
                     sa.Column('buckets', sa.Integer))
    op.execute(state.insert().values(buckets=1))
