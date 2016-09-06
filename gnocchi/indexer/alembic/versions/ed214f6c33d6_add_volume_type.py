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

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision = 'ed214f6c33d6'
down_revision = '7e6f9d542f8b'
branch_labels = None
depends_on = None


def upgrade():
    engine = op.get_bind().engine
    inspector = Inspector.from_engine(engine)
    table_names = inspector.get_table_names()
    for table_name in table_names:
        if table_name == 'volume':
            op.add_column('volume', sa.Column('volume_type',
                                              sa.String(length=255),
                                              nullable=True))
        elif table_name == 'volume_history':
            op.add_column('volume_history', sa.Column('volume_type',
                                                      sa.String(length=255),
                                                      nullable=True))
