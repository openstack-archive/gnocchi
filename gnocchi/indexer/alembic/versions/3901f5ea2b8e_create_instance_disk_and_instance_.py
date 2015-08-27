# Copyright 2015 OpenStack Foundation
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

"""create instance_disk and instance_network tables

Revision ID: 3901f5ea2b8e
Revises: 42ee7f3e25f8
Create Date: 2015-08-27 17:00:25.092891

"""

# revision identifiers, used by Alembic.
revision = '3901f5ea2b8e'
down_revision = '42ee7f3e25f8'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils


def upgrade():
    op.alter_column("resource", "type",
                    type_=sa.Enum('generic', 'instance', 'swift_account', 'volume', 'ceph_account', 'network', 'identity', 'ipmi', 'stack', 'image', 'instance_network', 'instance_disk', name='resource_type_enum'),
                    nullable=False)
    op.alter_column("resource_history", "type",
                    type_=sa.Enum('generic', 'instance', 'swift_account', 'volume', 'ceph_account', 'network', 'identity', 'ipmi', 'stack', 'image', 'instance_network', 'instance_disk', name='resource_type_enum'),
                    nullable=False)

    op.create_table('instance_disk',
        sa.Column('instance_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_instance_disk_id_resource_id", ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )
    op.create_index('ix_instance_disk_id', 'instance_disk', ['id'], unique=False)

    op.create_table('instance_disk_history',
        sa.Column('instance_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('revision', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_instance_disk_history_resource_history_revision", ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('revision'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )
    op.create_index('ix_instance_disk_history_revision', 'instance_disk_history', ['revision'], unique=False)

    op.create_table('instance_network',
        sa.Column('instance_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_instance_network_id_resource_id", ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )
    op.create_index('ix_instance_network_id', 'instance_network', ['id'], unique=False)

    op.create_table('instance_network_history',
        sa.Column('instance_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('revision', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_instance_network_history_resource_history_revision", ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('revision'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )
    op.create_index('ix_instance_network_history_revision', 'instance_network_history', ['revision'], unique=False)
