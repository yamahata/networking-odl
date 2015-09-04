# Copyright 2015 <PUT YOUR NAME/COMPANY HERE>
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

"""journal table

Revision ID: 1cb1aa18407e
Revises: 247501328046
Create Date: 2015-09-04 01:20:07.041921

"""

# revision identifiers, used by Alembic.
revision = '1cb1aa18407e'
down_revision = '247501328046'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'odl_journal',
        sa.Column('seqnum', sa.Integer, autoincrement=True),
        sa.Column('resource_type', sa.String(255), nullable=True),
        sa.Column('resource_id', sa.String(36), nullable=False),
        sa.Column('operation', sa.String(255), nullable=False),
        sa.Column('data', sa.String(4096), nullable=False),
        sa.Column('status', sa.String(32), nullable=False),

        sa.PrimaryKeyConstraint('seqnum')
    )
