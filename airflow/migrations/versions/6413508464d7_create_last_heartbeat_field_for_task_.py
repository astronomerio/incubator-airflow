# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""create last_heartbeat field for task_instance

Revision ID: 6413508464d7
Revises: a56c9515abdc
Create Date: 2019-10-23 19:35:02.834022

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '6413508464d7'
down_revision = 'a56c9515abdc'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add heartbeat to taskinstance"""
    op.add_column('task_instance',
                  sa.Column('last_heartbeat', sa.DateTime, nullable=True))
    op.create_index('idx_task_heartbeat', 'task_instance', ['task_id', 'last_heartbeat'], unique=False)


def downgrade():
    """Unapply add heartbeat to taskinstance"""
    op.drop_index('idx_task_heartbeat', table_name='task_instance')

    op.drop_column('task_instance',
                   sa.Column('last_heartbeat', sa.DateTime, nullable=True))
