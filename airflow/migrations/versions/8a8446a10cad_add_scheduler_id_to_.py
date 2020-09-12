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

"""Add scheduler_id to KubernetesWorkerIdentifier

Revision ID: 8a8446a10cad
Revises: e1a11ece99cc
Create Date: 2020-09-12 11:54:50.487508

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '8a8446a10cad'
down_revision = 'e1a11ece99cc'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add external executor ID to TI"""
    with op.batch_alter_table('kube_worker_uuid', schema=None) as batch_op:
        batch_op.add_column(sa.Column('scheduler_job_id', sa.String(length=250), nullable=True))


def downgrade():
    """Unapply Add external executor ID to TI"""
    with op.batch_alter_table('kube_worker_uuid', schema=None) as batch_op:
        batch_op.drop_column('scheduler_job_id')
