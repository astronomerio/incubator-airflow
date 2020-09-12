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

"""Drop one_row_id from KubeWorkerIdentifier

Revision ID: 6158ea5a4d55
Revises: 8a8446a10cad
Create Date: 2020-09-12 11:59:19.109314

"""

import sqlalchemy as sa
from alembic import op

RESOURCE_TABLE = "kube_worker_uuid"
# revision identifiers, used by Alembic.
revision = '6158ea5a4d55'
down_revision = '8a8446a10cad'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Drop one_row_id from KubeWorkerIdentifier"""
    op.drop_table(RESOURCE_TABLE)


def downgrade():
    """Unapply Drop one_row_id from KubeWorkerIdentifier"""

    columns_and_constraints = [
        sa.Column("one_row_id", sa.Boolean, server_default=sa.true(), primary_key=True),
        sa.Column("worker_uuid", sa.String(255))
    ]

    conn = op.get_bind()

    # alembic creates an invalid SQL for mssql and mysql dialects
    if conn.dialect.name in {"mysql"}:
        columns_and_constraints.append(
            sa.CheckConstraint("one_row_id<>0", name="kube_worker_one_row_id")
        )
    elif conn.dialect.name not in {"mssql"}:
        columns_and_constraints.append(
            sa.CheckConstraint("one_row_id", name="kube_worker_one_row_id")
        )

    table = op.create_table(
        RESOURCE_TABLE,
        *columns_and_constraints
    )

    op.bulk_insert(table, [
        {"worker_uuid": ""}
    ])
