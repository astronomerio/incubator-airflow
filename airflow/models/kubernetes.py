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

import uuid

from sqlalchemy import Boolean, Column, String, true as sqltrue
from sqlalchemy.orm import Session

from airflow.models.base import Base
from airflow.utils.session import provide_session


class KubeResourceVersion(Base):
    """Table containing Kubernetes Resource versions"""
    __tablename__ = "kube_resource_version"
    scheduler_job_id = Column(String(255))
    resource_version = Column(String(255), primary_key=True)

    def __init__(self, scheduler_id, resource_version):
        self.scheduler_job_id = scheduler_id
        self.resource_version = resource_version

    @staticmethod
    @provide_session
    def get_current_resource_version(scheduler_job_id, session: Session = None) -> str:
        """Get Current Kubernetes Resource Version from Airflow Metadata DB"""
        row = session.query(KubeResourceVersion) \
            .filter(KubeResourceVersion.scheduler_job_id == str(scheduler_job_id)).one_or_none()
        if row:
            _, resource_version = row
            print("found resource_version {}".format(resource_version))
            return resource_version
        else:
            print(f"adding resource version for scheduler_id {scheduler_job_id}")
            resource_version = KubeResourceVersion(str(scheduler_job_id), '0')
            session.add(resource_version)
            session.commit()
            return '0'

    @staticmethod
    @provide_session
    def checkpoint_resource_version(
        scheduler_job_id,
        resource_version,
        session: Session = None) -> None:
        """Update Kubernetes Resource Version in Airflow Metadata DB"""
        if resource_version:
            session.query(KubeResourceVersion) \
                .filter(KubeResourceVersion.scheduler_job_id == str(scheduler_job_id))  \
                .update({
                KubeResourceVersion.resource_version: resource_version
            })
            session.commit()

    @staticmethod
    @provide_session
    def reset_resource_version(scheduler_job_id, session: Session = None) -> str:
        """Reset Kubernetes Resource Version to 0 in Airflow Metadata DB"""
        session.query(KubeResourceVersion) \
            .filter(KubeResourceVersion.scheduler_job_id == str(scheduler_job_id)) \
            .update({
            KubeResourceVersion.resource_version: '0'
        })
        session.commit()
        return '0'


# class KubeWorkerIdentifier(Base):
#     """Table containing Kubernetes Worker Identified"""
#     __tablename__ = "kube_worker_uuid"
#     scheduler_job_id = Column(String(255))
#     worker_uuid = Column(String(255))
#
#     @staticmethod
#     @provide_session
#     def get_or_create_current_kube_worker_uuid(session: Session = None, job_id) -> str:
#         """Create & Store Worker UUID in DB if it doesn't exists in DB, retrieve otherwise"""
#         rows = session.query(KubeWorkerIdentifier.worker_uuid).filter(
#             KubeWorkerIdentifier.scheduler_job_id == job_id,
#         ).all()
#         if len(rows) == 0:
#             while True:
#                 worker_uuid = str(uuid.uuid4())
#                 KubeWorkerIdentifier.checkpoint_kube_worker_uuid(worker_uuid, session)
#
#         if worker_uuid == '':
#             worker_uuid = str(uuid.uuid4())
#         return worker_uuid
#
#     @staticmethod
#     @provide_session
#     def checkpoint_kube_worker_uuid(worker_uuid: str, session: Session = None) -> None:
#         """Update the Kubernetes Worker UUID in the DB"""
#         if worker_uuid:
#             session.query(KubeWorkerIdentifier).update({
#                 KubeWorkerIdentifier.worker_uuid: worker_uuid
#             })
#             session.commit()
