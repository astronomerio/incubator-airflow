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

import os

import pytest

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.state import State
from tests.test_utils import db

TEST_DAG_FOLDER = os.environ['AIRFLOW__CORE__DAGS_FOLDER']


@pytest.fixture()
def clear_db():
    def _clean_up():
        db.clear_db_dags()
        db.clear_db_jobs()
        db.clear_db_runs()
        db.clear_db_task_fail()

    _clean_up()
    yield


@pytest.fixture
def dag_maker(request):

    DEFAULT_DATE = timezone.datetime(2016, 1, 1)

    def create_dag_ti_dr(
        dag_id='test_dag', task_id='op1', python_callable=None, py_kwargs=None, with_dagbag=False, **kwargs
    ):
        # If python_callable we create a PythonOperator task
        # For ease of use, set start date to "DEFAULT_DATE" module variable from request
        if "start_date" not in kwargs and hasattr(request.module, 'DEFAULT_DATE'):
            kwargs['start_date'] = getattr(request.module, 'DEFAULT_DATE')
        else:
            kwargs['start_date'] = DEFAULT_DATE
        if with_dagbag:
            dagbag = DagBag(
                dag_folder=TEST_DAG_FOLDER,
                include_examples=False,
            )
            dag = dagbag.get_dag(dag_id)
            task = dag.get_task(task_id)
        else:
            dag = DAG(dag_id, start_date=kwargs['start_date'])
            if python_callable:
                task = PythonOperator(task_id=task_id, python_callable=python_callable, dag=dag, **py_kwargs)
            else:
                task = DummyOperator(task_id=task_id, run_as_user=kwargs.get("run_as_user", None), dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_id="test",
            state=kwargs.get('state', State.SUCCESS),
            execution_date=kwargs.get('execution_date', kwargs['start_date']),
            start_date=kwargs['start_date'],
        )
        yield dag
        yield task
        yield dr

    return create_dag_ti_dr
