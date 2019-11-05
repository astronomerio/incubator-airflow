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
#

from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.models import taskinstance
from airflow.utils.state import State
from tests.models import DEFAULT_DATE
from airflow.worker import task_runner_worker
import unittest


class TestTaskRunnerWorker(unittest.TestCase):

    async def get_application(self):
        """
        Override the get_app method to return your application.
        """

        app = await task_runner_worker.create_app()
        return app

    # @unittest_run_loop
    # async def test_hello(self):
    #     resp = await self.client.request("GET", "health?name=daniel")
    #     dag_id = 'test_requeue_over_dag_concurrency'
    #     task_id = 'test_requeue_over_dag_concurrency_op'
    #     dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE,
    #               max_active_runs=1, concurrency=2)
    #     task = DummyOperator(task_id=task_id, dag=dag)
    #     import datetime
    #     start_date = datetime.datetime(year=2019, day=1, month=1)
    #     ti = TI(task=task, execution_date=start_date, state=State.RUNNING)
    #
    #     task_runner_worker.running_tasks_map["test_requeue_over_dag_concurrency_op"] = ti
    #     await asyncio.sleep(5)
    #     text = await resp.text()
    #     # import time
    #     # time.sleep(60)
    #     print(text)
    #     task_runner_worker.running_tasks_map.pop("test_requeue_over_dag_concurrency_op")
    #     ti.set_state(State.SUCCESS)

    def tearDown(self):
        task_runner_worker._heartbeat.join()
        task_runner_worker._heartbeat.terminate()

    @unittest.expectedFailure
    def test_heartbeat_function(self):
        dag_id = 'test_heartbeat_func'
        task_id = 'test_heartbeat_func_op'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE,
                  max_active_runs=1, concurrency=2)
        task = DummyOperator(task_id=task_id, dag=dag)
        import datetime
        from airflow.utils.dag_processing import SimpleTaskInstance
        start_date = datetime.datetime(year=2019, day=1, month=1)
        ti = TI(task=task, execution_date=start_date, state=State.RUNNING)

        with create_session() as session:
            session.query(TI).filter(
                TI.dag_id == dag_id,
                TI.task_id == task_id,
            ).delete()
            task_runner_worker.create_heartbeat_process(session)
            session.query(TI).delete()
            session.commit()
            stale = taskinstance.get_stale_running_task_instances(session, stale_tolerance=2)
            self.assertEqual(stale, [])
            stale_time = timezone.utcnow() - datetime.timedelta(seconds=20)
            ti.heartbeat(session=session, time=stale_time)
            stale = taskinstance.get_stale_running_task_instances(session, stale_tolerance=2)
            self.assertNotEqual(stale, [])
            heartbeat1 = stale[0].last_heartbeat
            task_runner_worker.insert_into_heartbeat("test_heartbeat_func_op", SimpleTaskInstance(ti))
            import time
            time.sleep(3)
            task_runner_worker._heartbeat.terminate()
            stale = taskinstance.get_stale_running_task_instances(session, stale_tolerance=15)
            if stale:
                from datetime import timedelta
                heartbeat2 = stale[0].last_heartbeat
                self.assertNotEqual(heartbeat1, heartbeat2)
                stale_time = timezone.utcnow() - timedelta(seconds=15)
                self.assertEqual(stale, [], "there's something wrong with the heartbeating.\n"
                                            "Last heartbeat: {}\n"
                                            "current heartbeat: {}\n"
                                            "current time: {}\n"
                                            "stale time: {}".format(heartbeat1, heartbeat2, timezone.utcnow(),

                                                                    stale_time))

            self.assertEqual(stale, [])
