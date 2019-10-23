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

import asyncio
import logging
import os
from datetime import datetime
from typing import Any

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.models import (
    DagBag, TaskInstance
)
from airflow.utils.log.logging_mixin import (LoggingMixin)
from airflow.utils.net import get_hostname
from airflow.utils.state import State
from concurrent.futures import ProcessPoolExecutor

import pendulum
from contextlib import suppress

app = None  # type: Any
loop: asyncio.AbstractEventLoop = None
pool = None
executor = None
DAGS_FOLDER = settings.DAGS_FOLDER
from aiohttp import web

import asyncio
from aiohttp import web

running_tasks_map = {}

heartbeat_loop_task = None
num = 0


async def heartbeat():
    global running_tasks_map, num
    while True:
        num = num + 1
        for k, v in running_tasks_map.items():
            a = v
            await a.heartbeat()
        await asyncio.sleep(1)


async def health(request):
    name = request.rel_url.query.get("name", "Daniel")
    return web.Response(text="Hello, {}. There have been {} heartbeats.".format(name, num))


async def run_task(request):
    global running_tasks_map
    dag_id = request.rel_url.query['dag_id']
    task_id = request.rel_url.query['task_id']
    # subdir = request.rel_url.query['subdir']
    subdir = "/root/airflow/dags"
    response = None
    execution_date = pendulum.fromtimestamp(int(request.rel_url.query["execution_date"]))
    log = LoggingMixin().log
    log.info("running dag {} for task {} on date {} in subdir {}".format(dag_id, task_id, execution_date, subdir))
    logging.shutdown()
    key = (task_id, dag_id, execution_date).__str__()
    try:
        # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
        # behind multiple open sleeping connections while heartbeating, which could
        # easily exceed the database connection limit when
        # processing hundreds of simultaneous tasks.
        settings.configure_orm(disable_connection_pool=True)
        ti = get_task_instance(dag_id=dag_id,
                               task_id=task_id,
                               subdir=subdir,
                               execution_date=execution_date)
        out = run(ti)
        running_tasks_map[key] = ti
        run_task_instance(ti, log)


        if out.state == 'success':
            response = web.Response(
                body="successfully ran dag {} for task {} on date {}".format(dag_id, task_id, execution_date),
                status=200)
        else:
            response = web.Response(body="task failed", status=500)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        response = web.Response(body="failed {} {}".format(e, tb), status=500)
    finally:
        running_tasks_map.pop(key, None)
        return response


def process_subdir(subdir):
    if subdir:
        subdir = subdir.replace('DAGS_FOLDER', DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
        return subdir


def get_dag(dag_id: str, subdir: str) -> DAG:
    dagbag = DagBag(process_subdir(subdir))
    if dag_id not in dagbag.dags:
        raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(dag_id))
    return dagbag.dags[dag_id]


def get_task_instance(
    dag_id: str,
    task_id: str,
    subdir: str,
    execution_date: datetime,
):
    dag = get_dag(dag_id, subdir)

    task = dag.get_task(task_id=task_id)
    ti = TaskInstance(task, execution_date)
    return ti


def run(ti: TaskInstance):
    log = LoggingMixin().log

    # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.configure_orm(disable_connection_pool=True)

    run_task_instance(ti, log)
    logging.shutdown()
    return ti


def run_task_instance(ti: TaskInstance, log):
    ti.refresh_from_db()
    set_task_instance_to_running(ti)
    ti.init_run_context()
    hostname = get_hostname()
    log.info("Running %s on host %s", ti, hostname)
    ti._run_raw_task()


def set_task_instance_to_running(ti):
    ti.state = State.RUNNING
    session = settings.Session()
    session.merge(ti)
    session.commit()


async def on_shutdown(app):
    global heartbeat_loop_task
    heartbeat_loop_task.cancel()
    with suppress(asyncio.CancelledError):
        await heartbeat_loop_task  # await for task cancellation


async def create_app():
    global heartbeat_loop_task
    global loop, app, executor
    heartbeat_loop_task = asyncio.Task(heartbeat())

    loop = asyncio.get_event_loop()
    executor = ProcessPoolExecutor()
    loop.set_default_executor(executor)
    app = web.Application()
    app.add_routes([web.get('/health', health), web.get('/run', run_task)])
    return app
