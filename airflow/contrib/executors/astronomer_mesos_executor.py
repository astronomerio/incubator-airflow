#!/usr/bin/env python

import os
from threading import Lock

from future import standard_library
standard_library.install_aliases()
from builtins import str
from datetime import datetime
from itertools import count
import logging

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.utils.state import State
from airflow.exceptions import AirflowException

_log = logging.getLogger(__name__)

DEFAULT_FRAMEWORK_NAME = 'Airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'

def copy_env_var(command, env_var_name):
    env_var = command.environment.variables.add()
    env_var.name = env_var_name
    env_var.value = os.getenv(env_var_name, '')

class Task(object):
    _ids = count(0)

    def __init__(self, **kwargs):
        self.state = kwargs.get("state") or mesos_pb2.TASK_STAGING
        self.created = kwargs.get("created") or str(datetime.utcnow())
        self.updated = kwargs.get("updated") or "NA"
        self.key = kwargs.get("key") or None
        self.cmd = kwargs.get("cmd") or ""
        self.task_id = str(self._ids.next())
        self.agent_id = kwargs.get("agent_id") or None
        
        self.cpus = configuration.getint('mesos', 'TASK_CPU') or 1
        self.mem = configuration.getint('mesos', 'TASK_MEMORY') or 256
        self.clean = True

    def __repr__(self):
        return "Task({} {})".format(self.task_id, self.key)

    def __str__(self):
        return self.__repr__()

    def as_mesos_task(self, agent_id):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = self.task_id
        task.slave_id.value = agent_id
        task.name = "AirflowTask {} {}".format(self.task_id, self.key)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem

        container = mesos_pb2.ContainerInfo()
        container.type = 1 # mesos_pb2.ContainerInfo.Type.DOCKER
        volume = container.volumes.add()
        volume.host_path = "/airflow/logs"
        volume.container_path = "/airflow/logs"
        volume.mode = 1 # mesos_pb2.Volume.Mode.RW

        volume = container.volumes.add()
        volume.host_path = "/var/run/docker.sock"
        volume.container_path = "/var/run/docker.sock"
        volume.mode = 1 # mesos_pb2.Volume.Mode.RW

        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.image = "astronomerio/airflow"
        docker.force_pull_image = True

        container.docker.MergeFrom(docker)
        task.container.MergeFrom(container)

        command = mesos_pb2.CommandInfo()
        command.value = self.cmd

        # Copy some environemnt vars from scheduler to execution docker container
        copy_env_var(command, "AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        copy_env_var(command, "AWS_ACCESS_KEY_ID")
        copy_env_var(command, "AWS_SECRET_ACCESS_KEY")

        task.command.MergeFrom(command)

        return task

    def failed(self):
        return self.state in (mesos_pb2.TASK_LOST,
                              mesos_pb2.TASK_ERROR,
                              mesos_pb2.TASK_KILLED,
                              mesos_pb2.TASK_FAILED)

    def finished(self):
        return self.state == mesos_pb2.TASK_FINISHED

    def running(self):
        return self.state in (mesos_pb2.TASK_STAGING,
                              mesos_pb2.TASK_STARTING,
                              mesos_pb2.TASK_RUNNING)

    def update(self, update):
        _log.info("Updating status of %s from %s to %s",
                  self,
                  mesos_pb2.TaskState.Name(self.state),
                  mesos_pb2.TaskState.Name(update.state))
        self.updated = str(datetime.utcnow())
        self.state = update.state
        self.clean = True

    def get_task_status(self):
        task_status = mesos_pb2.TaskStatus()
        task_status.slave_id.value = self.agent_id
        task_status.task_id.value = self.task_id
        task_status.state = self.state
        return task_status

    def to_dict(self):
        task_dict = {
            "task_id": self.task_id,
            "state": self.state,
            "created": self.created,
            "updated": self.updated
        }
        return task_dict

    @classmethod
    def from_dict(cls, task_dict):
        task = cls(**task_dict)
        task.clean = not task.running()
        return task

class AirflowMesosScheduler(mesos.interface.Scheduler):
    def __init__(self, task_queue, task_mutex, result_queue, result_mutex):
        self.task_queue = task_queue
        self.task_mutex = task_mutex
        self.result_queue = result_queue
        self.result_mutex = result_mutex
        self.syncd = None
        self.tasks = {}

    def __repr__(self):
        return "AirflowMesosScheduler"

    def _task(self, taskId):
        return self.tasks.get(taskId)

    def triggerResync(self, driver, agentId=None):
        for task in self.tasks.itervalues():
                if task.running() and (not agentId or task.agent_id == agentId):
                    task.clean = False

        unclean_task_statuses = [task.get_task_status() for task in self.tasks.itervalues() if not task.clean]

        if not unclean_task_statuses:
            _log.info("No running tasks, assume sync'd. Triggering implicit reconciliation.")
            self.syncd = True
            driver.reconcileTasks([])
        else:
            _log.info("Triggering explicit reconciliation.")
            self.syncd = False
            driver.reconcileTasks(unclean_task_statuses)

    def registered(self, driver, frameworkId, masterInfo):
        _log.info("AirflowMesosScheduler registered to mesos with framework ID %s", frameworkId.value)

        if configuration.getboolean('mesos', 'CHECKPOINT') and configuration.get('mesos', 'FAILOVER_TIMEOUT'):
            # Import here to work around a circular import error
            from airflow.models import Connection

            # Update the Framework ID in the database.
            session = Session()
            conn_id = FRAMEWORK_CONNID_PREFIX + DEFAULT_FRAMEWORK_NAME
            connection = Session.query(Connection).filter_by(conn_id=conn_id).first()
            if connection is None:
                connection = Connection(conn_id=conn_id, conn_type='mesos_framework-id',
                                        extra=frameworkId.value)
            else:
                connection.extra = frameworkId.value

            session.add(connection)
            session.commit()
            Session.remove()

    def reregistered(self, driver, masterInfo):
        _log.info("AirflowScheduler re-registered to mesos")
        self.triggerResync(driver)

    def disconnected(self, driver):
        _log.info("AirflowScheduler disconnected from mesos")

    def offerRescinded(self, driver, offerId):
        _log.info("AirflowScheduler offer %s rescinded", str(offerId))

    def frameworkMessage(self, driver, executorId, slaveId, message):
        _log.info("AirflowScheduler received framework message %s", message)

    def executorLost(self, driver, executorId, slaveId, status):
        _log.warning("AirflowScheduler executor %s lost", str(executorId))

    def slaveLost(self, driver, slaveId):
        _log.warning("AirflowScheduler slave %s lost", str(slaveId))
        self.triggerResync(driver, slaveId)

    def error(self, driver, message):
        _log.error("AirflowScheduler driver aborted %s", message)
        raise AirflowException("AirflowScheduler driver aborted %s" % message)
        #os._exit(1)

    def resourceOffers(self, driver, offers):
        if self.syncd is None:
            _log.debug("Scheduler is not yet sync'd - triggering a resync")
            self.triggerResync(driver)

        if not self.syncd:
            _log.debug("Scheduler has not yet sync'd")
            for offer in offers:
                driver.declineOffer(offer.id)
            return

        for offer in offers:
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            _log.info("Received offer from %s with cpus: %s and mem: %s", offer.slave_id.value[-8:], offerCpus, offerMem)

            tasks = []
            i = 0

            self.task_mutex.acquire()
            try:
                while offerCpus > 0 and offerMem > 0 and i < len(self.task_queue):
                    task = self.task_queue[i]
                    if task.cpus <= offerCpus and task.mem <= offerMem:
                        self.task_queue.pop(i)
                        task.agent_id = offer.slave_id.value;
                        tasks.append(task)
                        self.tasks[task.task_id] = task
                        offerCpus -= task.cpus
                        offerMem -= task.mem
                    i += 1
            finally:
                self.task_mutex.release()

            if not tasks:
                _log.debug("Declining offer - no tasks to schedule")
                driver.declineOffer(offer.id)
                continue

            _log.info("Launching Tasks %s", tasks)
            
            operations = []
            for task in tasks:
                mesos_task = task.as_mesos_task(offer.slave_id.value)
                operation = mesos_pb2.Offer.Operation()
                operation.launch.task_infos.extend([mesos_task])
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operations.append(operation)
            
            driver.acceptOffers([offer.id], operations)

    def statusUpdate(self, driver, update):
        task = self._task(update.task_id.value)
        if not task or task.task_id != update.task_id.value:
            if update.state == mesos_pb2.TASK_RUNNING:
                _log.info("Killing unrecognized task: %s", update.task_id.value)
                driver.killTask(update.task_id)
            else:
                _log.info("Ignoring update from unrecognized stopped task.")
            return

        _log.debug("TASK_UPDATE - %s: %s on Slave %s",
                   mesos_pb2.TaskState.Name(update.state),
                   task,
                   update.slave_id.value[-7:])

        task.update(update)

        if not task.running():
            self.result_mutex.acquire()
            try:
                if task.failed():
                    _log.error("\t%s is in failed state %s with message '%s'",
                                task, mesos_pb2.TaskState.Name(update.state), update.message)
                    _log.error("\tData:  %s", repr(str(update.data)))
                    _log.error("\tSent by: %s", mesos_pb2.TaskStatus.Source.Name(update.source))
                    _log.error("\tReason: %s", mesos_pb2.TaskStatus.Reason.Name(update.reason))
                    _log.error("\tMessage: %s", update.message)
                    _log.error("\tHealthy: %s", update.healthy)
                    self.result_queue.append((task.key, State.FAILED))
                else:
                    _log.debug("%s ended successfully with message '%s'", task, update.message)
                    self.result_queue.append((task.key, State.SUCCESS))
            finally:
                self.result_mutex.release()

            del self.tasks[task.task_id]

        if not self.syncd and all(task.clean for task in self.tasks.values()):
            _log.debug("Agent is now sync'd - perform full reconcile to tidy up old tasks")
            self.syncd = True
            driver.reconcileTasks([])

class AstronomerMesosExecutor(BaseExecutor):

    def start(self):
        if not configuration.get('mesos', 'MASTER'):
            logging.error("Expecting mesos master URL for mesos executor")
            raise AirflowException("mesos.master not provided for mesos executor")
        if configuration.getboolean('mesos', 'AUTHENTICATE'):
            if not configuration.get('mesos', 'DEFAULT_PRINCIPAL'):
                logging.error("Expecting authentication principal in the environment")
                raise AirflowException("mesos.default_principal not provided in authenticated mode")
            if not configuration.get('mesos', 'DEFAULT_SECRET'):
                logging.error("Expecting authentication secret in the environment")
                raise AirflowException("mesos.default_secret not provided in authenticated mode")

        self.task_queue = []
        self.task_mutex = Lock()
        self.result_queue = []
        self.result_mutex = Lock()

        framework = mesos_pb2.FrameworkInfo()
        framework.user = ''
        framework.name = DEFAULT_FRAMEWORK_NAME
        implicit_acknowledgements = 1
        master = configuration.get('mesos', 'MASTER')

        if configuration.getboolean('mesos', 'CHECKPOINT'):
            framework.checkpoint = True

            if configuration.get('mesos', 'FAILOVER_TIMEOUT'):
                # Import here to work around a circular import error
                from airflow.models import Connection

                # Query the database to get the ID of the Mesos Framework, if available.
                conn_id = FRAMEWORK_CONNID_PREFIX + framework.name
                session = Session()
                connection = session.query(Connection).filter_by(conn_id=conn_id).first()
                if connection is not None:
                    # Set the Framework ID to let the scheduler reconnect with running tasks.
                    framework.id.value = connection.extra

                framework.failover_timeout = configuration.getint('mesos', 'FAILOVER_TIMEOUT')
        else:
            framework.checkpoint = False

        logging.info('MesosFramework master : %s, name : %s', master, framework.name)

        if configuration.getboolean('mesos', 'AUTHENTICATE'):
            credential = mesos_pb2.Credential()
            credential.principal = configuration.get('mesos', 'DEFAULT_PRINCIPAL')
            credential.secret = configuration.get('mesos', 'DEFAULT_SECRET')

            framework.principal = credential.principal

            driver = mesos.native.MesosSchedulerDriver(
                AirflowMesosScheduler(self.task_queue, self.task_mutex, self.result_queue, self.result_mutex),
                framework,
                master,
                implicit_acknowledgements,
                credential)
        else:
            framework.principal = 'Airflow'
            driver = mesos.native.MesosSchedulerDriver(
                AirflowMesosScheduler(self.task_queue, self.task_mutex, self.result_queue, self.result_mutex),
                framework,
                master,
                implicit_acknowledgements)

        self.mesos_driver = driver
        self.mesos_driver.start()

    def execute_async(self, key, command, queue=None):
        self.task_mutex.acquire()
        try:
            self.task_queue.append(Task(key=key, cmd=command))
        finally:
            self.task_mutex.release()

    def sync(self):
        self.result_mutex.acquire()
        try:
            while self.result_queue:
                results = self.result_queue.pop(0)
                self.change_state(*results)
        finally:
            self.result_mutex.release()

    def end(self):
        self.mesos_driver.stop()
