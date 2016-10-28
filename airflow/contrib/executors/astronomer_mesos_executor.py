"""Astronomer Airflow executor for Mesos"""
#!/usr/bin/env python

from future import standard_library
standard_library.install_aliases()
import os
from threading import Lock

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

DEFAULT_TASK_CPU = 1
DEFAULT_TASK_MEMORY = 256

# Tasks are reconciliated using an exponential backoff algorithm.
# A task is marked as old if it has not been updated since 2**backoff_iteration seconds

# The backoff level a task should have when switching from clean to dirty
# This applies to not-yet-running tasks that have not been updated for a while
BACKOFF_ITERATION_AFTER_DIRTY = 2
# The backoff level a task should have when switching from dirty to clean
BACKOFF_ITERATION_AFTER_CLEAN = 5
# The maximum backoff iteration level. Tasks that are still dirty get marked for termination here
BACKOFF_ITERATION_MAX = 8
# The initial backoff iteration level for fresh tasks
BACKOFF_ITERATION_INITIAL = 1


def copy_env_var(command, env_var_name):
    """Copies an environment variable to a Mesos Command"""
    env_var = command.environment.variables.add()
    env_var.name = env_var_name
    env_var.value = os.getenv(env_var_name, '')

class Task(object):
    """Encapsulates data about a specific Mesos task"""
    _ids = count(0)

    def __init__(self, **kwargs):
        self.state = kwargs.get("state") or mesos_pb2.TASK_STAGING
        self.created = kwargs.get("created") or datetime.utcnow()
        self.updated = kwargs.get("updated") or datetime.utcnow()
        self.key = kwargs.get("key") or None
        self.cmd = kwargs.get("cmd") or ""
        self.task_id = str(self._ids.next())
        self.agent_id = kwargs.get("agent_id") or None
        self.cpus = configuration.getint('mesos', 'TASK_CPU') or DEFAULT_TASK_CPU
        self.mem = configuration.getint('mesos', 'TASK_MEMORY') or DEFAULT_TASK_MEMORY
        self.clean = True
        self.backoff_iteration = BACKOFF_ITERATION_INITIAL

    def __repr__(self):
        return "Task({} {})".format(self.task_id, self.key)

    def __str__(self):
        return self.__repr__()

    def as_mesos_task(self, agent_id):
        """Returns a Mesos Task representation of the task"""
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
        """Is the current task in a failed state?"""
        return self.state in (mesos_pb2.TASK_LOST,
                              mesos_pb2.TASK_ERROR,
                              mesos_pb2.TASK_KILLED,
                              mesos_pb2.TASK_FAILED)

    def finished(self):
        """Has the current task succesfully finished?"""
        return self.state == mesos_pb2.TASK_FINISHED

    def running(self):
        """Is the task in a running state?"""
        return self.state in (mesos_pb2.TASK_STAGING,
                              mesos_pb2.TASK_STARTING,
                              mesos_pb2.TASK_RUNNING)

    def old(self):
        """Returns True if the task's last update is older than the current backoff timeout."""
        return not self.state == mesos_pb2.TASK_RUNNING and ((datetime.utcnow() - self.updated).total_seconds() > 2**self.backoff_iteration)

    def update(self, update):
        """Update the task's status"""
        _log.info("Updating status of %s from %s to %s",
                  self,
                  mesos_pb2.TaskState.Name(self.state),
                  mesos_pb2.TaskState.Name(update.state))
        self.state = update.state
        self.touch()
        self.make_clean()

    def touch(self):
        """Set the task's last updated time to now"""
        self.updated = datetime.utcnow()

    def make_dirty(self):
        """Make the task dirty and subject to reconciliation"""
        if self.clean:
            self.backoff_iteration = BACKOFF_ITERATION_AFTER_DIRTY
        else:
            self.backoff_iteration += 1

        if self.backoff_iteration > BACKOFF_ITERATION_MAX:
            self.task_id = None

        self.clean = False

    def make_clean(self):
        """Mark the task as clean"""
        self.clean = True
        self.backoff_iteration = BACKOFF_ITERATION_AFTER_CLEAN

    def get_task_status(self):
        """Returns a Mesos TaskStatus representation of the task"""
        task_status = mesos_pb2.TaskStatus()
        task_status.slave_id.value = self.agent_id
        task_status.task_id.value = self.task_id
        task_status.state = self.state
        return task_status

class AirflowMesosScheduler(mesos.interface.Scheduler):
    """Astronomer Mesos Scheduler for the Airflow executor"""
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
        """Returns a task with a specific task id"""
        return self.tasks.get(taskId)

    def makeOldTasksDirty(self, driver):
        """Mark all stale tasks as subject for reconciliation"""
        for task in self.tasks.itervalues():
            if task.old():
                task.make_dirty()
        self.triggerResync(driver)

    def makeTasksDirty(self, driver, agentId=None):
        """Mark tasks as subject for reconciliation - optionally specify an agent id"""
        for task in self.tasks.itervalues():
            if task.running() and (not agentId or task.agent_id == agentId):
                task.make_dirty()
        self.triggerResync(driver)

    def triggerResync(self, driver):
        """Trigger a reconciliation for dirty tasks"""
        unclean_task_statuses = [task.get_task_status() for task in self.tasks.itervalues() if not task.clean]

        if unclean_task_statuses:
            _log.info("Triggering explicit reconciliation.")
            self.syncd = False
            driver.reconcileTasks(unclean_task_statuses)

    def registered(self, driver, frameworkId, masterInfo):
        """Called when the Mesos framework is registered"""
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
        """Called when the Mesos framework is reregistered"""
        _log.info("AirflowScheduler re-registered to mesos")
        # Trigger an all-out reconciliation
        self.makeTasksDirty(driver)

    def disconnected(self, driver):
        """Called when the Mesos framework is disconnected from Mesos"""
        # TODO Should we just abort here, if no checkpointing?
        _log.info("AirflowScheduler disconnected from mesos")

    def offerRescinded(self, driver, offerId):
        """Called when a previous resource offer gets called back"""
        _log.info("AirflowScheduler offer %s rescinded", str(offerId))
        # Reconciliate all tasks, just in case we accepted the offer and we miss TASK_LOST
        self.makeTasksDirty(driver)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """Called when a framework message is received"""
        _log.info("AirflowScheduler received framework message %s", message)

    def executorLost(self, driver, executorId, slaveId, status):
        """Called when the Mesos executor is lost"""
        # TODO Not sure how to best handle this.
        _log.warning("AirflowScheduler executor %s lost", str(executorId))

    def slaveLost(self, driver, slaveId):
        """Called when a slave is disconnected from Mesos"""
        _log.warning("AirflowScheduler slave %s lost", str(slaveId))
        # Mark all tasks running on the slave as dirty
        self.makeTasksDirty(driver, slaveId)

    def error(self, driver, message):
        """Called by Mesos on a framework error"""
        _log.error("AirflowScheduler driver aborted %s", message)
        raise AirflowException("AirflowScheduler driver aborted %s" % message)
        #os._exit(1)

    def resourceOffers(self, driver, offers):
        """Called by Mesos with resource offers"""
        # If there's neved been a reconciliation done, trigger one
        if self.syncd is None:
            _log.debug("Scheduler is not yet sync'd - triggering a resync")
            self.makeTasksDirty(driver)

        # Scan for stale tasks, and trigger a reconciliation if needed
        self.makeOldTasksDirty(driver)

        for offer in offers:
            # Gather total offer resources
            offer_cpus = 0
            offer_mem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offer_cpus += resource.scalar.value
                elif resource.name == "mem":
                    offer_mem += resource.scalar.value

            _log.info("Received offer from %s with cpus: %s and mem: %s", offer.slave_id.value[-8:], offer_cpus, offer_mem)

            tasks = []
            i = 0

            # Aquire lock for reading the task queue
            self.task_mutex.acquire()
            try:
                while offer_cpus > 0 and offer_mem > 0 and i < len(self.task_queue):
                    task = self.task_queue[i]
                    if task.cpus <= offer_cpus and task.mem <= offer_mem:
                        self.task_queue.pop(i)
                        task.agent_id = offer.slave_id.value
                        tasks.append(task)
                        self.tasks[task.task_id] = task
                        offer_cpus -= task.cpus
                        offer_mem -= task.mem
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
                # Set the task update date to the moment of launch
                task.touch()
                mesos_task = task.as_mesos_task(offer.slave_id.value)
                operation = mesos_pb2.Offer.Operation()
                operation.launch.task_infos.extend([mesos_task])
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operations.append(operation)

            driver.acceptOffers([offer.id], operations)

    def statusUpdate(self, driver, update):
        """Called by Mesos with task status updates"""
        # Retrieve task information
        task = self._task(update.task_id.value)

        # Is the task unknown?
        if not task or task.task_id != update.task_id.value:
            if update.state == mesos_pb2.TASK_RUNNING:
                # Kill of running, unrecognized tasks
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

        # Is the task finished or failed?
        if not task.running():
            # Aquire a lock for writing to the result queue
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

            # Remove ended tasks from the current state
            del self.tasks[task.task_id]

        # If reconciliating, and all tasks are clean, we're done
        if not self.syncd and all(task.clean for task in self.tasks.values()):
            _log.debug("Agent is now sync'd - perform full reconcile to tidy up old tasks")
            self.syncd = True
            # Trigger a final implicit reconciliation, to fix potential lost tasks we don't know
            driver.reconcileTasks([])

class AstronomerMesosExecutor(BaseExecutor):
    """Airflow executor that schedules tasks on Mesos"""
    def start(self):
        """Called on executor launch"""
        # Make sure we have all needed configuration
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

        # FIFO task list
        self.task_queue = []
        # Lock for the task list
        self.task_mutex = Lock()
        # List for task results
        self.result_queue = []
        # Lock for the results list
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
        """Called by Airflow to schedule tasks for execution"""
        # Aquire lock for writing to the task queue
        self.task_mutex.acquire()
        try:
            self.task_queue.append(Task(key=key, cmd=command))
        finally:
            self.task_mutex.release()

    def sync(self):
        """Called by Airflow to retrieve a batch of task results"""
        # Aquire lock for reading from the results queue
        self.result_mutex.acquire()
        try:
            while self.result_queue:
                results = self.result_queue.pop(0)
                self.change_state(*results)
        finally:
            self.result_mutex.release()

    def end(self):
        """Called by Airflow to end execution"""
        self.mesos_driver.stop()
