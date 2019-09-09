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

import json
import time
import tenacity
from typing import Tuple, Optional

from airflow.settings import pod_mutation_hook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from datetime import datetime as dt
from airflow.kubernetes.pod import Pod
from airflow.kubernetes.kubernetes_request_factory import pod_request_factory as pod_factory
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from airflow import AirflowException
from requests.exceptions import BaseHTTPError
from .kube_client import get_kube_client

# Used to calculate semantic versioning
try:
    from packaging.version import parse as semantic_version
except ImportError:
    # Python 2
    from distutils.version import LooseVersion as semantic_version

class PodStatus:
    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'

class SidecarNames:
    ISTIO_PROXY = 'istio-proxy'

class SleepConfig:
    # Only polls during the start of a pod
    POD_STARTING_POLL = 1
    # Used to detect all cleanup jobs are completed
    # and the entire Pod is cleaned up
    POD_RUNNING_POLL = 1
    # Polls for the duration of the task execution
    # to detect when the task is done. The difference
    # between this and POD_RUNNING_POLL is sidecars.
    BASE_CONTAINER_RUNNING_POLL = 2

class PodLauncher(LoggingMixin):
    def __init__(self, kube_client=None, in_cluster=True, cluster_context=None,
                 extract_xcom=False):
        super().__init__()
        self._client = kube_client or get_kube_client(in_cluster=in_cluster,
                                                      cluster_context=cluster_context)
        self._watch = watch.Watch()
        self.extract_xcom = extract_xcom
        self.kube_req_factory = pod_factory.ExtractXcomPodRequestFactory(
        ) if extract_xcom else pod_factory.SimplePodRequestFactory()

    def run_pod_async(self, pod, **kwargs):
        pod_mutation_hook(pod)

        req = self.kube_req_factory.create(pod)
        self.log.debug('Pod Creation Request: \n%s', json.dumps(req, indent=2))
        try:
            resp = self._client.create_namespaced_pod(body=req, namespace=pod.namespace, **kwargs)
            self.log.debug('Pod Creation Response: %s', resp)
        except ApiException:
            self.log.exception('Exception when attempting to create Namespaced Pod.')
            raise
        return resp

    def delete_pod(self, pod):
        try:
            self._client.delete_namespaced_pod(
                pod.name, pod.namespace, body=client.V1DeleteOptions())
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def run_pod(
            self,
            pod: Pod,
            startup_timeout: int = 120,
            get_logs: bool = True) -> Tuple[State, Optional[str]]:
        """
        Launches the pod synchronously and waits for completion.

        :param pod:
        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :param get_logs:  whether to query k8s for logs
        :return:
        """
        resp = self.run_pod_async(pod)
        curr_time = dt.now()
        if resp.status.start_time is None:
            while self.pod_not_started(pod):
                delta = dt.now() - curr_time
                if delta.seconds >= startup_timeout:
                    raise AirflowException("Pod took too long to start")
                time.sleep(SleepConfig.POD_STARTING_POLL)
            self.log.debug('Pod not yet started')

        return self._monitor_pod(pod, get_logs)

    def _monitor_pod(self, pod: Pod, get_logs: bool) -> Tuple[State, Optional[str]]:
        if get_logs:
            logs = self.read_pod_logs(pod)
            for line in logs:
                self.log.info(line)
        result = None
        while self.base_container_is_running(pod):
            self.log.info('Container %s has state %s', pod.name, State.RUNNING)
            time.sleep(SleepConfig.BASE_CONTAINER_RUNNING_POLL)
        if self.extract_xcom:
            result = self._extract_xcom(pod)
            self.log.info(result)
            result = json.loads(result)
        self._handle_istio_proxy(pod)
        while self.pod_is_running(pod):
            self.log.info('Pod %s has state %s', pod.name, State.RUNNING)
            time.sleep(SleepConfig.POD_RUNNING_POLL)
        return self._task_status(self.read_pod(pod)), result

    def _task_status(self, event):
        self.log.info(
            'Event: %s had an event of type %s',
            event.metadata.name, event.status.phase)
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod):
        state = self._task_status(self.read_pod(pod))
        return state == State.QUEUED

    def pod_is_running(self, pod):
        state = self._task_status(self.read_pod(pod))
        return state != State.SUCCESS and state != State.FAILED

    def base_container_is_running(self, pod):
        event = self.read_pod(pod)
        status = next(iter(filter(lambda s: s.name == 'base',
                                  event.status.container_statuses)), None)
        return status.state.running is not None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod_logs(self, pod):

        try:
            return self._client.read_namespaced_pod_log(
                name=pod.name,
                namespace=pod.namespace,
                container='base',
                follow=True,
                tail_lines=10,
                _preload_content=False
            )
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod(self, pod):
        try:
            return self._client.read_namespaced_pod(pod.name, pod.namespace)
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    def _handle_istio_proxy(self, pod):
        """If an istio-proxy sidecar is detected, attempt to cleanly shutdown.
        If we detect a version of Istio before it's compatible with Kubernetes
        Jobs, then raise an informative error message.

        Args:
            pod (V1Pod): The pod which we are checking for the sidecar

        Returns:
            (bool): True if we detect and exit istio-proxy,
                    False if we do not detect istio-proxy

        Raises:
            AirflowException: if we find an istio-proxy, and we can't shut it down.
        """
        # describe the pod
        pod = self.read_pod(pod)
        for container in pod.spec.containers:
            if container.name != SidecarNames.ISTIO_PROXY:
                continue
            # Check if supported version of istio-proxy.
            # If we can't tell the version, proceed anyways.
            if ":" in container.image:
                _, tag = container.image.split(":")
                if semantic_version(tag) < semantic_version("1.3.0-rc.0"):
                    raise AirflowException(
                        'Please use istio version 1.3.0+ for KubernetesExecutor compatibility.' +\
                        ' Detected version {}'.format(tag))
            #  exec into the container
            resp = kubernetes_stream(self._client.connect_get_namespaced_pod_exec,
                                     pod.name, pod.namespace,
                                     container=SidecarNames.ISTIO_PROXY,
                                     command=['/bin/sh'], stdin=True, stdout=True,
                                     stderr=True, tty=False,
                                     _preload_content=False)
            # cleanly quit using the self-shutdown endpoint
            # /quitquitquit is a sidecar convention introduced by Envoy
            try:
                self._exec_pod_command(resp, 'curl http://127.0.0.1:15020/quitquitquit')
            finally:
                resp.close()
            return True
        return False

    def _extract_xcom(self, pod):
        resp = kubernetes_stream(self._client.connect_get_namespaced_pod_exec,
                                 pod.name, pod.namespace,
                                 container=self.kube_req_factory.SIDECAR_CONTAINER_NAME,
                                 command=['/bin/sh'], stdin=True, stdout=True,
                                 stderr=True, tty=False,
                                 _preload_content=False)
        try:
            result = self._exec_pod_command(
                resp, 'cat {}/return.json'.format(self.kube_req_factory.XCOM_MOUNT_PATH))
            self._exec_pod_command(resp, 'kill -s SIGINT 1')
        finally:
            resp.close()
        if result is None:
            raise AirflowException('Failed to extract xcom from pod: {}'.format(pod.name))
        return result

    def _exec_pod_command(self, resp, command):
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    return resp.read_stdout()
                if resp.peek_stderr():
                    self.log.info(resp.read_stderr())
                    break

    def process_status(self, job_id, status):
        status = status.lower()
        if status == PodStatus.PENDING:
            return State.QUEUED
        elif status == PodStatus.FAILED:
            self.log.info('Event with job id %s Failed', job_id)
            return State.FAILED
        elif status == PodStatus.SUCCEEDED:
            self.log.info('Event with job id %s Succeeded', job_id)
            return State.SUCCESS
        elif status == PodStatus.RUNNING:
            return State.RUNNING
        else:
            self.log.info('Event: Invalid state %s on job %s', status, job_id)
            return State.FAILED
