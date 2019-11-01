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
FROM ubuntu:18.04

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python3-dev \
        python3-pip \
        python3.6 \
        libczmq-dev \
        libcurlpp-dev \
        curl \
        libssl-dev \
        git \
        libpq-dev \
        inetutils-telnet \
        bind9utils \
        zip \
        unzip \
    && apt-get clean

RUN pip3 install --upgrade pip

# Since we install vanilla Airflow, we also want to have support for Postgres and Kubernetes
RUN pip3 install -U setuptools && \
    pip3 install kubernetes && \
    pip3 install cryptography && \
    pip3 install psycopg2-binary==2.7.4  # I had issues with older versions of psycopg2, just a warning

# install airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt
COPY airflow.tar.gz /tmp/airflow.tar.gz
RUN pip3 install --no-use-pep517 /tmp/airflow.tar.gz

COPY airflow-test-env-init.sh /tmp/airflow-test-env-init.sh

COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
