#!/usr/bin/env bash

# This file is Not licensed to ASF
# SKIP LICENSE INSERTION

export PYTHON_VERSION=3.6

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

# If the build is not a tag/release, build a dev version
if [[ -z ${TRAVIS_TAG:=} ]]; then
    echo
    AIRFLOW_VERSION=$(awk '/version/{print $NF}' airflow/version.py | tr -d \')
    export AIRFLOW_VERSION
    echo "Current Version is: ${AIRFLOW_VERSION}"

    if [[ ${AIRFLOW_VERSION} == *"dev"* ]]; then
        echo "Building and releasing a DEV Package"
        echo
        git fetch --unshallow

        COMMITS_SINCE_LAST_TAG="$(git rev-list "$(git describe --abbrev=0 --tags)"..HEAD --count)"
        sed -i -E "s/dev[0-9]+/dev${COMMITS_SINCE_LAST_TAG}/g" airflow/version.py

        UPDATED_AIRFLOW_VERSION=$(awk '/version/{print $NF}' airflow/version.py | tr -d \')
        export UPDATED_AIRFLOW_VERSION
        echo "Updated Airflow Version: $UPDATED_AIRFLOW_VERSION"

        python3 setup.py --quiet verify compile_assets sdist bdist_wheel
    else
        echo "Version does not contain 'dev' in airflow/version.py"
        echo "Skipping build and release process"
        echo
        exit 1
    fi
elif [[ ${TRAVIS_TAG:=} == "${TRAVIS_BRANCH:=}" ]]; then
    python3 setup.py --quiet verify compile_assets sdist bdist_wheel
fi

ls -altr dist/*
