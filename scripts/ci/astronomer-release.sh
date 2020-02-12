#!/usr/bin/env bash

# This file is Not licensed to ASF
# SKIP LICENSE INSERTION

set -euo pipefail
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHON_VERSION=3.6

# shellcheck source=scripts/ci/_utils.sh
. "${MY_DIR}/_utils.sh"

cd "${AIRFLOW_SOURCES}"

basic_sanity_checks

script_start

python3 setup.py verify compile_assets sdist bdist_wheel

ls -altr dist/*

script_end
