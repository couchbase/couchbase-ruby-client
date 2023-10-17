#!/usr/bin/env sh
# Copyright 2020-Present Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "HOSTNAME=${HOSTNAME}"
echo "NODE_NAME=${NODE_NAME}"
echo "CONTAINER_TAG=${CONTAINER_TAG}"
echo "JENKINS_SLAVE_LABELS=${JENKINS_SLAVE_LABELS}"
echo "NODE_LABELS=${NODE_LABELS}"

PROJECT_ROOT="$( cd "$(dirname "$0")/../.." >/dev/null 2>&1 ; pwd -P )"

set -x
set -e

if [ -f /etc/os-release ]
then
    OS_ID=$(grep -o '^ID=.*' /etc/os-release | sed 's/ID=\|"//g')
    OS_VERSION=$(grep -o '^VERSION_ID=.*' /etc/os-release | sed 's/VERSION_ID=\|"//g')
elif [ "$(uname -s)" = "Darwin" ]
then
    OS_ID="macos"
    OS_VERSION=$(uname -r)
fi

case ${OS_ID} in
    amzn)
        exec ${PROJECT_ROOT}/bin/jenkins/install-dependencies-for-amazon.sh
        ;;

    centos)
        exec ${PROJECT_ROOT}/bin/jenkins/install-dependencies-for-centos.sh
        ;;

    alpine)
        exec ${PROJECT_ROOT}/bin/jenkins/install-dependencies-for-alpine.sh
        ;;

    ubuntu|pop)
        exec ${PROJECT_ROOT}/bin/jenkins/install-dependencies-for-ubuntu.sh
        ;;

    macos)
        exec ${PROJECT_ROOT}/bin/jenkins/install-dependencies-for-macos.sh
        ;;

    *)
        echo "unknown system: OS_ID=${OS_ID}, OS_VERSION=${OS_VERSION}"
        exit 1
esac
