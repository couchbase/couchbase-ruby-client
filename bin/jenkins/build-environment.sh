#!/usr/bin/env bash
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
        source ${PROJECT_ROOT}/bin/jenkins/build-environment-for-amazon.sh
        ;;

    centos)
        source ${PROJECT_ROOT}/bin/jenkins/build-environment-for-centos.sh
        ;;

    alpine)
        source ${PROJECT_ROOT}/bin/jenkins/build-environment-for-alpine.sh
        ;;

    ubuntu|pop)
        source ${PROJECT_ROOT}/bin/jenkins/build-environment-for-ubuntu.sh
        ;;

    macos)
        source ${PROJECT_ROOT}/bin/jenkins/build-environment-for-macos.sh
        ;;

    *)
esac
