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

PROJECT_ROOT="$( cd "$(dirname "$0")/../.." >/dev/null 2>&1 ; pwd -P )"

set -x
set -e

[ "$(id -u)" -ne 0 ] && SUDO="sudo " || SUDO=""

if ! command -v ruby &> /dev/null
then
    ${SUDO} yum install -y ruby
fi

if [ ! -d /opt/rh/devtoolset-11/root ]
then
    ${SUDO} yum install -y epel-release centos-release-scl
    ${SUDO} yum install -y devtoolset-11
fi

${SUDO} yum erase -y openssl-devel || true
${SUDO} yum install -y \
    binutils \
    cmake3 \
    git \
    gzip \
    libyaml-devel \
    make \
    ninja-build \
    openssl11 \
    openssl11-devel \
    readline-devel \
    tar \
    which \
    xz \

CC_PREFIX=/opt/rh/devtoolset-11/root
export RUBY_CONFIGURE_FLAGS="--with-openssl-include=/usr/include/openssl11 --with-openssl-lib=/usr/lib64/openssl11 CC=${CC_PREFIX}/bin/gcc CXX=${CC_PREFIX}/bin/g++"

exec ${PROJECT_ROOT}/bin/jenkins/install-rubies.sh
