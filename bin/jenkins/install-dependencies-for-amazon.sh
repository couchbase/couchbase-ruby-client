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

${SUDO} yum erase -y openssl-devel || true
${SUDO} yum install -y \
    binutils \
    gcc10 \
    gcc10-c++ \
    git \
    gzip \
    libyaml-devel \
    make \
    ninja-build \
    openssl11-devel \
    readline-devel \
    tar \
    which \
    xz \

CMAKE_VERSION=3.28.1
if [ ! -d "${HOME}/.cmake-${CMAKE_VERSION}" ]
then
    MACHINE=$(uname -m)
    curl -L -o cmake-${CMAKE_VERSION}-linux-${MACHINE}.tar.gz https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${MACHINE}.tar.gz
    tar xvf cmake-${CMAKE_VERSION}-linux-${MACHINE}.tar.gz
    mv cmake-${CMAKE_VERSION}-linux-${MACHINE} ${HOME}/.cmake-${CMAKE_VERSION}
    rm cmake-${CMAKE_VERSION}-linux-${MACHINE}.tar.gz
fi

export RUBY_CONFIGURE_FLAGS="CC=gcc10-cc CXX=gcc10-c++"

exec ${PROJECT_ROOT}/bin/jenkins/install-rubies.sh
