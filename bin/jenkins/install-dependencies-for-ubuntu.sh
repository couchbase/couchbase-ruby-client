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

${SUDO} apt-get update -y

if ! command -v ruby &> /dev/null
then
    ${SUDO} apt install -y ruby
fi

${SUDO} apt-get install -y \
    clang \
    clang-format \
    clang-tidy \
    clang-tools \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    libc++-dev \
    libc++abi-dev \
    libssl-dev \
    libyaml-dev \
    make \
    ninja-build \
    xz-utils \

# NOTE: Ubuntu 22+ removed OpenSSL 1.1 compatibility,
#       so Ruby 3.0.x will not work without extra patches
export SUPPORTED_RUBY_VERSIONS="3.1 3.2 3.3"

rm -rf ${HOME}/.rubies
exec ${PROJECT_ROOT}/bin/jenkins/install-rubies.sh
