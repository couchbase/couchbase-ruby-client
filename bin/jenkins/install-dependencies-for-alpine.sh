#!/usr/bin/env sh
#  Copyright 2020-Present Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

PROJECT_ROOT="$( cd "$(dirname "$0")/../.." >/dev/null 2>&1 ; pwd -P )"

[ "$(id -u)" -ne 0 ] && SUDO="sudo " || SUDO=""

set -x
set -e

if ! command -v ruby &> /dev/null
then
    ${SUDO} /sbin/apk add ruby
fi

${SUDO} /sbin/apk add \
    bash \
    clang \
    clang-extra-tools \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    linux-headers \
    make \
    openssl \
    openssl-dev \
    readline-dev \
    xz \
    yaml-dev\
    zlib-dev \

# NOTE: we don't want to build 3.0 on Alpine, as it will require
#       switching OpenSSL to 1.1
export SUPPORTED_RUBY_VERSIONS="3.1 3.2 3.3"

exec bash ${PROJECT_ROOT}/bin/jenkins/install-rubies.sh
