#!/usr/bin/env bash
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

set -x
set -e

sw_vers
system_profiler SPSoftwareDataType SPHardwareDataType
xcode-select --print-path

brew doctor || true
brew list --versions || true
brew install \
    automake \
    bison \
    chruby \
    cmake \
    gcc@11 \
    gcc@12 \
    gdbm \
    libffi \
    libyaml \
    openssl@3 \
    readline \
    ruby \
    ruby-install \

if [ "x$(uname -m)" != "xarm64" ]
then
    GCC_VER=12
    CC_PREFIX=$(brew --prefix gcc@${GCC_VER})
    ${CC_PREFIX}/bin/gcc-${GCC_VER} --version
    ${CC_PREFIX}/bin/g++-${GCC_VER} --version
    export RUBY_CONFIGURE_FLAGS="CC=${CC_PREFIX}/bin/gcc-${GCC_VER} CXX=${CC_PREFIX}/bin/g++-${GCC_VER}"
fi

if [ ! -z "${BUILD_NUMBER}" ]
then
    # ensure the builder will not pick up anything globally
    rm -rf ${HOME}/.gem/ruby
fi
# NOTE: Exclude Ruby 3.0 to avoid openssl@1.1
export SUPPORTED_RUBY_VERSIONS="3.1 3.2 3.3"
exec ${PROJECT_ROOT}/bin/jenkins/install-rubies.sh
