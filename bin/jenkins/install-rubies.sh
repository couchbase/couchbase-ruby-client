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

SUPPORTED_RUBY_VERSIONS=${SUPPORTED_RUBY_VERSIONS:-"3.1 3.2 3.3"}
RUBY_CONFIGURE_FLAGS=${RUBY_CONFIGURE_FLAGS:-}

RUBY_INSTALL_VERSION=0.9.3
CHRUBY_VERSION=0.3.9


RUBY_INSTALL=/usr/local/bin/ruby-install
NEED_TO_INSTALL_RUBY_INSTALL=yes
if  [ -x ${RUBY_INSTALL} -a "x$(${RUBY_INSTALL} --version | grep -o '[0-9]\.[0-9]\.[0-9]')" = "x${RUBY_INSTALL_VERSION}" ]
then
    NEED_TO_INSTALL_RUBY_INSTALL=no
fi
if [ "x${NEED_TO_INSTALL_RUBY_INSTALL}" = "xyes" ]
then
    RUBY_INSTALL_URL=https://github.com/postmodern/ruby-install/releases/download/v${RUBY_INSTALL_VERSION}/ruby-install-${RUBY_INSTALL_VERSION}.tar.gz
    curl -L -o ruby-install-${RUBY_INSTALL_VERSION}.tar.gz ${RUBY_INSTALL_URL}
    tar -xzf ruby-install-${RUBY_INSTALL_VERSION}.tar.gz
    (
        cd ruby-install-${RUBY_INSTALL_VERSION}/
        ${SUDO} make install
    )
    rm -rf ruby-install-${RUBY_INSTALL_VERSION}*
fi

CHRUBY=/usr/local/bin/chruby-exec
NEED_TO_INSTALL_CHRUBY=yes
if [ -x ${CHRUBY} -a "x$(${CHRUBY} --version | grep -o '[0-9]\.[0-9]\.[0-9]')" = "x${CHRUBY_VERSION}" ]
then
    NEED_TO_INSTALL_CHRUBY=no
fi
if [ "x${NEED_TO_INSTALL_CHRUBY}" = "xyes" ]
then
    CHRUBY_URL=https://github.com/postmodern/chruby/releases/download/v${CHRUBY_VERSION}/chruby-${CHRUBY_VERSION}.tar.gz
    curl -L -o chruby-${CHRUBY_VERSION}.tar.gz ${CHRUBY_URL}
    tar -xzf chruby-${CHRUBY_VERSION}.tar.gz
    (
        cd chruby-${CHRUBY_VERSION}/
        ${SUDO} make install
    )
    rm -rf chruby-${CHRUBY_VERSION}*
fi

until ${RUBY_INSTALL} --update
do
    if [ $? != 0 ]
    then
        ${SUDO} rm -rf ${HOME}/.cache/ruby-install/ruby/*.part
        d=$(( RANDOM % 10 + 1 ))
        echo "sleep for $d seconds"
        sleep $d
    fi
done

for RUBY_VERSION in ${SUPPORTED_RUBY_VERSIONS}
do
    until ${RUBY_INSTALL} --jobs=6 --no-reinstall --no-install-deps ruby ${RUBY_VERSION} -- --disable-install-doc ${RUBY_CONFIGURE_FLAGS}
    do
        if [ $? != 0 ]
        then
            ${SUDO} rm -rf ${HOME}/.cache/ruby-install/ruby/*.part
            d=$(( RANDOM % 10 + 1 ))
            echo "sleep for $d seconds"
            sleep $d
        fi
    done
done
