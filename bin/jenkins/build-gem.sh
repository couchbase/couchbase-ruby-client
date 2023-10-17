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

echo "HOSTNAME=${HOSTNAME}"
echo "NODE_NAME=${NODE_NAME}"
echo "CONTAINER_TAG=${CONTAINER_TAG}"
echo "JENKINS_SLAVE_LABELS=${JENKINS_SLAVE_LABELS}"
echo "NODE_LABELS=${NODE_LABELS}"

SUPPORTED_RUBY_VERSIONS="3.3 3.2 3.1 3.0"
CMAKE_VERSION=3.28.1
if [ -d "${HOME}/.cmake-${CMAKE_VERSION}/bin" ]
then
    export PATH="${HOME}/.cmake-${CMAKE_VERSION}/bin:${PATH}"
fi

set -x
set -e

source ${PROJECT_ROOT}/bin/jenkins/build-environment.sh

for RUBY_VERSION in ${SUPPORTED_RUBY_VERSIONS}
do
    if ! chruby ruby-${RUBY_VERSION}
    then
        continue
    fi
    which ruby gem bundle
    ruby --version
    bundle --version

    bundle config set --local path ${PROJECT_ROOT}/vendor/bundle/${RUBY_VERSION}
    bundle install

    GEM_VERSION=$(ruby -r${PROJECT_ROOT}/lib/couchbase/version.rb -e "puts Couchbase::VERSION[:sdk]")
    echo "--- Configured build environment for ${GEM_VERSION} (ruby ${RUBY_VERSION})"
    SOURCE_GEM_PATH="${PROJECT_ROOT}/pkg/couchbase-${GEM_VERSION}.gem"
    if [ -f "${SOURCE_GEM_PATH}" ]
    then
        echo "--- Found source package ${GEM_VERSION} (${SOURCE_GEM_PATH})"
    else
        echo "--- Inject build number ${BUILD_NUMBER} into version ${GEM_VERSION}"
        ruby ${PROJECT_ROOT}/bin/jenkins/patch-version.rb ${BUILD_NUMBER}
        GEM_VERSION=$(ruby -r${PROJECT_ROOT}/lib/couchbase/version.rb -e "puts Couchbase::VERSION[:sdk]")
        SOURCE_GEM_PATH="${PROJECT_ROOT}/pkg/couchbase-${GEM_VERSION}.gem"
        echo "--- Build source package ${GEM_VERSION}"
        bundle exec rake build
    fi

    PRECOMPILED_PATH="${PROJECT_ROOT}/pkg/binary/$(ruby -rrbconfig -e 'print RbConfig::CONFIG["ruby_version"]')"
    mkdir -p ${PRECOMPILED_PATH}
    echo "--- Build binary package for ${GEM_VERSION} using ruby ${RUBY_VERSION} (${PRECOMPILED_PATH})"

    export CB_STATIC_BORINGSSL=1
    export CB_STATIC_STDLIB=1
    export CB_REMOVE_EXT_DIRECTORY=1
    bundle install
    bundle exec gem compile --prune --output ${PRECOMPILED_PATH} ${SOURCE_GEM_PATH}
done

echo "--- Repackage binary gems into single \"fat\" gem"
ruby ${PROJECT_ROOT}/bin/jenkins/repackage-extension.rb
