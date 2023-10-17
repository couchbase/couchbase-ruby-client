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

source $(brew --prefix chruby)/share/chruby/chruby.sh

# MacOS/ARM has issues building BoringSSL using GCC compiler
if [ "x$(uname -m)" != "xarm64" -a "x$(uname -m)" != "xx86_64" ]
then
  for v in 12 11
  do
      CC_PREFIX=$(brew --prefix gcc@${v})
      if [ -d ${CC_PREFIX} ]
      then
          export CB_CC="${CC_PREFIX}/bin/gcc-${v}"
          export CB_CXX="${CC_PREFIX}/bin/g++-${v}"
          export CB_AR="${CC_PREFIX}/bin/gcc-ar-${v}"
          break
      fi
  done
fi
