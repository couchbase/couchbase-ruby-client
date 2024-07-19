# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
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

module Couchbase
  # Version of the library and all bundled dependencies
  #
  # @example Display version (+Couchbase::BUILD_INFO+ contains more details)
  #   $ ruby -rcouchbase -e 'pp Couchbase::VERSION'
  #   {:sdk=>"3.4.0", :ruby_abi=>"3.1.0", :revision=>"416fe68e6029ec8a4c40611cf6e6b30d3b90d20f"}
  VERSION = {} unless defined?(VERSION) # rubocop:disable Style/MutableConstant
  VERSION.update(:sdk => "3.5.2.snapshot")
end
