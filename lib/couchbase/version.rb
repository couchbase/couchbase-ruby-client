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
  # @example Display version and all dependencies in command line
  #   # ruby -rcouchbase -e 'pp Couchbase::VERSION'
  #   {:sdk=>"3.2.0",
  #    :backend=>"1.6.0",
  #    :build_timestamp=>"2021-08-04 10:10:35",
  #    :revision=>"6c069e4e6965117c7240b331847dc3f62afe0554",
  #    :platform=>"Linux-4.18.0-326.el8.x86_64",
  #    :cpu=>"x86_64",
  #    :cc=>"GNU 8.5.0",
  #    :cxx=>"GNU 8.5.0",
  #    :ruby=>"2.7.0",
  #    :spdlog=>"1.8.1",
  #    :asio=>"1.18.0",
  #    :snappy=>"1.1.8",
  #    :http_parser=>"2.9.4",
  #    :openssl_headers=>"OpenSSL 1.1.1k  FIPS 25 Mar 2021",
  #    :openssl_runtime=>"OpenSSL 1.1.1k  FIPS 25 Mar 2021"}
  VERSION = {} unless defined?(VERSION) # rubocop:disable Style/MutableConstant
  VERSION.update(:sdk => "3.4.0".freeze)
end
