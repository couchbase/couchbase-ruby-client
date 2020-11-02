#    Copyright 2020 Couchbase, Inc.
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
  #   {:sdk=>"3.0.1",
  #    :backend=>"1.1.0",
  #    :build_timestamp=>"2020-10-05 11:19:50",
  #    :revision=>"b59cb40f11ec2dba992eda285eae5cd7238a59c3",
  #    :platform=>"Linux-4.15.0-66-generic",
  #    :cpu=>"x86_64",
  #    :cc=>"GNU 9.3.1",
  #    :cxx=>"GNU 9.3.1",
  #    :ruby=>"2.7.0",
  #    :spdlog=>"1.8.1",
  #    :asio=>"1.18.0",
  #    :snappy=>"1.1.8",
  #    :http_parser=>"2.9.4",
  #    :openssl_headers=>"OpenSSL 1.1.1c FIPS  28 May 2019",
  #    :openssl_runtime=>"OpenSSL 1.1.1c FIPS  28 May 2019"}
  VERSION = {} unless defined?(VERSION) # rubocop:disable Style/MutableConstant
  VERSION.update(:sdk => "3.0.2".freeze)
end
