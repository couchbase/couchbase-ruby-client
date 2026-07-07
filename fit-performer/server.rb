# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

$LOAD_PATH << File.expand_path('lib')

require 'logger'
require 'optparse'
require 'grpc'
require 'fit/performer/service'

$stdout.sync = true # For logging to appear when running in docker
logger = Logger.new($stdout)

port = "0.0.0.0:8060"
server = GRPC::RpcServer.new
server.add_http2_port(port, :this_port_is_insecure)
logger.info("Listening on #{port}")
server.handle(FIT::Performer::Service.new)
server.run_till_terminated_or_interrupted([1, 'int', 'SIGQUIT'])
