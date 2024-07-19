# frozen_string_literal: true

#   Copyright 2021 Couchbase, Inc.
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

require "time"

module Couchbase
  module Utils
    class GenericLoggerAdapter
      DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%6N"

      def initialize(logger, verbose: false)
        @logger = logger
        @verbose = verbose
      end

      def log(level, thread_id, seconds, nanoseconds, payload, filename, line, function)
        return unless @logger.respond_to?(level)

        progname = "cxxcbc##{thread_id}"
        payload += " at #{filename}:#{line} #{function}" if @verbose && filename
        @logger.send(level,
                     "[#{::Time.at(seconds, nanoseconds, :nanosecond).strftime(DATETIME_FORMAT)} #{progname}]  #{level} -- #{payload}")
      end
    end
  end
end
