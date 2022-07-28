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

require "logger"
require "time"

module Couchbase
  module Utils
    class StdlibLoggerAdapter
      def initialize(logger, verbose: false)
        raise ArgumentError, "logger argument must be or derive from stdlib Logger class" unless logger.is_a?(::Logger)

        @logger = logger
        @verbose = verbose
      end

      def log(level, thread_id, seconds, nanoseconds, payload, filename, line, function)
        logdev = @logger.instance_variable_get(:@logdev)
        return unless logdev

        severity = map_spdlog_level(level)
        return unless severity

        progname = "cxxcbc##{thread_id}"
        payload += " at #{filename}:#{line} #{function}" if @verbose && filename
        logdev.write(
          @logger.send(:format_message, @logger.send(:format_severity, severity), ::Time.at(seconds, nanoseconds, :nanosecond), progname,
                       payload)
        )
      end

      private

      def map_spdlog_level(level)
        case level
        when :trace, :debug
          ::Logger::Severity::DEBUG
        when :info
          ::Logger::Severity::INFO
        when :warn
          ::Logger::Severity::WARN
        when :error
          ::Logger::Severity::ERROR
        when :critical
          ::Logger::Severity::FATAL
        else # rubocop:disable Style/EmptyElse
          # covers :off
          nil
        end
      end
    end
  end
end
