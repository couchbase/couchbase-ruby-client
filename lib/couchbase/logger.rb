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
  # Set log level
  #
  # @note The level might be also be set with environment variable +COUCHBASE_BACKEND_LOG_LEVEL+
  #
  # @param [Symbol] level new log level.
  #
  #   Allowed levels (in order of decreasing verbosity):
  #   * +:trace+
  #   * +:debug+
  #   * +:info+ (default)
  #   * +:warn+
  #   * +:error+
  #   * +:critical+
  #   * +:off+
  #
  # @return [void]
  def self.log_level=(level)
    Backend.set_log_level(level)
  end

  # Get current log level
  #
  # @return [Symbol] current log level
  def self.log_level
    Backend.get_log_level
  end
end
