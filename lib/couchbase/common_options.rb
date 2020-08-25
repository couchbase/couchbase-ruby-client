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
  class CommonOptions
    # @return [Integer] the time in milliseconds allowed for the operation to complete
    attr_accessor :timeout

    # @return [Proc] the custom retry strategy, if set
    attr_accessor :retry_strategy

    # @return [Hash] the client context data, if set
    attr_accessor :client_context

    # @return [Span] If set holds the parent span, that should be used for this request
    attr_accessor :parent_span
  end
end
