# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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
  module Tracing
    # @!macro volatile
    class RequestSpan
      def set_attribute(key, value)
        raise NotImplementedError, "The RequestSpan does not implement #set_attribute"
      end

      def finish(end_timestamp: nil)
        raise NotImplementedError, "The RequestSpan does not implement #finish"
      end
    end
  end
end
