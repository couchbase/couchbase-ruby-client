# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011 Couchbase, Inc.
# License:: Apache License, Version 2.0
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
#

module Couchbase
  module HttpStatus
    class Status < Exception
      class << self
        attr_accessor :status_code, :status_message
        alias_method :to_i, :status_code

        def status_line
          "#{status_code} #{status_message}"
        end
      end

      attr_reader :error, :reason
      def initialize(error, reason)
        @error = error
        @reason = reason
        super("#{error}: #{reason}")
      end

      def status_code
        self.class.status_code
      end

      def status_message
        self.class.status_message
      end

      def status_line
        self.class.status_line
      end

      def to_i
        self.class.to_i
      end
    end

    StatusMessage = {
      400 => 'Bad Request',
      401 => 'Unauthorized',
      402 => 'Payment Required',
      403 => 'Forbidden',
      404 => 'Not Found',
      405 => 'Method Not Allowed',
      406 => 'Not Acceptable',
      407 => 'Proxy Authentication Required',
      408 => 'Request Timeout',
      409 => 'Conflict',
      410 => 'Gone',
      411 => 'Length Required',
      412 => 'Precondition Failed',
      413 => 'Request Entity Too Large',
      414 => 'Request-URI Too Large',
      415 => 'Unsupported Media Type',
      416 => 'Request Range Not Satisfiable',
      417 => 'Expectation Failed',
      422 => 'Unprocessable Entity',
      423 => 'Locked',
      424 => 'Failed Dependency',
      500 => 'Internal Server Error',
      501 => 'Not Implemented',
      502 => 'Bad Gateway',
      503 => 'Service Unavailable',
      504 => 'Gateway Timeout',
      505 => 'HTTP Version Not Supported',
      507 => 'Insufficient Storage'
    }

    Errors = {}

    StatusMessage.each do |status_code, status_message|
      klass = Class.new(Status)
      klass.status_code = status_code
      klass.status_message = status_message
      klass_name = status_message.gsub(/[ \-]/,'')
      const_set(klass_name, klass)
      HttpStatus::Errors[status_code] = const_get(klass_name)
    end
  end
end
