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

module FIT
  module Performer
    module Streaming
      class StreamOwner
        def initialize
          @logger = Logger.new($stdout)
          @streams = {}
        end

        def cancel(request)
          @streams[request.stream_id].cancel
          @streams.delete(request.stream_id)
        end

        def request_items(request)
          @streams[request.stream_id].request_items(request.num_items)
        end

        def wait_for_all_streams_from_run(run_id)
          @streams.each_value do |stream|
            stream.wait if stream.run_id == run_id
          end
          @logger.info("All streams from run #{run_id} have finished")
          @streams.delete_if { |_key, stream| stream.run_id == run_id }
        end

        def register_and_start_stream(stream)
          stream_id = stream.stream_id

          raise PerformerError, "Stream #{stream_id} already exists" if @streams.include?(stream_id)

          @logger.info("Registering stream #{stream_id}")
          @streams[stream_id] = stream
          stream.start
        end
      end
    end
  end
end
