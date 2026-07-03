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

require "fit/protocol/run.top_level_pb"
require "fit/protocol/streams.top_level_pb"
require "fit/protocol/shared.exceptions_pb"

module FIT
  module Performer
    module Streaming
      class Stream
        attr_reader :run_id
        attr_reader :stream_id

        def initialize(run_id:, results:, stream_type:, stream_id:, on_demand:, enumerator:)
          @logger = Logger.new($stdout)
          @run_id = run_id
          @results = results
          @stream_type = stream_type
          @stream_id = stream_id
          @on_demand = on_demand
          @enumerator = enumerator
          @stop = false
          @thread = nil
          @requested_item_count = 0
          @requested_item_count_semaphore = Mutex.new
        end

        def start
          @logger.info("Starting stream #{@stream_id}")
          @thread = Thread.new do # rubocop:disable ThreadSafety/NewThread
            send_created_signal
            run
          end
        end

        def cancel
          @stop = true
        end

        def wait
          @thread&.join
        end

        def request_items(num_items)
          @requested_item_count_semaphore.synchronize do
            @requested_item_count += num_items
          end
        end

        def self.build_stream(run_id:, results:, sdk_command:, enumerator:)
          stream_strategy = sdk_command.stream_config.stream_when
          on_demand =
            if stream_strategy == :automatically
              false
            elsif stream_strategy == :on_demand
              true
            else
              raise PerformerError, "Stream strategy #{stream_strategy} not supported"
            end

          Stream.new(
            run_id: run_id,
            results: results,
            stream_type: sdk_command.stream_type,
            stream_id: sdk_command.stream_config.stream_id,
            on_demand: on_demand,
            enumerator: enumerator,
          )
        end

        private

        def run
          @logger.info("Running stream #{@stream_id}")
          loop do
            if @stop
              send_cancelled_signal
              return
            end

            if @on_demand && @requested_item_count <= 0
              sleep(0.1)
              next
            end

            begin
              result = @enumerator.next
            rescue StopIteration
              send_complete_signal
              return
            end

            send_error_signal(result) if result.is_a?(FIT::Protocol::Shared::Exception)
            @results << result

            next unless @on_demand

            @requested_item_count_semaphore.synchronize do
              @requested_item_count -= 1
            end
          end
        end

        def send_created_signal
          @results.push(
            FIT::Protocol::Run::Result.new(
              stream: FIT::Protocol::Streams::Signal.new(
                created: FIT::Protocol::Streams::Created.new(stream_id: @stream_id, type: @stream_type),
              ),
            ),
          )
        end

        def send_cancelled_signal
          @results.push(
            FIT::Protocol::Run::Result.new(
              stream: FIT::Protocol::Streams::Signal.new(
                cancelled: FIT::Protocol::Streams::Cancelled.new(stream_id: @stream_id),
              ),
            ),
          )
        end

        def send_complete_signal
          @results.push(
            FIT::Protocol::Run::Result.new(
              stream: FIT::Protocol::Streams::Signal.new(
                complete: FIT::Protocol::Streams::Complete.new(stream_id: @stream_id),
              ),
            ),
          )
        end

        def send_error_signal(proto_exception)
          @logger.error("Sending error signal #{proto_exception.to_h}")
          @results.push(
            FIT::Protocol::Run::Result.new(
              stream: FIT::Protocol::Streams::Signal.new(
                error: FIT::Protocol::Streams::Error.new(stream_id: @stream_id, exception: proto_exception),
              ),
            ),
          )
        end
      end
    end
  end
end
