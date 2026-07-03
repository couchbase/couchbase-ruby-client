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

require 'couchbase/options'

require_relative 'results'
require_relative 'options_builder'

module FIT
  module Performer
    module Commands
      module KeyValue
        class ScanCommand
          attr_reader :stream_config

          def initialize(
            collection:, raw_scan_type:, initiated:, return_result:,
            stream_config:, get_span_fn:, content_as: nil, raw_options: nil
          )
            @collection = collection
            @raw_scan_type = raw_scan_type
            @initiated = initiated
            @return_result = return_result
            @raw_options = raw_options
            @stream_config = stream_config
            @content_as = content_as
            @cmd_args = []
            @get_span_fn = get_span_fn
          end

          def stream_type
            :STREAM_KV_RANGE_SCAN
          end

          def set_options
            return if @raw_options.nil?

            builder = OptionsBuilder.new(options: Couchbase::Options::Scan.new, raw_options: @raw_options)
            builder.set_timeout
                   .set_batch_byte_limit
                   .set_batch_item_limit
                   .set_concurrency
                   .set_ids_only
                   .set_consistent_with
                   .set_transcoder
                   .set_parent_span(@get_span_fn)
            @cmd_args.append(builder.options)
          end

          def set_scan_type
            scan_type =
              case @raw_scan_type.type
              when :range
                proto_range_scan = @raw_scan_type.range
                case proto_range_scan.range
                when :from_to
                  from_choice = proto_range_scan.from_to.from
                  to_choice = proto_range_scan.from_to.to
                  from =
                    case from_choice.choice
                    when :term
                      proto_term = from_choice.term
                      if proto_term.has_exclusive?
                        Couchbase::ScanTerm.new(proto_term.as_string, exclusive: proto_term.exclusive)
                      else
                        Couchbase::ScanTerm.new(proto_term.as_string)
                      end
                    when :default
                      nil
                    else
                      raise PerformerError, "Scan term type #{from_choice.choice} not supported"
                    end
                  to =
                    case to_choice.choice
                    when :term
                      proto_term = to_choice.term
                      if proto_term.has_exclusive?
                        Couchbase::ScanTerm.new(proto_term.as_string, exclusive: proto_term.exclusive)
                      else
                        Couchbase::ScanTerm.new(proto_term.as_string)
                      end
                    when :default
                      nil
                    else
                      raise PerformerError, "Scan term type #{to_choice.choice} not supported"
                    end
                  Couchbase::RangeScan.new(from: from, to: to)
                when :doc_id_prefix
                  Couchbase::PrefixScan.new(proto_range_scan.doc_id_prefix)
                else
                  raise PerformerError, "Range scan range definition with `#{proto_range_scan.range}` not supported"
                end
              when :sampling
                proto_sampling_scan = @raw_scan_type.sampling
                if proto_sampling_scan.has_seed?
                  Couchbase::SamplingScan.new(proto_sampling_scan.limit, proto_sampling_scan.seed)
                else
                  Couchbase::SamplingScan.new(proto_sampling_scan.limit)
                end
              else
                raise PerformerError, "Scan type `#{@raw_scan_type.type}` not recognised"
              end

            @cmd_args.append(scan_type)
          end

          def execute_command
            Results.as_scan_result_stream(
              initiated: @initiated, stream_id: @stream_config.stream_id, content_as: @content_as,
            ) do
              @collection.scan(*@cmd_args)
            end
          end

          def self.create_command(...)
            cmd = ScanCommand.new(...)
            cmd.set_scan_type
            cmd.set_options
            cmd
          end
        end
      end
    end
  end
end
