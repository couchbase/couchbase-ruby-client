# frozen_string_literal: true

#  Copyright 2022-Present. Couchbase, Inc.
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

require_relative "generated/query.v1_pb"

require "google/protobuf/well_known_types"

module Couchbase
  module StellarNebula
    class QueryOptions
      attr_accessor :adhoc
      attr_accessor :client_context_id
      attr_accessor :max_parallelism
      attr_accessor :readonly
      attr_accessor :scan_wait
      attr_accessor :scan_cap
      attr_accessor :pipeline_batch
      attr_accessor :pipeline_cap
      attr_accessor :metrics
      attr_accessor :profile
      attr_accessor :flex_index
      attr_accessor :preserve_expiry
      attr_accessor :scope_qualifier
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]
      attr_accessor :timeout

      def initialize(adhoc: true,
                     client_context_id: nil,
                     max_parallelism: nil,
                     readonly: false,
                     scan_wait: nil,
                     scan_cap: nil,
                     pipeline_cap: nil,
                     pipeline_batch: nil,
                     metrics: nil,
                     profile: :off,
                     flex_index: nil,
                     preserve_expiry: nil,
                     scope_qualifier: nil,
                     scan_consistency: :not_bounded,
                     mutation_state: nil,
                     transcoder: JsonTranscoder.new,
                     positional_parameters: nil,
                     named_parameters: nil,
                     timeout: nil)
        raise ArgumentError, "Cannot pass positional and named parameters at the same time" if positional_parameters && named_parameters

        @timeout = timeout
        @adhoc = adhoc
        @client_context_id = client_context_id
        @max_parallelism = max_parallelism
        @readonly = readonly
        @scan_wait = scan_wait
        @scan_cap = scan_cap
        @pipeline_cap = pipeline_cap
        @pipeline_batch = pipeline_batch
        @metrics = metrics
        @profile = profile
        @flex_index = flex_index
        @preserve_expiry = preserve_expiry
        @scope_qualifier = scope_qualifier
        @scan_consistency = scan_consistency
        @mutation_state = mutation_state
        @transcoder = transcoder
        @positional_parameters = positional_parameters
        @named_parameters = named_parameters
        @raw_parameters = {}
        @timeout = timeout

        yield self if block_given?
      end

      def raw(key, value)
        @raw_parameters[key] = JSON.generate(value)
      end

      def scan_consistency=(level)
        @mutation_state = nil if @mutation_state
        @scan_consistency = level
      end

      def consistent_with(mutation_state)
        @scan_consistency = nil if @scan_consistency
        @mutation_state = mutation_state
      end

      def positional_parameters(positional)
        @positional_parameters = positional
        @named_parameters = nil
      end

      def export_positional_parameters
        @positional_parameters&.map { |p| JSON.dump(p) }
      end

      def named_parameters(named)
        @named_parameters = named
        @positional_parameters = nil
      end

      def export_named_parameters
        @named_parameters&.each_with_object({}) { |(n, v), o| o[n.to_s] = JSON.dump(v) }
      end

      # @api private
      # @return [MutationState]
      attr_reader :mutation_state

      # @api private
      # @return [Hash<String => #to_json>]
      attr_reader :raw_parameters

      def to_request(scope_name: nil, bucket_name: nil)
        opts = {}

        unless @scope_qualifier.nil?
          if @scope_qualifier.include? ":"
            bucket_name, scope_name = @scope_qualifier.split(":")[1].split(".")
          else
            bucket_name, scope_name = @scope_qualifier.split(".")
          end
        end
        opts[:scope_name] = scope_name unless scope_name.nil?
        opts[:bucket_name] = bucket_name unless bucket_name.nil?

        opts[:read_only] = @readonly unless @readonly.nil?
        opts[:prepared] = !@adhoc

        tuning_opts = {}
        tuning_opts[:max_parallelism] = @max_parallelism unless @max_parallelism.nil?
        tuning_opts[:pipeline_batch] = @pipeline_batch unless @pipeline_batch.nil?
        tuning_opts[:pipeline_cap] = @pipeline_cap unless @pipeline_cap.nil?
        unless @scan_wait.nil?
          tuning_opts[:scan_wait] = Google::Protobuf::Duration.new(
            {:nanos => (10**6) * Utils::Time.extract_duration(expiry)}
          )
        end
        tuning_opts[:scan_cap] = @scan_cap unless @scan_cap.nil?
        tuning_opts[:disable_metrics] = !@metrics unless @metrics.nil?
        opts[:tuning_options] = Generated::Query::V1::QueryRequest::TuningOptions.new(**tuning_opts) unless tuning_opts.empty?

        opts[:client_context_id] = @client_context_id unless @client_context_id.nil?
        opts[:scan_consistency] = @scan_consistency.upcase unless @scan_consistency.nil?
        opts[:positional_parameters] = export_positional_parameters unless @positional_parameters.nil?
        opts[:named_parameters] = export_named_parameters unless @named_parameters.nil?
        opts[:flex_index] = @flex_index unless @flex_index.nil?
        opts[:preserve_expiry] = @preserve_expiry unless @preserve_expiry.nil?
        opts[:consistent_with] = @mutation_state.to_proto unless @mutation_state.nil?
        opts[:profile_mode] = @profile.upcase

        opts
      end

      DEFAULT = QueryOptions.new.freeze
    end
  end
end
