# frozen_string_literal: true

#  Copyright 2023-Present. Couchbase, Inc.
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

require "couchbase/collection_options"
require "couchbase/binary_collection_options"

module Couchbase
  module Protostellar
    module ResponseConverter
      class KV
        def self.to_get_result(resp, options)
          Couchbase::Collection::GetResult.new do |res|
            res.transcoder = options.transcoder
            res.cas = resp.cas
            res.expiry = extract_expiry_time(resp) if options.respond_to?(:with_expiry) && options.with_expiry
            res.encoded = resp.content
            res.flags = resp.content_flags
          end
        end

        def self.to_mutation_result(resp)
          Couchbase::Collection::MutationResult.new do |res|
            res.cas = resp.cas
            res.mutation_token = extract_mutation_token(resp)
          end
        end

        def self.to_exists_result(resp)
          Couchbase::Collection::ExistsResult.new do |res|
            res.cas = resp.cas
            res.exists = resp.result
          end
        end

        def self.to_lookup_in_result(resp, specs, options)
          pp resp
          Couchbase::Collection::LookupInResult.new do |res|
            res.cas = resp.cas
            res.transcoder = options.transcoder
            res.encoded = resp.specs.each_with_index.map do |s, idx|
              Couchbase::Collection::SubDocumentField.new do |f|
                f.exists = s.status.nil?
                puts s.status
                f.index = idx
                f.path = specs[idx].path
                f.value = s.content.empty? ? nil : s.content
              end
            end
          end
        end

        def self.to_mutate_in_result(resp, specs, options)
          Couchbase::Collection::MutateInResult.new do |res|
            res.cas = resp.cas
            res.transcoder = options.transcoder
            res.deleted = false  # TODO: GRPC response has no deleted field for now
            res.mutation_token = extract_mutation_token(resp)
            res.encoded = resp.specs.each_with_index.map do |s, idx|
              Couchbase::Collection::SubDocumentField.new do |f|
                f.index = idx
                f.path = specs[idx].path
                f.value = s.content
              end
            end
          end
        end

        def self.to_counter_result(resp)
          Couchbase::BinaryCollection::CounterResult.new do |res|
            res.cas = resp.cas
            res.content = resp.content
            res.mutation_token = extract_mutation_token(resp)
          end
        end

        def self.extract_mutation_token(resp)
          proto_token = resp.mutation_token
          return nil if proto_token.nil?

          Couchbase::MutationToken.new do |token|
            token.bucket_name = proto_token.bucket_name
            token.partition_id = proto_token.vbucket_id
            token.partition_uuid = proto_token.vbucket_uuid
            token.sequence_number = proto_token.seq_no
          end
        end

        def self.extract_expiry_time(resp)
          timestamp = resp.expiry
          Time.at(timestamp.seconds, timestamp.nanos, :nsec)
        end
      end
    end
  end
end
