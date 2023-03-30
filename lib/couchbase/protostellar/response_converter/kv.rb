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
        def self.from_get_response(resp, options)
          Couchbase::Collection::GetResult.new do |res|
            res.transcoder = options.transcoder
            res.cas = resp.cas
            res.expiry = resp.expiry if resp.has_expiry?
            res.encoded = resp.content

            # TODO: Handle conversion of content type & compression type to flag
            res.flags = resp.content_type.downcase
          end
        end

        def self.from_mutation_response(resp)
          Couchbase::Collection::MutationResult.new do |res|
            res.cas = resp.cas
            res.mutation_token = extract_mutation_token(resp)
          end
        end

        def self.from_touch_response(resp)
          from_mutation_response(resp)
        end

        def self.from_upsert_response(resp)
          from_mutation_response(resp)
        end

        def self.from_remove_response(resp)
          from_mutation_response(resp)
        end

        def self.from_insert_response(resp)
          from_mutation_response(resp)
        end

        def self.from_replace_response(resp)
          from_mutation_response(resp)
        end

        def self.from_exists_response(resp)
          Couchbase::Collection::ExistsResult.new do |res|
            res.cas = resp.cas
            res.exists = resp.result
          end
        end

        def self.from_lookup_in_response(resp, specs, options)
          Couchbase::Collection::LookupInResult.new do |res|
            res.cas = resp.cas
            res.transcoder = options.transcoder
            res.encoded = resp.specs.each_with_index.map do |s, idx|
              Couchbase::Collection::SubDocumentField.new do |f|
                f.exists = s.status.nil?
                f.index = idx
                f.path = specs[idx].path
                f.value = s.content.empty? ? nil : s.content
              end
            end
          end
        end

        def self.from_mutate_in_response(resp, specs, options)
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

        def self.from_increment_response(resp)
          from_counter_response(resp)
        end

        def self.from_decrement_response(resp)
          from_counter_response(resp)
        end

        def self.from_append_response(resp)
          from_mutation_response(resp)
        end

        def self.from_prepend_response(resp)
          from_mutation_response(resp)
        end

        def self.from_counter_response(resp)
          Couchbase::BinaryCollection::CounterResult.new do |res|
            res.cas = resp.cas
            res.content = resp.content
            res.mutation_token = extract_mutation_token(resp)
          end
        end

        def self.extract_mutation_token(resp)
          proto_token = resp.mutation_token
          Couchbase::MutationToken.new do |token|
            token.bucket_name = proto_token.bucket_name
            token.partition_id = proto_token.vbucket_id
            token.partition_uuid = proto_token.vbucket_uuid
            token.sequence_number = proto_token.seq_no
          end
        end
      end
    end
  end
end
