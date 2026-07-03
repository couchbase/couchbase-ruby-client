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

require 'couchbase/subdoc'

require 'securerandom'

require_relative 'key_value/get_command'
require_relative 'key_value/insert_command'
require_relative 'key_value/remove_command'
require_relative 'key_value/replace_command'
require_relative 'key_value/upsert_command'
require_relative 'key_value/get_and_lock_command'
require_relative 'key_value/get_and_touch_command'
require_relative 'key_value/exists_command'
require_relative 'key_value/touch_command'
require_relative 'key_value/unlock_command'
require_relative 'key_value/scan_command'
require_relative 'key_value/lookup_in_command'
require_relative 'key_value/lookup_in_any_replica_command'
require_relative 'key_value/lookup_in_all_replicas_command'
require_relative 'key_value/mutate_in_command'
require_relative 'key_value/append_command'
require_relative 'key_value/prepend_command'
require_relative 'key_value/increment_command'
require_relative 'key_value/decrement_command'
require_relative 'key_value/get_any_replica_command'
require_relative 'key_value/get_all_replicas_command'

module FIT
  module Performer
    module Commands
      module KeyValue # rubocop:disable Metrics/ModuleLength
        SUPPORTED_COMMANDS = [
          :lookup_in, :lookup_in_all_replicas, :lookup_in_any_replica, :get_and_lock, :unlock, :get_and_touch,
          :exists, :touch, :binary, :mutate_in, :get_any_replica, :get_all_replicas
        ].freeze

        def self.build_command(raw_kv_cmd, kv_cmd_type, cluster, executed_cmd_count, cmd_kwargs)
          return build_binary_command(raw_kv_cmd, cluster, executed_cmd_count, cmd_kwargs) if kv_cmd_type == :binary

          cmd_kwargs[:collection] = get_collection(raw_kv_cmd, cluster)
          cmd_kwargs[:raw_options] = raw_kv_cmd.options if raw_kv_cmd.has_options?

          cmd_kwargs[:doc_id] = get_doc_id(raw_kv_cmd.location, executed_cmd_count) if kv_cmd_type != :range_scan

          case kv_cmd_type
          when :insert
            cmd_kwargs[:content] = get_content(raw_kv_cmd.content)
            InsertCommand.create_command(**cmd_kwargs)
          when :get
            cmd_kwargs[:content_as] = raw_kv_cmd.content_as
            GetCommand.create_command(**cmd_kwargs)
          when :remove
            RemoveCommand.create_command(**cmd_kwargs)
          when :replace
            cmd_kwargs[:content] = get_content(raw_kv_cmd.content)
            ReplaceCommand.create_command(**cmd_kwargs)
          when :upsert
            cmd_kwargs[:content] = get_content(raw_kv_cmd.content)
            UpsertCommand.create_command(**cmd_kwargs)
          when :range_scan
            cmd_kwargs.update({
              raw_scan_type: raw_kv_cmd.scan_type,
              stream_config: raw_kv_cmd.stream_config,
            })
            cmd_kwargs[:content_as] = raw_kv_cmd.content_as if raw_kv_cmd.has_content_as?
            ScanCommand.create_command(**cmd_kwargs)
          when :lookup_in
            cmd_kwargs[:specs] = get_lookup_in_specs(raw_kv_cmd)
            cmd_kwargs[:raw_specs] = raw_kv_cmd.spec # Used for per-spec content_as
            LookupInCommand.create_command(**cmd_kwargs)
          when :lookup_in_any_replica
            cmd_kwargs[:specs] = get_lookup_in_specs(raw_kv_cmd)
            cmd_kwargs[:raw_specs] = raw_kv_cmd.spec # Used for per-spec content_as
            LookupInAnyReplicaCommand.create_command(**cmd_kwargs)
          when :lookup_in_all_replicas
            cmd_kwargs.update({
              specs: get_lookup_in_specs(raw_kv_cmd),
              raw_specs: raw_kv_cmd.spec, # Used for per-spec content_as
              stream_config: raw_kv_cmd.stream_config,
            })
            LookupInAllReplicasCommand.create_command(**cmd_kwargs)
          when :mutate_in
            cmd_kwargs[:specs] = get_mutate_in_specs(raw_kv_cmd)
            cmd_kwargs[:raw_specs] = raw_kv_cmd.spec
            MutateInCommand.create_command(**cmd_kwargs)
          when :get_and_lock
            cmd_kwargs[:lock_time] = raw_kv_cmd.duration.seconds
            cmd_kwargs[:content_as] = raw_kv_cmd.content_as
            GetAndLockCommand.create_command(**cmd_kwargs)
          when :get_and_touch
            cmd_kwargs[:expiry] = get_expiry(raw_kv_cmd)
            cmd_kwargs[:content_as] = raw_kv_cmd.content_as
            GetAndTouchCommand.create_command(**cmd_kwargs)
          when :exists
            ExistsCommand.create_command(**cmd_kwargs)
          when :touch
            cmd_kwargs[:expiry] = get_expiry(raw_kv_cmd)
            TouchCommand.create_command(**cmd_kwargs)
          when :unlock
            cmd_kwargs[:cas] = raw_kv_cmd.cas
            UnlockCommand.create_command(**cmd_kwargs)
          when :get_any_replica
            cmd_kwargs[:content_as] = raw_kv_cmd.content_as
            GetAnyReplicaCommand.create_command(**cmd_kwargs)
          when :get_all_replicas
            cmd_kwargs.update({
              content_as: raw_kv_cmd.content_as,
              stream_config: raw_kv_cmd.stream_config,
            })
            GetAllReplicasCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "KV command type `#{kv_cmd_type}` not supported"
          end
        end

        def self.build_binary_command(raw_cmd, cluster, executed_cmd_count, cmd_kwargs)
          binary_cmd_type = raw_cmd.command
          raw_binary_cmd = raw_cmd.public_send(binary_cmd_type)

          cmd_kwargs[:binary_collection] = get_collection(raw_binary_cmd, cluster).binary
          cmd_kwargs[:doc_id] = get_doc_id(raw_binary_cmd.location, executed_cmd_count)
          cmd_kwargs[:raw_options] = raw_binary_cmd.options if raw_binary_cmd.has_options?

          case binary_cmd_type
          when :append
            cmd_kwargs[:content] = raw_binary_cmd.content
            AppendCommand.create_command(**cmd_kwargs)
          when :prepend
            cmd_kwargs[:content] = raw_binary_cmd.content
            PrependCommand.create_command(**cmd_kwargs)
          when :increment
            IncrementCommand.create_command(**cmd_kwargs)
          when :decrement
            DecrementCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "Binary KV command type `#{binary_cmd_type}` not supported"
          end
        end

        def self.get_doc_id(raw_doc_location, executed_cmd_count)
          location_type = raw_doc_location.location
          case location_type
          when :specific
            raw_doc_location.specific.id
          when :uuid
            SecureRandom.uuid
          when :pool
            preface = raw_doc_location.pool.id_preface
            pool_size = raw_doc_location.pool.pool_size
            selection_strategy = raw_doc_location.pool.poolSelectionStrategy
            case selection_strategy
            when :random
              distribution = raw_doc_location.pool.random.distribution
              case distribution
              when :RANDOM_DISTRIBUTION_UNIFORM
                preface + rand(pool_size).to_s
              else
                raise PerformerError, "Random distribution `#{distribution}` not supported for random pool selection strategy"
              end
            when :counter
              preface + (executed_cmd_count % pool_size).to_s
            else
              raise PerformerError, "Pool selection strategy `#{selection_strategy}` not supported"
            end
          else
            raise PerformerError, "Document location type `#{raw_doc_location}` not supported"
          end
        end

        def self.get_collection(raw_kv_cmd, cluster)
          raw_coll =
            if raw_kv_cmd.respond_to?(:location)
              raw_doc_location = raw_kv_cmd.location
              raw_doc_location.public_send(raw_doc_location.location).collection
            else
              raw_kv_cmd.collection
            end
          cluster.bucket(raw_coll.bucket_name).scope(raw_coll.scope_name).collection(raw_coll.collection_name)
        end

        def self.get_content(raw_content)
          content_type = raw_content.content
          case content_type
          when :passthrough_string
            raw_content.passthrough_string
          when :convert_to_json
            JSON.parse(raw_content.convert_to_json)
          when :null
            nil
          when :byte_array
            raw_content.byte_array
          else
            raise PerformerError, "Content type `#{content_type}` not supported"
          end
        end

        def self.get_expiry(raw_kv_cmd)
          expiry_type = raw_kv_cmd.expiry.expiryType
          case expiry_type
          when :relativeSecs
            raw_kv_cmd.expiry.relativeSecs
          when :absoluteEpochSecs
            Time.at(raw_kv_cmd.expiry.absoluteEpochSecs)
          else
            raise PerformerError, "Expiry type `#{expiry_type}` not supported"
          end
        end

        def self.get_lookup_in_specs(raw_kv_cmd)
          raw_kv_cmd.spec.map do |raw_spec|
            spec =
              case raw_spec.operation
              when :exists
                Couchbase::LookupInSpec.exists(raw_spec.exists.path)
              when :get
                Couchbase::LookupInSpec.get(raw_spec.get.path)
              when :count
                Couchbase::LookupInSpec.count(raw_spec.count.path)
              else
                raise PerformerError, "LookupIn spec type `#{raw_spec.operation}` not supported"
              end
            op = raw_spec.public_send(raw_spec.operation)
            spec.xattr if op.has_xattr? && op.xattr
            spec
          end
        end

        def self.get_mutate_in_specs(raw_kv_cmd)
          raw_kv_cmd.spec.map do |raw_spec|
            op = raw_spec.public_send(raw_spec.operation)
            spec =
              case raw_spec.operation
              when :upsert
                Couchbase::MutateInSpec.upsert(op.path, get_mutate_in_value(op.content))
              when :insert
                Couchbase::MutateInSpec.insert(op.path, get_mutate_in_value(op.content))
              when :replace
                Couchbase::MutateInSpec.replace(op.path, get_mutate_in_value(op.content))
              when :remove
                Couchbase::MutateInSpec.remove(op.path)
              when :array_append
                Couchbase::MutateInSpec.array_append(op.path, get_mutate_in_values(op.content))
              when :array_prepend
                Couchbase::MutateInSpec.array_prepend(op.path, get_mutate_in_values(op.content))
              when :array_insert
                Couchbase::MutateInSpec.array_insert(op.path, get_mutate_in_values(op.content))
              when :array_add_unique
                Couchbase::MutateInSpec.array_add_unique(op.path, get_mutate_in_value(op.content))
              when :increment
                Couchbase::MutateInSpec.increment(op.path, op.delta)
              when :decrement
                Couchbase::MutateInSpec.decrement(op.path, op.delta)
              else
                raise PerformerError, "MutateIn spec type `#{raw_spec.operation}` not supported"
              end
            spec.xattr if op.has_xattr? && op.xattr
            spec.create_path if op.respond_to?(:create_path) && op.has_create_path? && op.create_path
            spec
          end
        end

        def self.get_mutate_in_value(proto_content)
          type = proto_content.content_or_macro
          case type
          when :content
            get_content(proto_content.content)
          when :macro
            case proto_content.macro
            when :CAS
              :cas
            when :SEQ_NO
              :seq_no
            when :VALUE_CRC_32C
              :value_crc32c
            else
              raise PerformerError, "Unexpected MutateInMacro `#{content_or_macro.macro}"
            end
          else
            raise PerformerError, "Unexpected ContentOrMacro type `#{type}`"
          end
        end

        def self.get_mutate_in_values(proto_contents)
          proto_contents.map { |proto_content| get_mutate_in_value(proto_content) }
        end
      end
    end
  end
end
