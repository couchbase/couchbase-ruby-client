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

require 'google/protobuf/timestamp_pb'

require_relative 'commands/key_value'
require_relative 'commands/query_index_manager'
require_relative 'commands/query'
require_relative 'commands/search_index_manager'
require_relative 'commands/search'
require_relative 'commands/collection_manager'
require_relative 'commands/bucket_manager'
require_relative 'commands/misc/update_authenticator_command'

module FIT
  module Performer
    module Commands
      def self.build_command(raw_command, connection, span_owner, executed_cmd_count)
        cluster = connection.cluster
        cmd_type = raw_command.command
        kv_cmd_types = [:insert, :get, :remove, :replace, :upsert, :range_scan]

        current_time = Time.now.to_f
        seconds = current_time.truncate
        nanos = ((current_time - seconds) * (10**9)).round

        cmd_kwargs = {
          initiated: Google::Protobuf::Timestamp.new(seconds: seconds, nanos: nanos),
          return_result: raw_command.return_result,
          get_span_fn: lambda { |span_id| span_owner.get_span(span_id) },
        }

        if cmd_type == :cluster_command
          cluster_cmd = raw_command.cluster_command
          cluster_cmd_type = cluster_cmd.command
          case cluster_cmd_type
          when :query_index_manager
            Commands::QueryIndexManager.build_cluster_level_command(cluster_cmd.query_index_manager, cluster, cmd_kwargs)
          when :query
            Commands::Query.build_cluster_level_command(cluster_cmd.query, cluster, cmd_kwargs)
          when :search_index_manager
            Commands::SearchIndexManager.build_cluster_level_command(cluster_cmd.search_index_manager, cluster, cmd_kwargs)
          when :bucket_manager
            Commands::BucketManager.build_command(cluster_cmd.bucket_manager, cluster, cmd_kwargs)
          when :search
            Commands::Search.build_cluster_level_command(cluster_cmd.search, cluster, cmd_kwargs)
          when :search_v2
            Commands::Search.build_cluster_level_command(cluster_cmd.search_v2, cluster, cmd_kwargs)
          when :authenticator
            Commands::Misc::UpdateAuthenticatorCommand.create_command(
              authenticator: connection.create_authenticator(cluster_cmd.authenticator),
              cluster: cluster,
              **cmd_kwargs,
            )
          else
            raise PerformerError, "Cluster-level command `#{cluster_cmd_type}` not implemented"
          end

        elsif cmd_type == :bucket_command
          bucket_cmd = raw_command.bucket_command
          bucket_cmd_type = bucket_cmd.command
          bucket = cluster.bucket(bucket_cmd.bucket_name)
          case bucket_cmd_type
          when :collection_manager
            Commands::CollectionManager.build_command(bucket_cmd.collection_manager, bucket, cmd_kwargs)
          else
            raise PerformerError, "Bucket-level command `#{bucket_cmd_type}` not implemented"
          end

        elsif cmd_type == :scope_command
          scope_cmd = raw_command.scope_command
          scope = scope_cmd.scope.then do |s|
            cluster.bucket(s.bucket_name).scope(s.scope_name)
          end
          scope_cmd_type = scope_cmd.command
          case scope_cmd_type
          when :query
            Commands::Query.build_scope_level_command(scope_cmd.query, scope, cmd_kwargs)
          when :search
            Commands::Search.build_scope_level_command(scope_cmd.search, scope, cmd_kwargs)
          when :search_v2
            Commands::Search.build_scope_level_command(scope_cmd.search_v2, scope, cmd_kwargs)
          when :search_index_manager
            Commands::SearchIndexManager.build_scope_level_command(scope_cmd.search_index_manager, scope, cmd_kwargs)
          else
            raise PerformerError, "Scope-level command `#{scope_cmd_type}` not implemented"
          end

        elsif cmd_type == :collection_command
          collection_cmd = raw_command.collection_command
          collection = nil
          if collection_cmd.has_collection?
            collection = collection_cmd.collection.then do |c|
              cluster.bucket(c.bucket_name).scope(c.scope_name).collection(c.collection_name)
            end
          end
          collection_cmd_type = collection_cmd.command
          case collection_cmd_type
          when :query_index_manager
            Commands::QueryIndexManager.build_collection_level_command(collection_cmd.query_index_manager, collection, cmd_kwargs)
          when *Commands::KeyValue::SUPPORTED_COMMANDS
            kv_cmd = collection_cmd.public_send(collection_cmd_type)
            Commands::KeyValue.build_command(kv_cmd, collection_cmd_type, cluster, executed_cmd_count, cmd_kwargs)
          else
            raise PerformerError, "Collection-level command `#{collection_cmd_type}` not implemented"
          end

        elsif kv_cmd_types.include?(cmd_type)
          raw_kv_cmd = raw_command.public_send(cmd_type)
          Commands::KeyValue.build_command(raw_kv_cmd, cmd_type, cluster, executed_cmd_count, cmd_kwargs)

        else
          raise PerformerError, "Command type `#{cmd_type}` not recognised"
        end
      end
    end
  end
end
