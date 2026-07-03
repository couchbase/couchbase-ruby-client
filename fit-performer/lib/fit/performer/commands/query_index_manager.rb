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

require 'fit/performer/performer_error'
require_relative 'query_index_manager/build_deferred_indexes_command'
require_relative 'query_index_manager/create_index_command'
require_relative 'query_index_manager/create_primary_index_command'
require_relative 'query_index_manager/drop_index_command'
require_relative 'query_index_manager/drop_primary_index_command'
require_relative 'query_index_manager/get_all_indexes_command'
require_relative 'query_index_manager/watch_indexes_command'

module FIT
  module Performer
    module Commands
      module QueryIndexManager
        def self.build_cluster_level_command(raw_cmd, cluster, cmd_kwargs)
          cmd_kwargs[:manager] = cluster.query_indexes
          cmd_kwargs[:bucket_name] = raw_cmd.bucket_name
          build_shared_command(raw_cmd.shared, cmd_kwargs)
        end

        def self.build_collection_level_command(raw_cmd, collection, cmd_kwargs)
          cmd_kwargs[:manager] = collection.query_indexes
          build_shared_command(raw_cmd.shared, cmd_kwargs)
        end

        def self.build_shared_command(raw_cmd, cmd_kwargs)
          cmd_type = raw_cmd.command
          case cmd_type
          when :create_primary_index
            cmd = raw_cmd.create_primary_index
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            CreatePrimaryIndexCommand.create_command(**cmd_kwargs)
          when :create_index
            cmd = raw_cmd.create_index
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            cmd_kwargs[:index_name] = cmd.index_name
            cmd_kwargs[:fields] = cmd.fields.to_a
            CreateIndexCommand.create_command(**cmd_kwargs)
          when :get_all_indexes
            cmd = raw_cmd.get_all_indexes
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            GetAllIndexesCommand.create_command(**cmd_kwargs)
          when :drop_primary_index
            cmd = raw_cmd.drop_primary_index
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            DropPrimaryIndexCommand.create_command(**cmd_kwargs)
          when :drop_index
            cmd = raw_cmd.drop_index
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            cmd_kwargs[:index_name] = cmd.index_name
            DropIndexCommand.create_command(**cmd_kwargs)
          when :watch_indexes
            cmd = raw_cmd.watch_indexes
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            cmd_kwargs[:index_names] = cmd.index_names.to_a
            cmd_kwargs[:timeout] = cmd.timeout_msecs
            WatchIndexesCommand.create_command(**cmd_kwargs)
          when :build_deferred_indexes
            cmd = raw_cmd.build_deferred_indexes
            cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?
            BuildDeferredIndexesCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "Query index manager command `#{cmd_type}` not implemented"
          end
        end
      end
    end
  end
end
