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

require_relative 'search_index_manager/allow_querying_command'
require_relative 'search_index_manager/analyze_document_command'
require_relative 'search_index_manager/disallow_querying_command'
require_relative 'search_index_manager/drop_index_command'
require_relative 'search_index_manager/freeze_plan_command'
require_relative 'search_index_manager/get_all_indexes_command'
require_relative 'search_index_manager/get_index_command'
require_relative 'search_index_manager/get_indexed_documents_count_command'
require_relative 'search_index_manager/pause_ingest_command'
require_relative 'search_index_manager/resume_ingest_command'
require_relative 'search_index_manager/unfreeze_plan_command'
require_relative 'search_index_manager/upsert_index_command'

module FIT
  module Performer
    module Commands
      module SearchIndexManager
        def self.get_index_definition(raw_cmd)
          raw_defn = JSON.parse(raw_cmd.index_definition)
          Couchbase::Management::SearchIndex.new do |index|
            index.name = raw_defn['name'] if raw_defn.key?('name')
            index.type = raw_defn['type'] if raw_defn.key?('type')
            index.uuid = raw_defn['uuid'] if raw_defn.key?('uuid')
            index.params = raw_defn['params'] if raw_defn.key?('params')
            index.source_name = raw_defn['sourceName'] if raw_defn.key?('sourceName')
            index.source_type = raw_defn['sourceType'] if raw_defn.key?('sourceType')
            index.source_uuid = raw_defn['sourceUUID'] if raw_defn.key?('sourceUUID')
            index.source_params = raw_defn['sourceParams'] if raw_defn.key?('sourceParams')
            index.plan_params = raw_defn['planParams'] if raw_defn.key?('planParams')
          end
        end

        def self.build_cluster_level_command(raw_cmd, cluster, cmd_kwargs)
          cmd_kwargs[:manager] = cluster.search_indexes
          build_shared_command(raw_cmd.shared, cmd_kwargs)
        end

        def self.build_scope_level_command(raw_cmd, scope, cmd_kwargs)
          cmd_kwargs[:manager] = scope.search_indexes
          build_shared_command(raw_cmd.shared, cmd_kwargs)
        end

        def self.build_shared_command(raw_cmd, cmd_kwargs)
          cmd_type = raw_cmd.command
          begin
            cmd = raw_cmd.public_send(cmd_type)
          rescue NoMethodError
            raise PerformerError, "Search index management command `#{cmd_type}` not implemented"
          end

          cmd_kwargs[:raw_options] = cmd.options

          case cmd_type
          when :get_index
            cmd_kwargs[:index_name] = cmd.index_name
            GetIndexCommand.create_command(**cmd_kwargs)
          when :get_all_indexes
            GetAllIndexesCommand.create_command(**cmd_kwargs)
          when :upsert_index
            cmd_kwargs[:index_definition] = get_index_definition(cmd)
            UpsertIndexCommand.create_command(**cmd_kwargs)
          when :drop_index
            cmd_kwargs[:index_name] = cmd.index_name
            DropIndexCommand.create_command(**cmd_kwargs)
          when :get_indexed_documents_count
            cmd_kwargs[:index_name] = cmd.index_name
            GetIndexedDocumentsCountCommand.create_command(**cmd_kwargs)
          when :pause_ingest
            cmd_kwargs[:index_name] = cmd.index_name
            PauseIngestCommand.create_command(**cmd_kwargs)
          when :resume_ingest
            cmd_kwargs[:index_name] = cmd.index_name
            ResumeIngestCommand.create_command(**cmd_kwargs)
          when :allow_querying
            cmd_kwargs[:index_name] = cmd.index_name
            AllowQueryingCommand.create_command(**cmd_kwargs)
          when :disallow_querying
            cmd_kwargs[:index_name] = cmd.index_name
            DisallowQueryingCommand.create_command(**cmd_kwargs)
          when :freeze_plan
            cmd_kwargs[:index_name] = cmd.index_name
            FreezePlanCommand.create_command(**cmd_kwargs)
          when :unfreeze_plan
            cmd_kwargs[:index_name] = cmd.index_name
            UnfreezePlanCommand.create_command(**cmd_kwargs)
          when :analyze_document
            cmd_kwargs[:index_name] = cmd.index_name
            cmd_kwargs[:document] = JSON.parse(cmd.document)
            AnalyzeDocumentCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "Search index management command `#{cmd_type}` not implemented"
          end
        end
      end
    end
  end
end
