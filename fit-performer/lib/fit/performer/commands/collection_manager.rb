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

require 'fit/protocol/sdk.bucket.collection_manager_pb'

require_relative 'collection_manager/get_all_scopes_command'
require_relative 'collection_manager/create_scope_command'
require_relative 'collection_manager/drop_scope_command'
require_relative 'collection_manager/create_collection_command'
require_relative 'collection_manager/update_collection_command'
require_relative 'collection_manager/drop_collection_command'

module FIT
  module Performer
    module Commands
      module CollectionManager
        def self.build_command(raw_cmd, bucket, cmd_kwargs)
          cmd_type = raw_cmd.command
          cmd = raw_cmd.public_send(cmd_type)

          cmd_kwargs[:manager] = bucket.collections
          cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?

          case cmd_type
          when :get_all_scopes
            GetAllScopesCommand.create_command(**cmd_kwargs)
          when :create_scope
            cmd_kwargs[:scope_name] = cmd.name
            CreateScopeCommand.create_command(**cmd_kwargs)
          when :drop_scope
            cmd_kwargs[:scope_name] = cmd.name
            DropScopeCommand.create_command(**cmd_kwargs)
          when :create_collection
            cmd_kwargs[:scope_name] = cmd.scope_name
            cmd_kwargs[:collection_name] = cmd.name
            cmd_kwargs[:settings] = get_create_collection_settings(cmd)
            CreateCollectionCommand.create_command(**cmd_kwargs)
          when :update_collection
            cmd_kwargs[:scope_name] = cmd.scope_name
            cmd_kwargs[:collection_name] = cmd.name
            cmd_kwargs[:settings] = get_update_collection_settings(cmd)
            UpdateCollectionCommand.create_command(**cmd_kwargs)
          when :drop_collection
            cmd_kwargs[:scope_name] = cmd.scope_name
            cmd_kwargs[:collection_name] = cmd.name
            DropCollectionCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "Collection management command `#{cmd_type}` not supported"
          end
        end

        def self.get_create_collection_settings(raw_mgmt_cmd)
          return nil unless raw_mgmt_cmd.has_settings?

          Couchbase::Management::CreateCollectionSettings.new do |settings|
            settings.max_expiry = raw_mgmt_cmd.settings.expiry_secs if raw_mgmt_cmd.settings.has_expiry_secs?
            settings.history = raw_mgmt_cmd.settings.history if raw_mgmt_cmd.settings.has_history?
          end
        end

        def self.get_update_collection_settings(raw_mgmt_cmd)
          Couchbase::Management::UpdateCollectionSettings.new do |settings|
            settings.max_expiry = raw_mgmt_cmd.settings.expiry_secs if raw_mgmt_cmd.settings.has_expiry_secs?
            settings.history = raw_mgmt_cmd.settings.history if raw_mgmt_cmd.settings.has_history?
          end
        end
      end
    end
  end
end
