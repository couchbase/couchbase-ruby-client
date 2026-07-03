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

require_relative 'query/query_command'

module FIT
  module Performer
    module Commands
      module Query
        def self.build_cluster_level_command(raw_cmd, cluster, cmd_kwargs)
          cmd_kwargs[:cluster] = cluster
          build_command(raw_cmd, cmd_kwargs)
        end

        def self.build_scope_level_command(raw_cmd, scope, cmd_kwargs)
          cmd_kwargs[:scope] = scope
          build_command(raw_cmd, cmd_kwargs)
        end

        def self.build_command(raw_cmd, cmd_kwargs)
          cmd_kwargs[:statement] = raw_cmd.statement
          cmd_kwargs[:raw_options] = raw_cmd.options if raw_cmd.has_options?
          cmd_kwargs[:content_as] = raw_cmd.content_as
          QueryCommand.create_command(**cmd_kwargs)
        end
      end
    end
  end
end
