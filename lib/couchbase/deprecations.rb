# frozen_string_literal: true

#  Copyright 2020-2025 Couchbase, Inc.
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

module Couchbase
  module Deprecations
    def self.deprecate_constants(removed_in_version, parent, constants)
      deprecator = Module.new do
        define_method(:const_missing) do |old_name|
          return super unless constants.key?(old_name)

          new_name = constants[old_name]

          warn "NOTE: #{name}::#{old_name} is deprecated; use Couchbase::#{new_name} instead. " \
               "It will be removed in version #{removed_in_version}."
          Couchbase.const_get(new_name)
        end
      end
      parent.singleton_class.prepend(deprecator)
    end
  end

  Deprecations.deprecate_constants("3.6.0", Cluster,
                                   AnalyticsOptions: "Options::Analytics",
                                   ClusterOptions: "Options::Cluster",
                                   DiagnosticsOptions: "Options::Diagnostics",
                                   QueryOptions: "Options::Query",
                                   SearchOptions: "Options::Search")

  Deprecations.deprecate_constants("3.6.0", Bucket,
                                   PingOptions: "Options::Ping",
                                   ViewOptions: "Options::View")

  Deprecations.deprecate_constants("3.6.0", Collection,
                                   ExistsOptions: "Options::Exists",
                                   GetAllReplicasOptions: "Options::GetAllReplicas",
                                   GetAndLockOptions: "Options::GetAndLock",
                                   GetAndTouchOptions: "Options::GetAndTouch",
                                   GetAnyReplicaOptions: "Options::GetAnyReplica",
                                   GetOptions: "Options::Get",
                                   InsertOptions: "Options::Insert",
                                   LookupInOptions: "Options::LookupIn",
                                   MutateInOptions: "Options::MutateIn",
                                   RemoveOptions: "Options::Remove",
                                   ReplaceOptions: "Options::Replace",
                                   TouchOptions: "Options::Touch",
                                   UnlockOptions: "Options::Unlock",
                                   UpsertOptions: "Options::Upsert")
end
