#  Copyright 2020-2021 Couchbase, Inc.
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

require "couchbase/errors"

require "time"

module Couchbase
  module Management
    class UserManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Get a user
      #
      # @param [String] username ID of the user
      # @param [GetUserOptions] options
      #
      # @return [UserAndMetadata]
      #
      # @raise [ArgumentError]
      # @raise [Error::UserNotFound]
      def get_user(username, options = GetUserOptions.new)
        resp = @backend.user_get(options.domain, username, options.timeout)
        extract_user(resp)
      end

      # Gets all users
      #
      # @return [Array<UserAndMetadata>]
      def get_all_users(options = GetAllUsersOptions.new)
        resp = @backend.user_get_all(options.domain, options.timeout)
        resp.map { |entry| extract_user(entry) }
      end

      # Creates or updates a user
      #
      # @param [User] user the new version of the user
      # @param [UpsertUserOptions] options
      #
      # @raise [ArgumentError]
      def upsert_user(user, options = UpsertUserOptions.new)
        @backend.user_upsert(
          options.domain,
          {
            username: user.username,
            display_name: user.display_name,
            groups: user.groups,
            password: user.password,
            roles: user.roles.map do |role|
                     {
                       name: role.name,
                       bucket: role.bucket,
                       scope: role.scope,
                       collection: role.collection,
                     }
                   end,
          }, options.timeout
        )
      end

      # Removes a user
      #
      # @param [String] username ID of the user
      # @param [DropUserOptions] options
      def drop_user(username, options = DropUserOptions.new)
        @backend.user_drop(options.domain, username, options.timeout)
      end

      # Gets all roles supported by the server
      #
      # @param [GetRolesOptions] options
      #
      # @return [Array<RoleAndDescription>]
      def get_roles(options = GetRolesOptions.new)
        resp = @backend.role_get_all(options.timeout)
        resp.map do |r|
          RoleAndDescription.new do |role|
            role.name = r[:name]
            role.display_name = r[:display_name]
            role.description = r[:description]
            role.bucket = r[:bucket]
            role.scope = r[:scope]
            role.collection = r[:collection]
          end
        end
      end

      # Gets a group
      #
      # @param [String] group_name name of the group to get
      #
      # @return [Group]
      #
      # @raise [ArgumentError]
      # @raise [Error::GroupNotFound]
      def get_group(group_name, options = GetGroupOptions.new)
        resp = @backend.group_get(group_name, options.timeout)
        extract_group(resp)
      end

      # Gets all groups
      #
      # @param [GetAllGroupsOptions] options
      #
      # @return [Array<Group>]
      def get_all_groups(options = GetAllGroupsOptions.new)
        resp = @backend.group_get_all(options.timeout)
        resp.map { |entry| extract_group(entry) }
      end

      # Creates or updates a group
      #
      # @param [Group] group the new version of the group
      # @param [UpsertGroupOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::GroupNotFound]
      def upsert_group(group, options = UpsertGroupOptions.new)
        @backend.group_upsert({
          name: group.name,
          description: group.description,
          ldap_group_reference: group.ldap_group_reference,
          roles: group.roles.map do |role|
                   {
                     name: role.name,
                     bucket: role.bucket,
                     scope: role.scope,
                     collection: role.collection,
                   }
                 end,
        }, options.timeout)
      end

      # Removes a group
      #
      # @param [String] group_name name of the group
      # @param [DropGroupOptions] options
      #
      # @raise [Error::GroupNotFound]
      def drop_group(group_name, options = DropGroupOptions.new)
        @backend.group_drop(group_name, options.timeout)
      end

      class GetUserOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetUserOptions] self
        def initialize
          @domain = :local
          yield self if block_given?
        end
      end

      class GetAllUsersOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllUsersOptions] self
        def initialize
          @domain = :local
          yield self if block_given?
        end
      end

      class UpsertUserOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [UpsertUserOptions] self
        def initialize
          @domain = :local
          yield self if block_given?
        end
      end

      class DropUserOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropUserOptions] self
        def initialize
          @domain = :local
          yield self if block_given?
        end
      end

      class GetRolesOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetRolesOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetGroupOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetGroupOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetAllGroupsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllGroupsOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class UpsertGroupOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [UpsertGroupOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class DropGroupOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropGroupOptions] self
        def initialize
          yield self if block_given?
        end
      end

      private

      def extract_group(resp)
        Group.new do |group|
          group.name = resp[:name]
          group.description = resp[:description]
          group.ldap_group_reference = resp[:ldap_group_reference]
          if resp[:roles]
            group.roles = resp[:roles].map do |r|
              Role.new do |role|
                role.name = r[:name]
                role.bucket = r[:bucket]
                role.scope = r[:scope]
                role.collection = r[:collection]
              end
            end
          end
        end
      end

      def extract_user(resp)
        UserAndMetadata.new do |user|
          user.domain = resp[:domain]
          user.username = resp[:username]
          user.display_name = resp[:display_name]
          user.groups = resp[:groups]
          user.external_groups = resp[:external_groups]
          user.password_changed = Time.parse(resp[:password_changed]) if resp[:password_changed]
          if resp[:roles]
            user.roles = resp[:roles].map do |r|
              Role.new do |role|
                role.name = r[:name]
                role.bucket = r[:bucket]
                role.scope = r[:scope]
                role.collection = r[:collection]
              end
            end
          end
          if resp[:effective_roles]
            user.effective_roles = resp[:effective_roles].map do |r|
              RoleAndOrigins.new do |role|
                role.name = r[:name]
                role.bucket = r[:bucket]
                role.scope = r[:scope]
                role.collection = r[:collection]
                if r[:origins]
                  role.origins = r[:origins].map do |o|
                    Origin.new do |origin|
                      origin.type = o[:type]
                      origin.name = o[:name]
                    end
                  end
                end
              end
            end
          end
        end
      end
    end

    class Role
      # @return [String]
      attr_accessor :name

      # @return [String]
      attr_accessor :bucket

      # @return [String]
      attr_accessor :scope

      # @return [String]
      attr_accessor :collection

      # @yieldparam [Role] self
      def initialize
        yield self if block_given?
      end
    end

    class RoleAndDescription < Role
      # @return [String]
      attr_accessor :display_name

      # @return [String]
      attr_accessor :description

      # @yieldparam [RoleAndDescription] self
      def initialize
        super
        yield self if block_given?
      end
    end

    class Origin
      # @return [String]
      attr_writer :type

      # @return [String]
      attr_writer :name

      # @yieldparam [Origin] self
      def initialize
        yield self if block_given?
      end
    end

    class RoleAndOrigins < Role
      # @return [Array<Origin>]
      attr_writer :origins

      # @yieldparam [RoleAndOrigins] self
      def initialize
        super
        @origins = []
        yield self if block_given?
      end
    end

    class User
      # @return [String]
      attr_accessor :username

      # @return [String]
      attr_accessor :display_name

      # @return [Array<String>]
      attr_accessor :groups

      # @return [Array<Role>]
      attr_accessor :roles

      # @return [String]
      attr_accessor :password

      # @yieldparam [User] self
      def initialize
        @groups = []
        @roles = []
        yield self if block_given?
      end
    end

    class UserAndMetadata < User
      # @return [:local, :external]
      attr_accessor :domain

      # @return [Array<RoleAndOrigins>]
      attr_accessor :effective_roles

      # @return [Time]
      attr_accessor :password_changed

      # @return [Array<String>]
      attr_accessor :external_groups

      # @yieldparam [UserAndMetadata] self
      def initialize
        super
        @effective_roles = []
        yield self if block_given?
      end
    end

    class Group
      # @return [String]
      attr_accessor :name

      # @return [String]
      attr_accessor :description

      # @return [Array<Role>]
      attr_accessor :roles

      # @return [String]
      attr_accessor :ldap_group_reference

      # @yieldparam [Group] self
      def initialize
        @roles = []
        yield self if block_given?
      end
    end
  end
end
