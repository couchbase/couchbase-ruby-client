#    Copyright 2020 Couchbase, Inc.
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

module Couchbase
  module Management
    class UserManager
      alias_method :inspect, :to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Get a user
      #
      # @param [String] user_name ID of the user
      # @param [GetUserOptions] options
      #
      # @return [UserAndMetadata]
      #
      # @raise [ArgumentError]
      # @raise [Error::UserNotFound]
      def get_user(user_name, options = GetUserOptions.new)
        # GET /settings/rbac/users/#{options.domain}/#{user_name}
      end

      # Gets all users
      #
      # @return [Array<UserAndMetadata>]
      def get_all_users(options = GetAllUsersOptions.new)
        # GET /settings/rbac/users/#{options.domain}
      end

      # Creates or updates a user
      #
      # @param [User] user the new version of the user
      # @param [UpsertUserOptions] options
      #
      # @raise [ArgumentError]
      def upsert_user(user, options = UpsertUserOptions.new)
        # PUT /settings/rbac/users/#{options.domain}/#{user_name}
      end

      # Removes a user
      #
      # @param [String] user_name ID of the user
      # @param [DropUserOptions] options
      def drop_user(user_name, options = DropUserOptions.new)
        # DELETE /settings/rbac/users/#{options.domain}/#{user_name}
      end

      # Gets all roles supported by the server
      #
      # @param [GetRolesOptions] options
      #
      # @return [Array<RoleAndDescription>]
      def get_roles(options = GetRolesOptions.new)
        # GET /settings/rbac/roles
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
        # GET /settings/rbac/groups/#{group_name}
      end

      # Gets all groups
      #
      # @param [GetAllGroupsOptions] options
      #
      # @return [Array<Group>]
      def get_all_groups(options = GetAllGroupsOptions.new)
        # GET /settings/rbac/groups
      end

      # Creates or updates a group
      #
      # @param [Group] group the new version of the group
      # @param [UpsertGroupOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::GroupNotFound]
      def upsert_group(group, options = UpsertGroupOptions.new)
        # PUT /settings/rbac/groups/#{group.name}
      end

      # Removes a group
      #
      # @param [String] group_name name of the group
      # @param [DropGroupOptions] options
      #
      # @raise [Error::GroupNotFound]
      def drop_group(group_name, options = DropGroupOptions.new) end

      class GetUserOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @domain_name = :local
          yield self if block_given?
        end
      end

      class GetAllUsersOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @domain_name = :local
          yield self if block_given?
        end
      end

      class UpsertUserOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @domain_name = :local
          yield self if block_given?
        end
      end

      class DropUserOptions
        # @return [:local, :external] Name of the user's domain. Defaults to +:local+
        attr_accessor :domain_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @domain_name = :local
          yield self if block_given?
        end
      end

      class GetRolesOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class GetGroupOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class GetAllGroupsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class UpsertGroupOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class DropGroupOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end
    end

    class Role
      # @return [String]
      attr_accessor :name

      # @return [String]
      attr_accessor :bucket

      def initialize
        yield self if block_given?
      end
    end

    class RoleAndDescription
      # @return [Role]
      attr_reader :role

      # @return [String]
      attr_reader :display_name

      # @return [String]
      attr_reader :description

      def initialize
        yield self if block_given?
      end
    end

    class Origin
      # @return [String]
      attr_reader :type

      # @return [String]
      attr_reader :name

      def initialize
        yield self if block_given?
      end
    end

    class RoleAndOrigins
      # @return [Role]
      attr_reader :role

      # @return [Array<Origin>]
      attr_reader :origins

      def initialize
        yield self if block_given?
      end
    end

    class User
      # @return [String]
      attr_accessor :user_name

      # @return [String]
      attr_accessor :display_name

      # @return [Array<String>]
      attr_accessor :groups

      # @return [Array<Role>]
      attr_accessor :roles

      # @return [String]
      attr_writer :password

      def initialize
        yield self if block_given?
      end
    end

    class UserAndMetadata
      # @return [:local, :external]
      attr_reader :domain

      # @return [User]
      attr_reader :user

      # @return [Array<RoleAndOrigins>]
      attr_reader :effective_roles

      # @return [Time]
      attr_reader :password_changed

      # @return [Array<String>]
      attr_reader :external_groups

      def initialize
        yield self if block_given?
      end
    end

    class Group
      # @return [String]
      attr_reader :name

      # @return [String]
      attr_reader :description

      # @return [Array<Role>]
      attr_reader :roles

      # @return [String]
      attr_reader :ldap_group_reference

      def initialize
        yield self if block_given?
      end
    end
  end
end
