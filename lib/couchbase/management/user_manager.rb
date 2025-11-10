# frozen_string_literal: true

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
require "couchbase/options"

require "time"

module Couchbase
  module Management
    module Options
      module User
        # Options for {UserManager#get_user}
        class GetUser < ::Couchbase::Options::Base
          # @return [:local, :external] Name of the user's domain. Defaults to +:local+
          attr_accessor :domain

          # Creates an instance of options for {UserManager#get_user}
          # @param [:local, :external] domain the name of the user's domain. Defaults to +:local+
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetUser] self
          def initialize(domain: :local,
                         timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, parent_span: parent_span)
            @domain = domain
            yield self if block_given?
          end

          # @api private
          DEFAULT = GetUser.new.freeze
        end

        # Options for {UserManager#get_all_users}
        class GetAllUsers < ::Couchbase::Options::Base
          # @return [:local, :external] Name of the user's domain. Defaults to +:local+
          attr_accessor :domain

          # Creates an instance of options for {UserManager#get_all_users}
          # @param [:local, :external] the name of the user's domain. Defaults to +:local+
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllUsers] self
          def initialize(domain: :local,
                         timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, parent_span: parent_span)
            @domain = domain
            yield self if block_given?
          end
        end

        # Options for {UserManager#upsert_user}
        class UpsertUser < ::Couchbase::Options::Base
          # @return [:local, :external] Name of the user's domain. Defaults to +:local+
          attr_accessor :domain

          # Creates an instance of options for {UserManager#upsert_user}
          # @param [:local, :external] domain the name of the user's domain. Defaults to +:local+
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UpsertUser] self
          def initialize(domain: :local,
                         timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, parent_span: parent_span)
            @domain = domain
            yield self if block_given?
          end

          # @api private
          DEFAULT = UpsertUser.new.freeze
        end

        # Options for {UserManager#drop_user}
        class DropUser < ::Couchbase::Options::Base
          # @return [:local, :external] Name of the user's domain. Defaults to +:local+
          attr_accessor :domain

          # Creates an instance of options for {UserManager#drop_user}
          # @param [:local, :external] domain the name of the user's domain. Defaults to +:local+
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropUser] self
          def initialize(domain: :local,
                         timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, parent_span: parent_span)
            @domain = domain
            yield self if block_given?
          end

          # @api private
          DEFAULT = DropUser.new.freeze
        end

        # Options for {UserManager#change_password}
        class ChangePassword < ::Couchbase::Options::Base
          # Creates an instance of options for {UserManager#change_password}
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [ChangePassword] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = ChangePassword.new.freeze
        end

        # Options for {UserManager#get_roles}
        class GetRoles < ::Couchbase::Options::Base
          # Creates an instance of options for {UserManager#get_roles}
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetRoles] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = GetRoles.new.freeze
        end

        # Options for {UserManager#get_group}
        class GetGroup < ::Couchbase::Options::Base
          # Creates an instance of options for {UserManager#get_group}
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetGroup] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = GetGroup.new.freeze
        end

        # Options for {UserManager#get_all_groups}
        class GetAllGroups < ::Couchbase::Options::Base
          # Creates an instance of options for {UserManager#get_all_groups}
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllGroups] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = GetAllGroups.new.freeze
        end

        # Options for {UserManager#upsert_group}
        class UpsertGroup < ::Couchbase::Options::Base
          # Creates an instance of options for {UserManager#upsert_group}
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UpsertGroup] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = UpsertGroup.new.freeze
        end

        # Options for {UserManager#drop_group}
        class DropGroup < ::Couchbase::Options::Base
          # Creates an instance of options for {UserManager#drop_group}
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropGroup] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = DropGroup.new.freeze
        end
      end
    end

    class UserManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Get a user
      #
      # @param [String] username ID of the user
      # @param [Options::User::GetUser] options
      #
      # @return [UserAndMetadata]
      #
      # @raise [ArgumentError]
      # @raise [Error::UserNotFound]
      def get_user(username, options = Options::User::GetUser::DEFAULT)
        resp = @backend.user_get(options.domain, username, options.timeout)
        extract_user(resp)
      end

      # Gets all users
      #
      # @param [Options::User::GetAllUsers] options
      #
      # @return [Array<UserAndMetadata>]
      def get_all_users(options = Options::User::GetAllUsers::DEFAULT)
        resp = @backend.user_get_all(options.domain, options.timeout)
        resp.map { |entry| extract_user(entry) }
      end

      # Creates or updates a user
      #
      # @param [User] user the new version of the user
      # @param [Options::User::UpsertUser] options
      #
      # @raise [ArgumentError]
      def upsert_user(user, options = Options::User::UpsertUser::DEFAULT)
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
      # @param [Options::User::DropUser] options
      def drop_user(username, options = Options::User::DropUser::DEFAULT)
        @backend.user_drop(options.domain, username, options.timeout)
      end

      # Gets all roles supported by the server
      #
      # @param [Options::User::GetRoles] options
      #
      # @return [Array<RoleAndDescription>]
      def get_roles(options = Options::User::GetRoles::DEFAULT)
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

      # Changes the password of the currently authenticated user
      #
      # @param [Options::User::ChangePassword] options
      #
      # @raise [ArgumentError]
      def change_password(new_password, options = Options::User::ChangePassword::DEFAULT)
        @backend.change_password(new_password, options.timeout)
      end

      # Gets a group
      #
      # @param [String] group_name name of the group to get
      # @param [Options::User::GetGroup] options
      #
      # @return [Group]
      #
      # @raise [ArgumentError]
      # @raise [Error::GroupNotFound]
      def get_group(group_name, options = Options::User::GetGroup::DEFAULT)
        resp = @backend.group_get(group_name, options.timeout)
        extract_group(resp)
      end

      # Gets all groups
      #
      # @param [Options::User::GetAllGroups] options
      #
      # @return [Array<Group>]
      def get_all_groups(options = Options::User::GetAllGroups::DEFAULT)
        resp = @backend.group_get_all(options.timeout)
        resp.map { |entry| extract_group(entry) }
      end

      # Creates or updates a group
      #
      # @param [Group] group the new version of the group
      # @param [Options::User::UpsertGroup] options
      #
      # @raise [ArgumentError]
      # @raise [Error::GroupNotFound]
      def upsert_group(group, options = Options::User::UpsertGroup::DEFAULT)
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
      # @param [Options::User::DropGroup] options
      #
      # @raise [Error::GroupNotFound]
      def drop_group(group_name, options = Options::User::DropGroup::DEFAULT)
        @backend.group_drop(group_name, options.timeout)
      end

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::GetUser} instead
      GetUserOptions = ::Couchbase::Management::Options::User::GetUser

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::GetAllUsers} instead
      GetAllUsersOptions = ::Couchbase::Management::Options::User::GetAllUsers

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::UpsertUser} instead
      UpsertUserOptions = ::Couchbase::Management::Options::User::UpsertUser

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::DropUser} instead
      DropUserOptions = ::Couchbase::Management::Options::User::DropUser

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::ChangePassword} instead
      ChangePasswordOptions = ::Couchbase::Management::Options::User::ChangePassword

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::GetRoles} instead
      GetRolesOptions = ::Couchbase::Management::Options::User::GetRoles

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::GetGroup} instead
      GetGroupOptions = ::Couchbase::Management::Options::User::GetGroup

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::GetAllGroups} instead
      GetAllGroupsOptions = ::Couchbase::Management::Options::User::GetAllGroups

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::UpsertGroup} instead
      UpsertGroupOptions = ::Couchbase::Management::Options::User::UpsertGroup

      # @api private
      # @deprecated Use {Couchbase::Management::Options::User::DropGroup} instead
      DropGroupOptions = ::Couchbase::Management::Options::User::DropGroup

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
