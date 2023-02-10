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

require_relative "test_helper"

module Couchbase
  class UserManagerTest < Minitest::Test
    include TestUtilities

    def setup
      connect
      @test_username = "test_user"
      @test_password = "a_password"
      @test_user = Management::User.new do |u|
        u.username = @test_username
        u.password = @test_password
        u.roles = [
          Management::Role.new do |r|
            r.name = "data_reader"
            r.bucket = "*"
          end,
          Management::Role.new do |r|
            r.name = "query_select"
            r.bucket = "*"
          end,
          Management::Role.new do |r|
            r.name = "data_writer"
            r.bucket = "*"
          end,
          Management::Role.new do |r|
            r.name = "query_insert"
            r.bucket = "*"
          end,
          Management::Role.new do |r|
            r.name = "query_delete"
            r.bucket = "*"
          end,
          Management::Role.new do |r|
            r.name = "query_manage_index"
            r.bucket = "*"
          end,
        ]
      end
      @cluster.users.upsert_user(@test_user)

      # Ensure that the user has been created
      deadline = Time.now + 10
      while Time.now < deadline
        begin
          @cluster.users.get_user(@test_username)
        rescue Error::UserNotFound
          sleep(0.01)
          next
        end
        sleep(0.1)
        return
      end
      raise "User could not be created"
    end

    def teardown
      connect
      @cluster.users.drop_user(@test_username)
      disconnect
    end

    def test_change_password
      skip("#{name}: CAVES does not support change_password") if use_caves?

      # Connect to the cluster with the test user
      orig_options = Cluster::ClusterOptions.new
      orig_options.authenticate(@test_username, @test_password)
      deadline = Time.now + 10
      success = false
      while Time.now < deadline
        begin
          @cluster = Cluster.connect(@env.connection_string, orig_options)
          success = true
          break
        rescue Error::AuthenticationFailure
          sleep(0.1)
        end
      end

      raise "Could not connect to the cluster using the newly created user" unless success

      # Change the test user's password
      new_password = "a_new_password"
      @cluster.users.change_password(new_password)

      # Verify that the connection succeeds with the new password
      new_options = Cluster::ClusterOptions.new
      new_options.authenticate(@test_username, new_password)
      deadline = Time.now + 10
      success = false
      while Time.now < deadline
        begin
          @cluster = Cluster.connect(@env.connection_string, new_options)
          success = true
          break
        rescue Error::AuthenticationFailure
          sleep(0.1)
        end
      end

      raise "Could not connect to the cluster using the new password" unless success

      # Verify that the connection fails with the old password
      assert_raises(Error::AuthenticationFailure) { Cluster.connect(@env.connection_string, orig_options) }
    end
  end
end
