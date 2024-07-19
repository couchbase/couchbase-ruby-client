# frozen_string_literal: true

#  Copyright 2024. Couchbase, Inc.
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

require 'net/http'
require 'json'

module Couchbase
  module TestUtilities
    class ConsistencyHelper
      attr_reader :management_hosts

      RETRY_DELAY_SECS = 0.5
      DEFAULT_TIMEOUT_SECS = 10

      def initialize(management_endpoint, username, password)
        @username = username
        @password = password
        fetch_hosts(management_endpoint)
      end

      def fetch_hosts(management_endpoint, timeout: DEFAULT_TIMEOUT_SECS)
        @management_hosts = []

        # If no management endpoint is configured, run the tests without consistency checks
        return if management_endpoint.nil?

        deadline = Time.now + timeout
        while Time.now < deadline
          uri = URI("#{management_endpoint}/pools/nodes")
          req = Net::HTTP::Get.new(uri)
          req.basic_auth(@username, @password)
          resp = Net::HTTP.start(uri.hostname, uri.port) { |http| http.request(req) }

          # Retry if it was not possible to retrieve the cluster config - if the timeout is exceeded, consistency checks will be disabled
          next unless resp.code == "200"

          resp_body = JSON.parse(resp.body)
          resp_body["nodes"].each do |node|
            @management_hosts << node["configuredHostname"]
          end
          break
        end
      end

      def wait_until_bucket_present(name, timeout: DEFAULT_TIMEOUT_SECS)
        wait_until(timeout, "Bucket `#{name}` is not present in all nodes") do
          resource_satisfies_predicate("pools/default/buckets/#{name}") do |resp|
            resp["nodes"].all? { |node| node["status"] == "healthy" && node["clusterMembership"] == "active" }
          end
        end
      end

      def wait_until_bucket_dropped(name, timeout: DEFAULT_TIMEOUT_SECS)
        wait_until(timeout, "Bucket `#{name}` has not been dropped in all nodes") do
          resource_is_absent("pools/default/buckets/#{name}")
        end
      end

      def wait_until_scope_present(bucket_name, scope_name, timeout: DEFAULT_TIMEOUT_SECS)
        wait_until(timeout, "Scope `#{scope_name}` in bucket `#{bucket_name}` is not present in all nodes") do
          resource_satisfies_predicate("pools/default/buckets/#{bucket_name}/scopes") do |resp|
            resp["scopes"].any? do |scope|
              scope["name"] == scope_name
            end
          end
        end
      end

      def wait_until_scope_dropped(bucket_name, scope_name, timeout: DEFAULT_TIMEOUT_SECS)
        wait_until(timeout, "Scope `#{scope_name}` in bucket `#{bucket_name}` is not present in all nodes") do
          resource_satisfies_predicate("pools/default/buckets/#{bucket_name}/scopes") do |resp|
            resp["scopes"].none? do |scope|
              scope["name"] == scope_name
            end
          end
        end
      end

      def wait_until_collection_present(bucket_name, scope_name, collection_name, timeout: DEFAULT_TIMEOUT_SECS)
        wait_until(timeout,
                   "Collection `#{collection_name}` in scope `#{scope_name}` & bucket `#{bucket_name}` is not present in all nodes") do
          resource_satisfies_predicate("pools/default/buckets/#{bucket_name}/scopes") do |resp|
            resp["scopes"].any? do |scope|
              scope["name"] == scope_name && scope["collections"].any? { |c| c["name"] == collection_name }
            end
          end
        end
      end

      def wait_until_collection_dropped(bucket_name, scope_name, collection_name, timeout: DEFAULT_TIMEOUT_SECS)
        wait_until(timeout, "Scope `#{scope_name}` in bucket `#{bucket_name}` is not present in all nodes") do
          resource_satisfies_predicate("pools/default/buckets/#{bucket_name}/scopes") do |resp|
            resp["scopes"].none? do |scope|
              scope["name"] == scope_name && scope["collections"].any? { |c| c["name"] == collection_name }
            end
          end
        end
      end

      private

      def wait_until(timeout, error_msg)
        deadline = Time.now + timeout
        while Time.now < deadline
          return if yield

          sleep(RETRY_DELAY_SECS)
        end

        raise error_msg
      end

      def resource_is_present(path)
        @management_hosts.all? do |host|
          uri = URI("http://#{host}/#{path}")
          puts "Checking that resource is present at #{uri}"
          req = Net::HTTP::Get.new(uri)
          req.basic_auth(@username, @password)
          resp = Net::HTTP.start(uri.hostname, uri.port) { |http| http.request(req) }
          resp.code == "200"
        end
      end

      def resource_is_absent(path)
        @management_hosts.all? do |host|
          uri = URI("http://#{host}/#{path}")
          puts "Checking that resource is absent at #{uri}"
          req = Net::HTTP::Get.new(uri)
          req.basic_auth(@username, @password)
          resp = Net::HTTP.start(uri.hostname, uri.port) { |http| http.request(req) }
          resp.code == "404"
        end
      end

      def resource_satisfies_predicate(path, &predicate)
        @management_hosts.all? do |host|
          uri = URI("http://#{host}/#{path}")
          puts "Checking that resource at #{uri} satisfies the predicate defined at #{predicate.source_location.join(':')}"
          req = Net::HTTP::Get.new(uri)
          req.basic_auth(@username, @password)
          resp = Net::HTTP.start(uri.hostname, uri.port) { |http| http.request(req) }

          return false if resp.code != "200"

          yield(JSON.parse(resp.body))
        end
      end
    end
  end
end
