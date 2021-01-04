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

require "couchbase"

module ActiveSupport
  module Cache
    # A cache store implementation which stores data in Couchbase: https://couchbase.com
    #
    # * Local cache. Hot in-memory primary cache within block/middleware scope.
    # * +read_multi+ and +write_multi+ support.
    # * +delete_matched+ support using N1QL queries.
    # * +clear+ for erasing whole collection (optionally can flush the bucket).
    #
    # To use this store, add the select it in application config
    #
    #     config.cache_store = :couchbase_store, {
    #       connection_string: "couchbase://localhost",
    #       username: "app_cache_user",
    #       password: "s3cret",
    #       bucket: "app_cache"
    #     }
    #
    # @see https://guides.rubyonrails.org/caching_with_rails.html#cache-stores
    class CouchbaseStore < Store
      MAX_KEY_BYTESIZE = 250
      DEFAULT_ERROR_HANDLER = lambda do |method:, returning:, exception:, logger: CouchbaseStore.logger|
        logger&.error { "CouchbaseStore: #{method} failed, returned #{returning.inspect}: #{exception.class}: #{exception.message}" }
      end

      # Advertise cache versioning support.
      def self.supports_cache_versioning?
        true
      end

      module LocalCacheWithRaw # :nodoc:
        private

        def write_entry(key, entry, **options)
          if options[:raw] && local_cache
            raw_entry = Entry.new(entry.value.to_s)
            raw_entry.expires_at = entry.expires_at
            super(key, raw_entry, **options)
          else
            super
          end
        end

        def write_multi_entries(entries, **options)
          if options[:raw] && local_cache
            raw_entries = entries.map do |_key, entry|
              raw_entry = Entry.new(serialize_entry(entry, raw: true))
              raw_entry.expires_at = entry.expires_at
            end.to_h

            super(raw_entries, **options)
          else
            super
          end
        end
      end

      prepend Strategy::LocalCache
      prepend LocalCacheWithRaw

      def initialize(options = nil)
        super
        @error_handler = options.delete(:error_handler) { DEFAULT_ERROR_HANDLER }
        @couchbase_options = {}
        @couchbase_options[:connection_string] =
          @options.delete(:connection_string) do
            raise ArgumentError, "Missing connection string for Couchbase cache store. Use :connection_string in the store options"
          end
        @couchbase_options[:username] =
          @options.delete(:username) do
            raise ArgumentError, "Missing username for Couchbase cache store. Use :username in the store options"
          end
        @couchbase_options[:password] =
          @options.delete(:password) do
            raise ArgumentError, "Missing password for Couchbase cache store. Use :password in the store options"
          end
        @couchbase_options[:bucket] =
          @options.delete(:bucket) { raise ArgumentError, "Missing bucket for Couchbase cache store. Use :bucket in the store options" }
        @couchbase_options[:scope] = @options.delete(:scope) if @options.key?(:scope)
        @couchbase_options[:collection] = @options.delete(:collection) if @options.key?(:collection)
        @last_mutation_token = nil
      end

      def collection
        @collection ||= build_collection
      end

      def cluster
        @cluster ||= build_cluster
      end

      def inspect
        "#<#{self.class} options=#{options.inspect} collection=#{@collection.inspect}>"
      end

      # Deletes all entries with keys matching the regular expression.
      #
      # The +matcher+ must be valid pattern for N1QL +REGEXP_MATCHES+ function. More info at
      # https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/patternmatchingfun.html#section_regex_matches
      #
      # Because the operation performed on query engine, and it might take time to propagate changes
      # from key/value engine to the indexer. Therefore the keys, that were created a moment ago
      # might not be deleted.
      #
      # Also this method assumes, that primary index created on the target bucket
      def delete_matched(matcher, _options = nil)
        pattern =
          case matcher
          when Regexp
            matcher.inspect[1..-2]
          when String
            matcher.tr("?", ".").gsub("*", ".*")
          else
            raise NotImplementedError, "Unable to convert #{matcher.inspect} to Regexp pattern"
          end
        operation_options = ::Couchbase::Options::Query(named_parameters: {"pattern" => pattern})
        operation_options.consistent_with(::Couchbase::MutationState.new(@last_mutation_token)) if @last_mutation_token
        begin
          cluster.query("DELETE FROM #{scope_qualifier} cache WHERE REGEXP_MATCHES(META(cache).id, $pattern)", operation_options)
        rescue ::Couchbase::Error::ParsingFailure
          raise NotImplementedError, "The server does not support delete_matched operation"
        end
      end

      # Increments an integer value in the cache.
      #
      # Note that it uses binary collection interface, therefore will fail if the value is not
      # represented as ASCII-encoded number using +:raw+.
      def increment(name, amount = 1, expires_in: nil, initial: nil, **options)
        instrument :increment, name, amount: amount do
          failsafe :increment do
            key = normalize_key(name, options)
            res = collection.binary.increment(
              key, ::Couchbase::Options::Increment(delta: amount, expiry: expires_in, initial: initial)
            )
            @last_mutation_token = res.mutation_token
            res.content
          end
        end
      end

      # Decrements an integer value in the cache.
      #
      # Note that it uses binary collection interface, therefore will fail if the value is not
      # represented as ASCII-encoded number using +:raw+.
      #
      # Note that the counter represented by non-negative number on the server, and it will not
      # cycle to maximum integer when decrementing zero.
      def decrement(name, amount = 1, expires_in: nil, initial: nil, **_options)
        instrument :decrement, name, amount: amount do
          failsafe :decrement do
            key = normalize_key(name, options)
            res = collection.binary.decrement(
              key, ::Couchbase::Options::Decrement(delta: amount, expiry: expires_in, initial: initial)
            )
            @last_mutation_token = res.mutation_token
            res.content
          end
        end
      end

      # Clears the entire cache. Be careful with this method since it could
      # affect other processes if shared cache is being used.
      #
      # When +use_flush+ option set to +true+ it will flush the bucket. Otherwise, it uses N1QL query
      # and relies on default index.
      def clear(use_flush: false, **_options)
        failsafe(:clear) do
          if use_flush
            cluster.buckets.flush_bucket(@couchbase_options[:bucket_name])
          else
            operation_options = ::Couchbase::Options::Query.new
            operation_options.consistent_with(::Couchbase::MutationState.new(@last_mutation_token)) if @last_mutation_token
            cluster.query("DELETE FROM #{scope_qualifier}", operation_options)
          end
        end
      end

      private

      def deserialize_entry(payload, raw:)
        if payload && raw
          Entry.new(payload, compress: false)
        else
          super(payload)
        end
      end

      def serialize_entry(entry, raw: false)
        if raw
          entry.value.to_s
        else
          super(entry)
        end
      end

      # Reads an entry from the cache
      def read_entry(key, **options)
        failsafe(:read_entry, returning: nil) do
          result = collection.get(key, ::Couchbase::Options::Get(transcoder: nil))
          deserialize_entry(result.content, raw: options&.fetch(:raw, false))
        end
      end

      # Reads multiple entries from the cache implementation. Subclasses MAY
      # implement this method.
      def read_multi_entries(names, **options)
        return {} if names.empty?

        keys = names.map { |name| normalize_key(name, options) }
        return_value = {}
        failsafe(:read_multi_entries, returning: return_value) do
          results = collection.get_multi(keys, ::Couchbase::Options::GetMulti(transcoder: nil))
          results.each_with_index do |result, index|
            next unless result.success?

            entry = deserialize_entry(result.content, raw: options[:raw])
            unless entry.nil? || entry.expired? || entry.mismatched?(normalize_version(names[index], options))
              return_value[names[index]] = entry.value
            end
          end
          return_value
        end
      end

      # Writes an entry to the cache
      def write_entry(key, entry, raw: false, expires_in: nil, race_condition_ttl: nil, **_options)
        if race_condition_ttl && expires_in && expires_in.positive? && !raw
          # Add few minutes to expiry in the future to support race condition TTLs on read
          expires_in += 5.minutes
        end
        failsafe(:write_entry, returning: false) do
          res = collection.upsert(key, serialize_entry(entry, raw: raw),
                                  ::Couchbase::Options::Upsert(transcoder: nil, expiry: expires_in))
          @last_mutation_token = res.mutation_token
          true
        end
      end

      def write_multi_entries(hash, **options)
        return 0 if hash.empty?

        return super if local_cache

        expires_in = options[:expires_in]
        if options[:race_condition_ttl] && expires_in && expires_in.positive?
          # Add few minutes to expiry in the future to support race condition TTLs on read
          expires_in += 5.minutes
        end
        operation_options = ::Couchbase::Options::UpsertMulti(transcoder: nil, expiry: expires_in)

        pairs = hash.map do |key, entry|
          [key, options[:raw] ? entry.value.to_s : serialize_entry(entry)]
        end
        failsafe(:write_multi_entries, returning: nil) do
          successful = collection.upsert_multi(pairs, operation_options).select(&:success?)
          return 0 if successful.empty?

          @last_mutation_token = successful.max_by { |r| r.mutation_token.sequence_number }
          successful.count
        end
      end

      # Deletes an entry from the cache implementation. Subclasses must
      # implement this method.
      def delete_entry(key, **_options)
        failsafe(:delete_entry, returning: false) do
          res = collection.remove(key)
          @last_mutation_token = res.mutation_token
          true
        end
      end

      # Deletes multiple entries in the cache. Returns the number of entries deleted.
      def delete_multi_entries(entries, **_options)
        return if entries.empty?

        failsafe(:delete_multi_entries, returning: nil) do
          successful = collection.remove_multi(entries).select(&:success?)
          return 0 if successful.empty?

          @last_mutation_token = successful.max_by { |r| r.mutation_token.sequence_number }
          successful.count
        end
      end

      def failsafe(method, returning: nil)
        yield
      rescue ::Couchbase::Error::CouchbaseError => e
        handle_exception(exception: e, method: method, returning: returning)
        returning
      end

      def handle_exception(exception:, method:, returning:)
        return unless @error_handler

        @error_handler.call(method: method, exception: exception, returning: returning)
      rescue StandardError => e
        warn "CouchbaseStore ignored exception in handle_exception: #{e.class}: #{e.message}\n  #{e.backtrace.join("\n  ")}"
      end

      # Truncate keys that exceed 250 characters
      def normalize_key(key, options)
        truncate_key super&.b
      end

      def truncate_key(key)
        if key && key.bytesize > MAX_KEY_BYTESIZE
          suffix = ":sha2:#{::Digest::SHA2.hexdigest(key)}"
          truncate_at = MAX_KEY_BYTESIZE - suffix.bytesize
          "#{key.byteslice(0, truncate_at)}#{suffix}"
        else
          key
        end
      end

      # Connects to the Couchbase cluster
      def build_cluster
        ::Couchbase::Cluster.connect(
          @couchbase_options[:connection_string],
          ::Couchbase::Options::Cluster(authenticator: ::Couchbase::PasswordAuthenticator.new(
            @couchbase_options[:username], @couchbase_options[:password]
          ))
        )
      end

      # Connects to the Couchbase cluster, opens specified bucket and returns collection object.
      def build_collection
        bucket = cluster.bucket(@couchbase_options[:bucket])
        if @couchbase_options[:scope] && @couchbase_options[:collection]
          bucket.scope(@couchbase_options[:scope]).collection(@couchbase_options[:collection])
        else
          bucket.default_collection
        end
      end

      def scope_qualifier
        if @couchbase_options[:scope] && @couchbase_options[:collection]
          "`#{@couchbase_options[:bucket]}`.#{@couchbase_options[:scope]}.#{@couchbase_options[:collection]}"
        else
          "`#{@couchbase_options[:bucket]}`"
        end
      end
    end
  end
end
