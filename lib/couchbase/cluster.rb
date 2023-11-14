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

require "couchbase/configuration"
require "couchbase/authenticator"
require "couchbase/bucket"
require "couchbase/cluster_registry"

require "couchbase/management"
require "couchbase/options"

require "couchbase/search_options"
require "couchbase/query_options"
require "couchbase/analytics_options"
require "couchbase/diagnostics"

require "couchbase/protostellar"

module Couchbase
  # The main entry point when connecting to a Couchbase cluster.
  class Cluster
    alias inspect to_s

    # Connect to the Couchbase cluster
    #
    # @overload connect(connection_string_or_config, options)
    #   @param [String, Configuration] connection_string_or_config connection string used to locate the Couchbase Cluster
    #   @param [Options::Cluster] options custom options when creating the cluster connection
    #
    # @overload connect(connection_string_or_config, username, password, options)
    #   Shortcut for {PasswordAuthenticator}
    #   @param [String] connection_string_or_config connection string used to locate the Couchbase Cluster
    #   @param [String] username name of the user
    #   @param [String] password password of the user
    #   @param [Options::Cluster, nil] options custom options when creating the cluster connection
    #
    # @example Explicitly create options object and initialize PasswordAuthenticator internally
    #   options = Cluster::ClusterOptions.new
    #   options.authenticate("Administrator", "password")
    #   Cluster.connect("couchbase://localhost", options)
    #
    # @example Pass authenticator object to Options
    #   Cluster.connect("couchbase://localhost",
    #     Options::Cluster(authenticator: PasswordAuthenticator.new("Administrator", "password")))
    #
    # @example Shorter version, more useful for interactive sessions
    #   Cluster.connect("couchbase://localhost", "Administrator", "password")
    #
    # @example Authentication with TLS client certificate (note +couchbases://+ schema)
    #   Cluster.connect("couchbases://localhost?trust_certificate=/tmp/ca.pem",
    #     Options::Cluster(authenticator: CertificateAuthenticator.new("/tmp/certificate.pem", "/tmp/private.key")))
    #
    # @see https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html
    #
    # @return [Cluster]
    def self.connect(connection_string_or_config, *options)
      connection_string = if connection_string_or_config.is_a?(Configuration)
                            connection_string_or_config.connection_string
                          else
                            connection_string_or_config
                          end
      if connection_string =~ /\Acouchbases?:\/\/.*\z/i || !connection_string.include?("://")
        Cluster.new(connection_string_or_config, *options)
      else
        ClusterRegistry.instance.connect(connection_string_or_config, *options)
      end
    end

    # Returns an instance of the {Bucket}
    #
    # @param [String] name name of the bucket
    #
    # @return [Bucket]
    def bucket(name)
      Bucket.new(@backend, name)
    end

    # Performs a query against the query (N1QL) services
    #
    # @param [String] statement the N1QL query statement
    # @param [Options::Query] options the custom options for this query
    #
    # @example Select first ten hotels from travel sample dataset
    #   cluster.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10",
    #                 Options::Query(named_parameters: {type: "hotel"}, metrics: true))
    #
    # @example Execute query with consistency requirement. Make sure that the index is in sync with selected mutation
    #   res = collection.upsert("user:42", {
    #     "name" => "Brass Doorknob",
    #     "email" => "brass.doorknob@example.com",
    #   })
    #   cluster.query("SELECT name, email FROM `mybucket`",
    #                 Options::Query(consistent_with: MutationState.new(res.mutation_token)))
    #
    # @return [QueryResult]
    def query(statement, options = Options::Query::DEFAULT)
      resp = @backend.document_query(statement, options.to_backend)

      QueryResult.new do |res|
        res.meta_data = QueryMetaData.new do |meta|
          meta.status = resp[:meta][:status]
          meta.request_id = resp[:meta][:request_id]
          meta.client_context_id = resp[:meta][:client_context_id]
          meta.signature = JSON.parse(resp[:meta][:signature]) if resp[:meta][:signature]
          meta.profile = JSON.parse(resp[:meta][:profile]) if resp[:meta][:profile]
          meta.metrics = QueryMetrics.new do |metrics|
            if resp[:meta][:metrics]
              metrics.elapsed_time = resp[:meta][:metrics][:elapsed_time]
              metrics.execution_time = resp[:meta][:metrics][:execution_time]
              metrics.sort_count = resp[:meta][:metrics][:sort_count]
              metrics.result_count = resp[:meta][:metrics][:result_count]
              metrics.result_size = resp[:meta][:metrics][:result_size]
              metrics.mutation_count = resp[:meta][:metrics][:mutation_count]
              metrics.error_count = resp[:meta][:metrics][:error_count]
              metrics.warning_count = resp[:meta][:metrics][:warning_count]
            end
          end
          meta.warnings = resp[:warnings].map { |warn| QueryWarning.new(warn[:code], warn[:message]) } if resp[:warnings]
        end
        res.instance_variable_set(:@rows, resp[:rows])
      end
    end

    # Performs an analytics query
    #
    # @param [String] statement the N1QL query statement
    # @param [Options::Analytics] options the custom options for this query
    #
    # @example Select name of the given user
    #   cluster.analytics_query("SELECT u.name AS uname FROM GleambookUsers u WHERE u.id = $user_id ",
    #                           Options::Analytics(named_parameters: {user_id: 2}))
    #
    # @return [AnalyticsResult]
    def analytics_query(statement, options = Options::Analytics::DEFAULT)
      resp = @backend.document_analytics(statement, options.to_backend)

      AnalyticsResult.new do |res|
        res.transcoder = options.transcoder
        res.meta_data = AnalyticsMetaData.new do |meta|
          meta.status = resp[:meta][:status]
          meta.request_id = resp[:meta][:request_id]
          meta.client_context_id = resp[:meta][:client_context_id]
          meta.signature = JSON.parse(resp[:meta][:signature]) if resp[:meta][:signature]
          meta.profile = JSON.parse(resp[:meta][:profile]) if resp[:meta][:profile]
          meta.metrics = AnalyticsMetrics.new do |metrics|
            if resp[:meta][:metrics]
              metrics.elapsed_time = resp[:meta][:metrics][:elapsed_time]
              metrics.execution_time = resp[:meta][:metrics][:execution_time]
              metrics.result_count = resp[:meta][:metrics][:result_count]
              metrics.result_size = resp[:meta][:metrics][:result_size]
              metrics.error_count = resp[:meta][:metrics][:error_count]
              metrics.warning_count = resp[:meta][:metrics][:warning_count]
              metrics.processed_objects = resp[:meta][:metrics][:processed_objects]
            end
          end
          res[:warnings] = resp[:warnings].map { |warn| AnalyticsWarning.new(warn[:code], warn[:message]) } if resp[:warnings]
        end
        res.instance_variable_set(:@rows, resp[:rows])
      end
    end

    # Performs a Full Text Search (FTS) query
    #
    # @param [String] index_name the name of the search index
    # @param [SearchQuery] query the query tree
    # @param [Options::Search] options the query tree
    #
    # @example Return first 10 results of "hop beer" query and request highlighting
    #   cluster.search_query("beer_index", Cluster::SearchQuery.match_phrase("hop beer"),
    #                        Options::Search(
    #                          limit: 10,
    #                          fields: %w[name],
    #                          highlight_style: :html,
    #                          highlight_fields: %w[name description]
    #                        ))
    #
    # @return [SearchResult]
    def search_query(index_name, query, options = Options::Search::DEFAULT)
      resp = @backend.document_search(index_name, JSON.generate(query), options.to_backend)

      SearchResult.new do |res|
        res.meta_data = SearchMetaData.new do |meta|
          meta.metrics.max_score = resp[:meta_data][:metrics][:max_score]
          meta.metrics.error_partition_count = resp[:meta_data][:metrics][:error_partition_count]
          meta.metrics.success_partition_count = resp[:meta_data][:metrics][:success_partition_count]
          meta.metrics.took = resp[:meta_data][:metrics][:took]
          meta.metrics.total_rows = resp[:meta_data][:metrics][:total_rows]
          meta.errors = resp[:meta_data][:errors]
        end
        res.rows = resp[:rows].map do |r|
          SearchRow.new do |row|
            row.transcoder = options.transcoder
            row.index = r[:index]
            row.id = r[:id]
            row.score = r[:score]
            row.fragments = r[:fragments]
            unless r[:locations].empty?
              row.locations = SearchRowLocations.new(
                r[:locations].map do |loc|
                  SearchRowLocation.new do |location|
                    location.field = loc[:field]
                    location.term = loc[:term]
                    location.position = loc[:position]
                    location.start_offset = loc[:start_offset]
                    location.end_offset = loc[:end_offset]
                    location.array_positions = loc[:array_positions]
                  end
                end
              )
            end
            row.instance_variable_set(:@fields, r[:fields])
            row.explanation = JSON.parse(r[:explanation]) if r[:explanation]
          end
        end
        if resp[:facets]
          res.facets = resp[:facets].each_with_object({}) do |(k, v), o|
            facet = case options.facets[k]
                    when SearchFacet::SearchFacetTerm
                      SearchFacetResult::TermFacetResult.new do |f|
                        f.terms =
                          if v[:terms]
                            v[:terms].map do |t|
                              SearchFacetResult::TermFacetResult::TermFacet.new(t[:term], t[:count])
                            end
                          else
                            []
                          end
                      end
                    when SearchFacet::SearchFacetDateRange
                      SearchFacetResult::DateRangeFacetResult.new do |f|
                        f.date_ranges =
                          if v[:date_ranges]
                            v[:date_ranges].map do |r|
                              SearchFacetResult::DateRangeFacetResult::DateRangeFacet.new(r[:name], r[:count], r[:start_time], r[:end_time])
                            end
                          else
                            []
                          end
                      end
                    when SearchFacet::SearchFacetNumericRange
                      SearchFacetResult::NumericRangeFacetResult.new do |f|
                        f.numeric_ranges =
                          if v[:numeric_ranges]
                            v[:numeric_ranges].map do |r|
                              SearchFacetResult::NumericRangeFacetResult::NumericRangeFacet.new(r[:name], r[:count], r[:min], r[:max])
                            end
                          else
                            []
                          end
                      end
                    else
                      next # ignore unknown facet result
                    end
            facet.name = v[:name]
            facet.field = v[:field]
            facet.total = v[:total]
            facet.missing = v[:missing]
            facet.other = v[:other]
            o[k] = facet
          end
        end
      end
    end

    # @return [Management::UserManager]
    def users
      Management::UserManager.new(@backend)
    end

    # @return [Management::BucketManager]
    def buckets
      Management::BucketManager.new(@backend)
    end

    # @return [Management::QueryIndexManager]
    def query_indexes
      Management::QueryIndexManager.new(@backend)
    end

    # @return [Management::AnalyticsIndexManager]
    def analytics_indexes
      Management::AnalyticsIndexManager.new(@backend)
    end

    # @return [Management::SearchIndexManager]
    def search_indexes
      Management::SearchIndexManager.new(@backend)
    end

    # Closes all connections to services and free allocated resources
    #
    # @return [void]
    def disconnect
      @backend.close
    end

    # Creates diagnostic report that can be used to determine the health of the network connections.
    #
    # It does not proactively perform any I/O against the network
    #
    # @param [Options::Diagnostics] options
    #
    # @return [DiagnosticsResult]
    def diagnostics(options = Options::Diagnostics::DEFAULT)
      resp = @backend.diagnostics(options.report_id)
      DiagnosticsResult.new do |res|
        res.version = resp[:version]
        res.id = resp[:id]
        res.sdk = resp[:sdk]
        resp[:services].each do |type, svcs|
          res.services[type] = svcs.map do |svc|
            DiagnosticsResult::ServiceInfo.new do |info|
              info.id = svc[:id]
              info.state = svc[:state]
              info.last_activity_us = svc[:last_activity_us]
              info.remote = svc[:remote]
              info.local = svc[:local]
              info.details = svc[:details]
            end
          end
        end
      end
    end

    # Performs application-level ping requests against services in the couchbase cluster
    #
    # @param [Options::Ping] options
    #
    # @return [PingResult]
    def ping(options = Options::Ping::DEFAULT)
      resp = @backend.ping(nil, options.to_backend)
      PingResult.new do |res|
        res.version = resp[:version]
        res.id = resp[:id]
        res.sdk = resp[:sdk]
        resp[:services].each do |type, svcs|
          res.services[type] = svcs.map do |svc|
            PingResult::ServiceInfo.new do |info|
              info.id = svc[:id]
              info.state = svc[:state]
              info.latency = svc[:latency]
              info.remote = svc[:remote]
              info.local = svc[:local]
              info.error = svc[:error]
            end
          end
        end
      end
    end

    private

    # Initialize {Cluster} object
    #
    # @overload new(connection_string, options)
    #   @param [String] connection_string connection string used to locate the Couchbase Cluster
    #   @param [Options::Cluster] options custom options when creating the cluster connection
    #
    # @overload new(connection_string, username, password, options)
    #   Shortcut for {PasswordAuthenticator}
    #   @param [String] connection_string connection string used to locate the Couchbase Cluster
    #   @param [String] username name of the user
    #   @param [String] password password of the user
    #   @param [Options::Cluster, nil] options custom options when creating the cluster connection
    #
    # @overload new(configuration)
    #   @param [Configuration] configuration configuration object
    def initialize(connection_string, *args)
      credentials = {}
      open_options = {}

      if connection_string.is_a?(Configuration)
        options = connection_string
        connection_string = options.connection_string
        credentials[:username] = options.username
        credentials[:password] = options.password
        raise ArgumentError, "missing connection_string" unless connection_string
        raise ArgumentError, "missing username" unless credentials[:username]
        raise ArgumentError, "missing password" unless credentials[:password]
      else
        options = args.shift
        case options
        when String
          credentials[:username] = options
          credentials[:password] = args.shift
          raise ArgumentError, "missing username" unless credentials[:username]
          raise ArgumentError, "missing password" unless credentials[:password]
        when Options::Cluster
          open_options = options&.to_backend || {}
          authenticator = options&.authenticator
          case authenticator
          when PasswordAuthenticator
            credentials[:username] = authenticator&.username
            raise ArgumentError, "missing username" unless credentials[:username]

            credentials[:password] = authenticator&.password
            raise ArgumentError, "missing password" unless credentials[:password]

            open_options[:allowed_sasl_mechanisms] = authenticator&.allowed_sasl_mechanisms
          when CertificateAuthenticator
            credentials[:certificate_path] = authenticator&.certificate_path
            raise ArgumentError, "missing certificate path" unless credentials[:certificate_path]

            credentials[:key_path] = authenticator&.key_path
            raise ArgumentError, "missing key path" unless credentials[:key_path]

          else
            raise ArgumentError, "options must have authenticator configured"
          end
        else
          raise ArgumentError, "unexpected second argument, have to be String or ClusterOptions"
        end
      end

      @backend = Backend.new
      @backend.open(connection_string, credentials, open_options)
    end

    # @api private
    ClusterOptions = ::Couchbase::Options::Cluster
    # @api private
    DiagnosticsOptions = ::Couchbase::Options::Diagnostics
    # @api private
    AnalyticsOptions = ::Couchbase::Options::Analytics
    # @api private
    QueryOptions = ::Couchbase::Options::Query
    # @api private
    SearchOptions = ::Couchbase::Options::Search
  end
end
