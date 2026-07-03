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

require "fit/performer/observability/otel"

require "couchbase/opentelemetry"

require "securerandom"
require "tmpdir"

module FIT
  module Performer
    class Connection
      attr_accessor :cluster
      attr_accessor :tracer

      def initialize(request)
        @logger = Logger.new($stdout)
        opts = create_cluster_options(request)
        conn_str = get_connection_string(request)
        @logger.info("Creating connection using connection string `#{conn_str}` (ID = #{request.cluster_connection_id})")
        @cluster = Couchbase::Cluster.connect(conn_str, opts)
      end

      def close
        @tracer_provider&.shutdown
        @meter_provider&.shutdown
        @cluster.disconnect

        return if @temp_dir.nil?

        FileUtils.remove_entry(@temp_dir)
      end

      def create_authenticator(proto_auth)
        case proto_auth.authenticator
        when :password_auth
          Couchbase::PasswordAuthenticator.new(
            proto_auth.password_auth.username,
            proto_auth.password_auth.password,
          )
        when :certificate_auth
          certificate_path = File.join(temp_dir, "#{SecureRandom.uuid}-cert.pem")
          File.write(certificate_path, proto_auth.certificate_auth.cert)
          key_path = File.join(temp_dir, "#{SecureRandom.uuid}-key.pem")
          File.write(key_path, proto_auth.certificate_auth.key)

          Couchbase::CertificateAuthenticator.new(
            certificate_path,
            key_path,
          )
        when :jwt_auth
          Couchbase::JWTAuthenticator.new(proto_auth.jwt_auth.jwt)
        else
          raise PerformerError, "Authenticator type `#{proto_auth.authenticator}` is not supported"
        end
      end

      private

      def temp_dir
        @temp_dir ||= Dir.mktmpdir
      end

      def get_duration(config, field, unit = :millis)
        has_method_name = :"has_#{field}?"
        return unless config.public_send(has_method_name)

        factor =
          case unit
          when :millis
            1
          when :secs
            1000
          else
            raise "Unknown duration unit `#{unit}`"
          end
        config.public_send(field) * factor
      end

      def create_application_telemetry_options(config)
        Couchbase::Options::Cluster::ApplicationTelemetry.new do |opts|
          opts.enable = config.enable_app_telemetry if config.has_enable_app_telemetry?
          opts.backoff = get_duration(config, :app_telemetry_backoff_secs, :secs)
          opts.ping_interval = get_duration(config, :app_telemetry_ping_interval_secs, :secs)
          opts.ping_timeout = get_duration(config, :app_telemetry_ping_timeout_secs, :secs)
          opts.override_endpoint = config.app_telemetry_endpoint if config.has_enable_app_telemetry?
        end
      end

      def create_tracer(tracing_config)
        @tracer_provider = Observability::Otel.create_tracer_provider(tracing_config)
        @tracer = Couchbase::OpenTelemetry::RequestTracer.new(@tracer_provider)
      end

      def create_meter(metrics_config)
        @meter_provider = Observability::Otel.create_meter_provider(metrics_config)
        @meter = Couchbase::OpenTelemetry::Meter.new(@meter_provider)
      end

      def create_cluster_options(request)
        cluster_options = {}
        cluster_options[:authenticator] = if request.has_authenticator?
                                            create_authenticator(request.authenticator)
                                          else
                                            Couchbase::PasswordAuthenticator.new(
                                              request.cluster_username,
                                              request.cluster_password,
                                            )
                                          end

        if request.has_cluster_config?
          config = request.cluster_config
          cluster_options.update(
            {
              key_value_timeout: get_duration(config, :kv_timeout_millis),
              view_timeout: get_duration(config, :view_timeout_secs, :secs),
              query_timeout: get_duration(config, :query_timeout_secs, :secs),
              analytics_timeout: get_duration(config, :analytics_timeout_secs, :secs),
              search_timeout: get_duration(config, :search_timeout_secs, :secs),
              management_timeout: get_duration(config, :management_timeout_secs, :secs),
              tcp_keep_alive_interval: get_duration(config, :tcp_keep_alive_time_millis),
              config_poll_interval: get_duration(config, :config_poll_interval_secs, :secs),
              config_poll_floor: get_duration(config, :config_poll_floor_interval_secs, :secs),
              config_idle_redial_timeout: get_duration(config, :config_idle_redial_timeout_secs, :secs),
              idle_http_connection_timeout: get_duration(config, :idle_http_connection_timeout_secs, :secs),
              preferred_server_group: config.has_preferred_server_group? ? config.preferred_server_group : nil,
              # [if:3.8.0]
              application_telemetry: create_application_telemetry_options(config),
              # [end]
            },
          )

          if config.has_observability_config?
            observability_config = config.observability_config

            if observability_config.use_noop_tracer
              cluster_options[:enable_tracing] = false
            elsif observability_config.has_tracing?
              cluster_options[:tracer] = create_tracer(observability_config.tracing)
            end

            cluster_options[:meter] = create_meter(observability_config.metrics) if observability_config.has_metrics?

            if observability_config.has_threshold_logging_tracer?
              threshold_config = observability_config.threshold_logging_tracer
              cluster_options.update(
                {
                  threshold_emit_interval: get_duration(threshold_config, :emit_interval_millis),
                  key_value_threshold: get_duration(threshold_config, :kv_threshold_millis),
                  query_threshold: get_duration(threshold_config, :query_threshold_millis),
                  analytics_threshold: get_duration(threshold_config, :analytics_threshold_millis),
                  search_threshold: get_duration(threshold_config, :search_threshold_millis),
                  view_threshold: get_duration(threshold_config, :views_threshold_millis),
                  threshold_sample_size: threshold_config.has_sample_size? ? threshold_config.sample_size : nil,
                  enable_tracing: threshold_config.has_enabled? ? threshold_config.enabled : nil,
                },
              )
            end

            if observability_config.has_logging_meter?
              logging_meter_config = observability_config.logging_meter
              cluster_options.update(
                {
                  metrics_emit_interval: get_duration(logging_meter_config, :emit_interval_millis),
                  enable_metrics: logging_meter_config.has_enabled? ? logging_meter_config.enabled : nil,
                },
              )
            end

            if observability_config.has_orphan_response?
              orphan_config = observability_config.orphan_response
              cluster_options.update(
                {
                  orphaned_emit_interval: get_duration(orphan_config, :emit_interval_millis),
                  oprhaned_sample_size: orphan_config.has_sample_size? ? orphan_config.sample_size : nil,
                  # TODO(RCBC-539): We don't currently have an option for disabling orphan reporting. Once this ticket
                  # is done, add the option here.
                },
              )
            end
          end
        end

        Couchbase::Options::Cluster.new(**cluster_options)
      end

      def get_connection_string(request)
        conn_str = request.cluster_hostname
        conn_str = "couchbase://#{conn_str}" unless conn_str.include?("://")

        return conn_str unless request.has_cluster_config?

        config = request.cluster_config

        if config.has_insecure? && config.insecure
          conn_str += if conn_str.include?("?")
                        "&tls_verify=none"
                      else
                        "?tls_verify=none"
                      end
        end

        # Either 'cert' or 'cert_path' are set, not both
        cert_path =
          if config.has_cert_path?
            config.cert_path
          elsif config.has_cert?
            # Create a temporary file with the certificate
            path = File.join(temp_dir, "#{SecureRandom.uuid}-cluster-cert.pem")
            File.write(path, config.cert)
            path
          end

        return conn_str if cert_path.nil?

        # The certificate path in the connection string takes precedence over the one in ClusterConfig
        return conn_str if conn_str.include?("trust_certificate=")

        if conn_str.include?("?")
          conn_str + "&trust_certificate=#{cert_path}"
        else
          conn_str + "?trust_certificate=#{cert_path}"
        end
      end
    end
  end
end
