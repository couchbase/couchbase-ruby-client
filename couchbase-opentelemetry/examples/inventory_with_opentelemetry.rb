#!/usr/bin/env ruby
# frozen_string_literal: true

# inventory_with_opentelemetry — Couchbase Ruby SDK + OpenTelemetry example
#
# Demonstrates distributed tracing and metrics for a Couchbase Ruby application
# using OpenTelemetry, exporting both signals via OTLP to a local observability
# stack (Jaeger, Prometheus, Grafana).
#
# The Docker Compose files for the telemetry stack live in the couchbase-cxx-client-demo
# repository: https://github.com/couchbaselabs/couchbase-cxx-client-demo/tree/main/telemetry-cluster
#
# See README.md in this directory for full setup instructions, environment variable
# reference, signal flow diagram, and guidance on reading traces in Jaeger and
# metrics in Prometheus/Grafana.

require "logger"
require "couchbase"
require "couchbase/opentelemetry"
require "opentelemetry-sdk"
require "opentelemetry-metrics-sdk"
require "opentelemetry-exporter-otlp"
require "opentelemetry-exporter-otlp-metrics"

# ============================================================================
# Configuration
# ============================================================================
#
# All settings can be overridden via environment variables.  Defaults are chosen
# to work against a stock local Couchbase cluster and the bundled telemetry stack.

# service.name and service.version are attached to every exported span and metric
# data point as OTel Resource attributes.  In Jaeger the service name appears in
# the "Service" drop-down; in Prometheus it becomes part of the job/instance labels.
SERVICE_NAME    = "inventory-service"
SERVICE_VERSION = "1.0.0"

# Couchbase connection parameters.  Env: CONNECTION_STRING, USER_NAME, PASSWORD,
# BUCKET_NAME, SCOPE_NAME, COLLECTION_NAME, PROFILE.
CB_CONNECTION_STRING = ENV.fetch("CONNECTION_STRING", "couchbase://127.0.0.1")
CB_USER_NAME         = ENV.fetch("USER_NAME",         "Administrator")
CB_PASSWORD          = ENV.fetch("PASSWORD",          "password")
CB_BUCKET_NAME       = ENV.fetch("BUCKET_NAME",       "default")
CB_SCOPE_NAME        = ENV.fetch("SCOPE_NAME",        "_default")
CB_COLLECTION_NAME   = ENV.fetch("COLLECTION_NAME",   "_default")
CB_PROFILE           = ENV.fetch("PROFILE",           nil)

# Print full Couchbase SDK trace-level logs to stderr.
# Env: VERBOSE  (yes / y / on / true / 1)
TRUTHY = %w[yes y on true 1].freeze
CB_VERBOSE = TRUTHY.include?(ENV.fetch("VERBOSE", "").downcase)

# Print OTel SDK internal diagnostics (export errors, sampler events) to stderr.
# Env: OTEL_VERBOSE  (yes / y / on / true / 1)
OTEL_VERBOSE = TRUTHY.include?(ENV.fetch("OTEL_VERBOSE", "").downcase)

# Number of upsert+get iterations to run.
# Env: NUM_ITERATIONS  (default: 1000)
NUM_ITERATIONS = (ENV.fetch("NUM_ITERATIONS", nil) || ARGV[0] || 1000).to_i

# OTLP HTTP endpoint for traces.
# Env: OTEL_TRACES_ENDPOINT  (default: http://localhost:4318/v1/traces)
OTEL_TRACES_ENDPOINT = ENV.fetch("OTEL_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")

# OTLP HTTP endpoint for metrics.
# Env: OTEL_METRICS_ENDPOINT  (default: http://localhost:4318/v1/metrics)
OTEL_METRICS_ENDPOINT = ENV.fetch("OTEL_METRICS_ENDPOINT", "http://localhost:4318/v1/metrics")

# How often the PeriodicMetricReader wakes up, collects all registered instruments,
# and calls the exporter.
# Env: OTEL_METRICS_READER_EXPORT_INTERVAL_MS  (default: 5000 ms)
OTEL_METRICS_READER_EXPORT_INTERVAL_MS = Integer(ENV.fetch("OTEL_METRICS_READER_EXPORT_INTERVAL_MS", 5_000))

# Maximum time the reader waits for a single export call to complete before
# abandoning it and recording an error.
# Env: OTEL_METRICS_READER_EXPORT_TIMEOUT_MS  (default: 10000 ms)
OTEL_METRICS_READER_EXPORT_TIMEOUT_MS = Integer(ENV.fetch("OTEL_METRICS_READER_EXPORT_TIMEOUT_MS", 10_000))

# Explicit bucket boundaries applied to every registered HISTOGRAM instrument
# via a process-wide catch-all View (see apply_opentelemetry_meter_options).
#
# Why custom boundaries are necessary
# ------------------------------------
# The Couchbase Ruby SDK records db.client.operation.duration in seconds.
# The OpenTelemetry SDK's built-in default boundaries:
#
#   [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000] s
#
# are calibrated for millisecond values (a convention rooted in the HTTP latency
# metrics of the OTel semantic conventions).  For second-valued Couchbase histograms
# — where a well-connected operation typically completes in under 10 ms — almost
# every sample would land in the first bucket, making p50/p99 estimates meaningless.
#
# The defaults below
# -------------------
# Eight boundaries spanning 100 µs to 10 s, chosen to match the Couchbase Java
# SDK's canonical nanosecond recommendation, scaled to seconds (÷ 1 000 000 000):
#
#   Java SDK (nanoseconds)    Ruby SDK (seconds)    Human-readable
#              100 000   →        0.0001            100 µs
#              250 000   →        0.00025           250 µs
#              500 000   →        0.0005            500 µs
#            1 000 000   →        0.001               1 ms
#           10 000 000   →        0.01               10 ms
#          100 000 000   →        0.1               100 ms
#        1 000 000 000   →        1.0                 1 s
#       10 000 000 000   →       10.0                10 s
HISTOGRAM_BOUNDARIES_S = [
  0.0001,  # 100 µs
  0.00025, # 250 µs
  0.0005,  # 500 µs
  0.001,   #   1 ms
  0.01,    #  10 ms
  0.1,     # 100 ms
  1.0,     #   1 s
  10.0,    #  10 s
].freeze

# ============================================================================
# Config dump
# ============================================================================
#
# Print all resolved configuration values before doing anything so that a
# developer looking at the logs can quickly verify the settings.
def dump_config
  puts format("   CONNECTION_STRING: %s", CB_CONNECTION_STRING)
  puts format("          USER_NAME: %s", CB_USER_NAME)
  puts "           PASSWORD: [HIDDEN]"
  puts format("        BUCKET_NAME: %s",   CB_BUCKET_NAME)
  puts format("         SCOPE_NAME: %s",   CB_SCOPE_NAME)
  puts format("    COLLECTION_NAME: %s",   CB_COLLECTION_NAME)
  puts format("            VERBOSE: %s",   CB_VERBOSE)
  puts format("       OTEL_VERBOSE: %s",   OTEL_VERBOSE)
  puts format("     NUM_ITERATIONS: %d",   NUM_ITERATIONS)
  puts format("            PROFILE: %s",   CB_PROFILE.nil? ? "[NONE]" : CB_PROFILE)
  puts
  puts format("        OTEL_TRACES_ENDPOINT: %s", OTEL_TRACES_ENDPOINT)
  puts
  puts format("       OTEL_METRICS_ENDPOINT: %s",               OTEL_METRICS_ENDPOINT)
  puts format("  OTEL_METRICS_READER_EXPORT_INTERVAL_MS: %d",   OTEL_METRICS_READER_EXPORT_INTERVAL_MS)
  puts format("  OTEL_METRICS_READER_EXPORT_TIMEOUT_MS:  %d",   OTEL_METRICS_READER_EXPORT_TIMEOUT_MS)
  puts format("  OTEL_METRICS_HISTOGRAM_BOUNDARIES: [%s]",
              HISTOGRAM_BOUNDARIES_S.join(", "))
  puts
end

# ============================================================================
# Optional: Couchbase SDK verbose logging
# ============================================================================
#
# When VERBOSE=true, enable full trace-level logging from the Couchbase C++ core
# to stderr.  Very noisy — useful only when debugging connection or protocol issues.
if CB_VERBOSE
  cb_logger = Logger.new($stderr)
  cb_logger.level = Logger::DEBUG
  Couchbase.set_logger(cb_logger, level: :trace)
end

# ============================================================================
# Optional: OTel SDK internal diagnostics
# ============================================================================
#
# By default the OTel Ruby SDK discards its own internal log messages.  When
# OTEL_VERBOSE=true, route them to stderr so export failures and other SDK
# problems become visible.  At WARN level to avoid flooding the terminal with
# routine housekeeping messages.
if OTEL_VERBOSE
  otel_logger = Logger.new($stderr)
  otel_logger.level = Logger::WARN
  otel_logger.formatter = proc { |sev, _time, _, msg| "[OTel #{sev}] #{msg}\n" }
  OpenTelemetry.logger = otel_logger
end

# ============================================================================
# OTel Resource
# ============================================================================
#
# An OTel Resource describes the entity producing telemetry — in this case, this
# process.  Attributes set here are stamped on every exported span and metric batch.
# At minimum, service.name must be set; it is the primary key for all trace and
# metric queries in Jaeger, Prometheus, and Grafana.
OTEL_RESOURCE = OpenTelemetry::SDK::Resources::Resource.create(
  "service.name" => SERVICE_NAME,
  "service.version" => SERVICE_VERSION,
)

# ============================================================================
# Tracing pipeline setup
# ============================================================================
#
# Sets up the OpenTelemetry tracing pipeline and returns a RequestTracer adapter
# to wire into the Couchbase cluster options.
#
# Pipeline:
#   Application span  (created in main with app_tracer.start_span)
#     → Couchbase SDK child spans  (RequestTracer adapter)
#     → BatchSpanProcessor         (buffers completed spans in memory)
#     → OtlpExporter               (HTTP POST to OTLP endpoint as JSON)
#     → OTel Collector             (receives on :4318, forwards to Jaeger via OTLP/gRPC)
#     → Jaeger                     (stores and visualises traces)
#
# Returns [tracer_provider, request_tracer] so the caller can:
#   - Pass request_tracer to Couchbase::Options::Cluster.new(tracer: ...)
#   - Call tracer_provider.force_flush before exit to drain buffered spans
def build_tracer_provider
  # --- OTLP HTTP span exporter ---
  # Serialises completed spans as protobuf and POSTs them to the OTLP HTTP
  # endpoint.  The OTel Collector forwards them to Jaeger via OTLP/gRPC.
  otlp_span_exporter = OpenTelemetry::Exporter::OTLP::Exporter.new(
    endpoint: OTEL_TRACES_ENDPOINT,
  )

  # --- Batch span processor ---
  # Accumulates completed spans in an in-memory ring buffer and exports them in
  # batches to reduce HTTP overhead.  Default tuning:
  #   max_queue_size        = 2048 spans
  #   max_export_batch_size = 512 spans
  #   schedule_delay        = 5 s  (background flush interval)
  # force_flush (called before exit) drains the buffer synchronously, ensuring
  # no spans are lost even when the program runs for less than 5 s.

  # --- Sampler ---
  # AlwaysOnSampler records and exports every span (100 % sampling rate).
  # This is appropriate for development and short-lived demos.
  # For production services consider ParentBased(TraceIdRatioBased(0.01))
  # for 1 % head-based sampling, which dramatically reduces export volume while
  # preserving full traces for a statistically representative fraction of requests.
  sampler = OpenTelemetry::SDK::Trace::Samplers.parent_based(
    root: OpenTelemetry::SDK::Trace::Samplers::ALWAYS_ON,
  )

  tracer_provider = OpenTelemetry::SDK::Trace::TracerProvider.new(
    sampler: sampler,
    resource: OTEL_RESOURCE,
  )
  tracer_provider.add_span_processor(
    OpenTelemetry::SDK::Trace::Export::BatchSpanProcessor.new(otlp_span_exporter),
  )

  OpenTelemetry.tracer_provider = tracer_provider

  # Couchbase SDK integration: RequestTracer implements the SDK's tracer interface
  # and forwards every start_span/end_span call to the underlying OTel Tracer.
  # When an operation's options carry a parent_span, the SDK creates its internal
  # spans (upsert, dispatch_to_server, etc.) as children of that parent, producing
  # a complete nested trace hierarchy in Jaeger.
  request_tracer = Couchbase::OpenTelemetry::RequestTracer.new(tracer_provider)

  [tracer_provider, request_tracer]
end

# ============================================================================
# Metrics pipeline setup
# ============================================================================
#
# Sets up the OpenTelemetry metrics pipeline and returns a Meter adapter to wire
# into the Couchbase cluster options.
#
# Pipeline:
#   Couchbase SDK instruments
#     → Couchbase::OpenTelemetry::Meter  (SDK adapter, implements couchbase meter interface)
#     → MeterProvider
#     → View (custom histogram buckets: 100 µs … 10 s)
#     → PeriodicMetricReader   (fires every OTEL_METRICS_READER_EXPORT_INTERVAL_MS)
#     → OtlpMetricsExporter    (HTTP POST to OTLP endpoint as JSON)
#     → OTel Collector         (receives on :4318, exposes Prometheus scrape on :8889)
#     → Prometheus             (scrapes :8889 every 15 s)
#
# Returns [meter_provider, sdk_meter] so the caller can:
#   - Pass sdk_meter to Couchbase::Options::Cluster.new(meter: ...)
#   - Obtain the MeterProvider to create application instruments and call force_flush
def build_meter_provider
  # --- OTLP HTTP metric exporter ---
  # Serialises each metric batch as protobuf and POSTs it to the OTLP HTTP endpoint.
  # The OTel Collector receives batches on :4318, translates them to Prometheus
  # format, and exposes a scrape endpoint on :8889.
  otlp_metrics_exporter = OpenTelemetry::Exporter::OTLP::Metrics::MetricsExporter.new(
    endpoint: OTEL_METRICS_ENDPOINT,
  )

  meter_provider = OpenTelemetry::SDK::Metrics::MeterProvider.new(
    resource: OTEL_RESOURCE,
  )

  # --- Histogram View: explicit bucket boundaries ---
  # Registers a single catch-all View that overrides the aggregation for every
  # HISTOGRAM instrument in the process, replacing the OTel SDK's built-in
  # default boundaries with the calibrated set from HISTOGRAM_BOUNDARIES_S.
  #
  # This View must be registered before add_metric_reader and before any
  # meter/instrument is created — AddView has no effect on instruments that
  # are already recording.
  meter_provider.add_view(
    "*",
    instrument_type: :histogram,
    aggregation: OpenTelemetry::SDK::Metrics::Aggregation::ExplicitBucketHistogram.new(
      boundaries: HISTOGRAM_BOUNDARIES_S,
      record_min_max: true,
    ),
  )

  # --- Periodic metric reader ---
  # Wakes up on a background thread every OTEL_METRICS_READER_EXPORT_INTERVAL_MS
  # (default 5 s), calls collect on every registered meter to gather current
  # instrument values, then passes the batch to the exporter.
  # OTEL_METRICS_READER_EXPORT_TIMEOUT_MS caps how long a single export HTTP call
  # may run before it is abandoned.
  meter_provider.add_metric_reader(
    OpenTelemetry::SDK::Metrics::Export::PeriodicMetricReader.new(
      exporter: otlp_metrics_exporter,
      export_interval_millis: OTEL_METRICS_READER_EXPORT_INTERVAL_MS,
      export_timeout_millis: OTEL_METRICS_READER_EXPORT_TIMEOUT_MS,
    ),
  )

  # Couchbase SDK integration: Couchbase::OpenTelemetry::Meter is the bridge
  # adapter that implements the SDK's meter interface and forwards every record()
  # call to the underlying OTel MeterProvider.  The SDK uses it to record
  # per-operation latency histograms, retry counters, and timeout events — all
  # labelled with bucket, scope, collection, and operation type.
  sdk_meter = Couchbase::OpenTelemetry::Meter.new(meter_provider)

  [meter_provider, sdk_meter]
end

# ============================================================================
# Main
# ============================================================================

# Capture the program start time so we can report total run duration as a
# diagnostic metric.  This is intentionally early so the measurement includes
# connection setup, all cluster operations, and teardown.
demo_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)

dump_config

tracer_provider, request_tracer = build_tracer_provider
meter_provider,  sdk_meter      = build_meter_provider

# ------------------------------------------------------------------
# Cluster setup
# ------------------------------------------------------------------
# OTel metrics and tracing MUST be wired into cluster options before calling
# Couchbase::Cluster.connect.  The SDK reads the meter and tracer from the
# options object at connect time; changing them afterwards has no effect.
cluster_options = Couchbase::Options::Cluster.new(
  authenticator: Couchbase::PasswordAuthenticator.new(CB_USER_NAME, CB_PASSWORD),
  tracer: request_tracer,
  meter: sdk_meter,
)
cluster_options.apply_profile(CB_PROFILE) if CB_PROFILE

cluster    = Couchbase::Cluster.connect(CB_CONNECTION_STRING, cluster_options)
collection = cluster.bucket(CB_BUCKET_NAME).scope(CB_SCOPE_NAME).collection(CB_COLLECTION_NAME)

# ------------------------------------------------------------------
# Application-level tracer and instruments
# ------------------------------------------------------------------
# Obtain a scoped Tracer from the same provider that backs the SDK adapter.
# All Couchbase SDK operations in the loop are given cb_parent as their
# parent_span so the SDK's internal spans (upsert, get, dispatch_to_server, …)
# appear as children of "update-inventory" in Jaeger, giving a single trace
# that covers one loop iteration.
app_tracer = tracer_provider.tracer(SERVICE_NAME, SERVICE_VERSION)

# Per-iteration diagnostic metric.
# Instruments must be created once and reused across recordings; creating a
# new histogram on every iteration is wasteful and produces duplicate
# instrument warnings from the OTel SDK.
# In Prometheus:
#   inventory_demo_iteration_duration_ms_bucket{...}
#   inventory_demo_iteration_duration_ms_sum
#   inventory_demo_iteration_duration_ms_count
app_meter = meter_provider.meter(SERVICE_NAME, version: SERVICE_VERSION)
iteration_duration = app_meter.create_histogram(
  "inventory_demo_iteration_duration",
  description: "Wall-clock duration of a single upsert+get iteration",
  unit: "ms",
)

# ------------------------------------------------------------------
# Progress bar — mirrors the C++ demo's \r-based inline progress
# ------------------------------------------------------------------
error_count = 0
last_error  = nil

print_progress = lambda do |iteration|
  done      = iteration + 1
  pct       = (done * 100 / NUM_ITERATIONS).to_i
  bar_width = 30
  filled    = pct * bar_width / 100
  bar       = "=" * filled
  bar      += ">" if filled < bar_width
  bar      += " " * [bar_width - filled - 1, 0].max
  line      = "\r[#{bar}] #{pct.to_s.rjust(3)}% #{done}/#{NUM_ITERATIONS}  errors: #{error_count}"
  line     += "  last error: #{last_error}" if last_error
  line     += "   "
  print line
  $stdout.flush
end

# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------
NUM_ITERATIONS.times do |iteration|
  document_id = "item::WIDGET-#{iteration}"
  iter_start  = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  # Start root span for this iteration — all Couchbase SDK child spans
  # (upsert, get, dispatch_to_server, …) will be nested under it in Jaeger.
  top_span = app_tracer.start_span("update-inventory")

  # WithActiveSpan sets top_span as the active span on the current thread's context.
  # Any OTel-instrumented library called from this scope that does automatic
  # context propagation will automatically use top_span as its parent.
  ctx = OpenTelemetry::Trace.context_with_span(top_span)

  OpenTelemetry::Context.with_current(ctx) do
    # RequestSpan bridges the OTel Span type to the couchbase request_span
    # interface expected by the SDK's parent_span option.
    cb_parent = Couchbase::OpenTelemetry::RequestSpan.new(top_span)

    # parent_span(cb_parent) attaches this operation to the "update-inventory" trace.
    # The SDK emits an "upsert" child span with "request_encoding" and
    # "dispatch_to_server" grandchildren capturing serialization and the
    # server round-trip duration.
    begin
      collection.upsert(
        document_id,
        {"name" => "Widget Pro", "sku" => "WIDGET-001",
         "category" => "widgets", "quantity" => 42, "price" => 29.99},
        Couchbase::Options::Upsert.new(parent_span: cb_parent),
      )
    rescue StandardError => e
      error_count += 1
      last_error   = "upsert: #{e.message}"
    end

    # Same parent span as the upsert: both operations appear under the same root
    # trace in Jaeger, making it easy to see the full sequence at a glance.
    # The SDK emits a "get" child span with a "dispatch_to_server" grandchild.
    begin
      collection.get(
        document_id,
        Couchbase::Options::Get.new(parent_span: cb_parent),
      )
    rescue StandardError => e
      error_count += 1
      last_error   = "get: #{e.message}"
    end
  end

  print_progress.call(iteration)

  # Mark the root span successful and close it.  The SDK child spans (upsert,
  # get) are already ended by the time collection.upsert/get return.
  top_span.status = OpenTelemetry::Trace::Status.ok
  top_span.finish

  iter_elapsed_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - iter_start) * 1_000
  iteration_duration.record(iter_elapsed_ms)
end

puts "\n"

cluster.disconnect

# ------------------------------------------------------------------
# Demo-app diagnostic metric: total run duration
# ------------------------------------------------------------------
#
# Record the total wall-clock time from process start to cluster close as a
# single histogram sample.  This serves as a simple end-to-end smoke-test for
# the metrics pipeline: if you can see this metric in Prometheus it means the
# full chain (OTel SDK → OTLP exporter → OTel Collector → Prometheus scrape)
# is working correctly.
#
# How to find it in Prometheus (http://localhost:9090):
#   inventory_demo_run_duration_ms_bucket
#   inventory_demo_run_duration_ms_sum
#   inventory_demo_run_duration_ms_count
#
# The metric carries the service.name="inventory-service" resource attribute so
# you can filter by {job="inventory-service"} or similar in Grafana.
demo_elapsed_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - demo_start) * 1_000
puts "Demo run duration: #{demo_elapsed_ms.round} ms"

run_duration = app_meter.create_histogram(
  "inventory_demo_run_duration",
  description: "Total wall-clock duration of the inventory demo run, from process start to cluster close",
  unit: "ms",
)
run_duration.record(demo_elapsed_ms)

# ------------------------------------------------------------------
# Flush before exit
# ------------------------------------------------------------------
#
# ForceFlush ensures all buffered data is exported before the process exits.
# For metrics this is critical: PeriodicMetricReader does not do a final
# collection pass on shutdown, so any data accumulated since the last export
# interval would be silently dropped without it.  For traces, BatchSpanProcessor
# does drain the queue on shutdown, so force_flush is redundant there — but
# kept for symmetry and safety.
#
# Shutdown is intentionally not called here.  Both providers shut down via their
# destructors; an explicit call here would trigger a double-invocation and the
# warning: "[MeterContext::Shutdown] Shutdown can be invoked only once."
tracer_provider.force_flush
meter_provider.force_flush
