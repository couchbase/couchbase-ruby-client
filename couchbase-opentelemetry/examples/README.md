# Couchbase Ruby SDK — OpenTelemetry examples

## inventory_with_opentelemetry.rb

Demonstrates how to instrument a Couchbase Ruby application with OpenTelemetry
distributed tracing and metrics, and how to ship both signals to a local
observability stack (Jaeger, Prometheus, Grafana) via an OTel Collector.

### OpenTelemetry integration with the Couchbase Ruby SDK

The SDK exposes two hook points in `Couchbase::Options::Cluster`:

**Tracing** (`Couchbase::OpenTelemetry::RequestTracer`)

Wraps an `OpenTelemetry::Trace::TracerProvider`.  Installed via:

```ruby
options = Couchbase::Options::Cluster.new(tracer: request_tracer)
```

Every SDK operation — upsert, get, query, etc. — creates a child span under the
`parent_span` supplied at call time (e.g. `Couchbase::Options::Upsert.new(parent_span: cb_parent)`).
Child spans are annotated with the bucket, scope, collection, and internal timing
(encode / dispatch / decode).

**Metrics** (`Couchbase::OpenTelemetry::Meter`)

Wraps an `OpenTelemetry::Metrics::MeterProvider`.  Installed via:

```ruby
options = Couchbase::Options::Cluster.new(meter: sdk_meter)
```

The SDK records per-operation latency histograms (`db.client.operation.duration`,
unit `"s"`) and retry/timeout counters, all labelled by bucket, scope, collection,
and operation type.  A `PeriodicMetricReader` (default interval: 5 s) pushes those
measurements to the configured OTLP endpoint.

**Histogram bucket calibration** — The OTel SDK's built-in default histogram
boundaries are calibrated for millisecond values.  For second-valued Couchbase
histograms — where a well-connected operation typically completes in under 10 ms —
almost every sample would land in the first bucket, making p50/p99 estimates
meaningless.  The example installs a process-wide catch-all View that replaces
those defaults with eight boundaries spanning 100 µs to 10 s, matching the
Couchbase Java SDK's canonical nanosecond recommendation scaled to seconds:

| Java SDK (ns)  | Ruby SDK (s) | Human-readable |
|----------------|--------------|----------------|
| 100 000        | 0.0001       | 100 µs         |
| 250 000        | 0.00025      | 250 µs         |
| 500 000        | 0.0005       | 500 µs         |
| 1 000 000      | 0.001        |   1 ms         |
| 10 000 000     | 0.01         |  10 ms         |
| 100 000 000    | 0.1          | 100 ms         |
| 1 000 000 000  | 1.0          |   1 s          |
| 10 000 000 000 | 10.0         |  10 s          |

Both providers use an AlwaysOn sampler / cumulative aggregation and export via
OTLP/HTTP to the OTel Collector.  `force_flush` is called explicitly before exit
so no spans or metrics are dropped.

> **Note** — AlwaysOn sampling (100 %) is fine for demos and development but is
> rarely appropriate in production.  Consider `ParentBased(TraceIdRatioBased(N))`
> for head-based probabilistic sampling or a tail-based sampler in the Collector.

> **Note** — The Couchbase Ruby SDK currently supports only metrics and traces.
> It does not emit logs via OpenTelemetry.  The Loki and Promtail containers in
> the telemetry stack are present for completeness but receive no data from this
> example.

### Signal flow

```
This program
  │  OTLP/HTTP  http://localhost:4318/v1/traces    (traces)
  │  OTLP/HTTP  http://localhost:4318/v1/metrics   (metrics)
  ▼
OpenTelemetry Collector  (telemetry-cluster/otel-collector-config.yaml)
  │  traces  ── OTLP/gRPC ──► Jaeger                         (port 16686)
  │  metrics ── Prometheus scrape endpoint :8889 ──► Prometheus (port 9090)
  ▼
Jaeger      http://localhost:16686 — distributed trace viewer
Prometheus  http://localhost:9090  — time-series metrics store
Grafana     http://localhost:3000  — unified dashboards (queries both)
```

### Quick-start

#### 1. Start the observability stack

The Docker Compose files for the telemetry stack live in the
[couchbase-cxx-client-demo](https://github.com/couchbaselabs/couchbase-cxx-client-demo/tree/main/telemetry-cluster)
repository.  Clone it and start the stack:

```sh
git clone https://github.com/couchbaselabs/couchbase-cxx-client-demo.git
cd couchbase-cxx-client-demo/telemetry-cluster
docker compose up -d
```

Containers started: `otel-collector`, `jaeger`, `prometheus`, `loki`,
`promtail`, `grafana`.  Allow ~10 s for all services to become healthy.

#### 2. Install dependencies

```sh
cd couchbase-opentelemetry/examples
bundle install
```

#### 3. Run the example

```sh
CONNECTION_STRING=couchbase://127.0.0.1 \
USER_NAME=Administrator \
PASSWORD=password \
BUCKET_NAME=default \
  bundle exec ruby inventory_with_opentelemetry.rb
```

The OTLP endpoints default to `http://localhost:4318/v1/{traces,metrics}`,
pointing at the OTel Collector started above.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CONNECTION_STRING` | `couchbase://127.0.0.1` | Couchbase connection string |
| `USER_NAME` | `Administrator` | RBAC username |
| `PASSWORD` | `password` | RBAC password |
| `BUCKET_NAME` | `default` | Bucket to write into |
| `SCOPE_NAME` | `_default` | Scope within the bucket |
| `COLLECTION_NAME` | `_default` | Collection within the scope |
| `PROFILE` | _(none)_ | SDK connection profile, e.g. `wan_development` |
| `NUM_ITERATIONS` | `1000` | Number of upsert+get loop iterations |
| `VERBOSE` | `false` | `true` — enable Couchbase SDK trace-level logging to stderr |
| `OTEL_VERBOSE` | `false` | `true` — print OTel SDK internal warnings/errors to stderr |
| `OTEL_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP HTTP endpoint for traces |
| `OTEL_METRICS_ENDPOINT` | `http://localhost:4318/v1/metrics` | OTLP HTTP endpoint for metrics |
| `OTEL_METRICS_READER_EXPORT_INTERVAL_MS` | `5000` | How often the metric reader collects and exports |
| `OTEL_METRICS_READER_EXPORT_TIMEOUT_MS` | `10000` | Timeout for a single metric export call |

### Where to see the results

**Traces → Jaeger UI** `http://localhost:16686`

1. Open the Jaeger UI in a browser.
2. In the **Service** drop-down select `inventory-service`.
3. Click **Find Traces**.
4. Open an `update-inventory` trace.  The hierarchy looks like:

   ```
   update-inventory                   ← top-level span (this program)
     upsert                           ← SDK upsert operation
       request_encoding               ← document serialization
       dispatch_to_server             ← server round-trip
     get                              ← SDK get operation
       dispatch_to_server             ← server round-trip
   ```

   Operation spans (`upsert`, `get`) carry: `db.system.name`, `db.namespace`,
   `db.operation.name`, `couchbase.collection.name`, `couchbase.scope.name`,
   `couchbase.service`, `couchbase.retries`.

   `dispatch_to_server` spans carry: `network.peer.address`, `network.peer.port`,
   `network.transport`, `server.address`, `server.port`, `couchbase.operation_id`,
   `couchbase.server_duration`, `couchbase.local_id`.

**Metrics → Prometheus** `http://localhost:9090`

The OTel Collector exposes a Prometheus scrape endpoint on `:8889`; Prometheus
scrapes it every 15 s (`telemetry-cluster/prometheus.yml`).

The Couchbase Ruby SDK records:

```
db_client_operation_duration_seconds_bucket  — per-bucket sample counts (use for percentiles)
db_client_operation_duration_seconds_sum     — cumulative latency across all operations
db_client_operation_duration_seconds_count   — total number of completed operations
```

Each series is labelled with the service type (`kv`, `query`, …) and operation
name (`upsert`, `get`, …).

The example also records two application-level histograms:

```
inventory_demo_iteration_duration_ms  — wall-clock duration of each upsert+get iteration
inventory_demo_run_duration_ms        — total wall-clock duration of the demo run
```

**Metrics + Traces → Grafana** `http://localhost:3000`

Grafana is pre-provisioned (anonymous Admin, no login required) with Prometheus
and Jaeger as data sources.

- **Explore → Prometheus**: query SDK metrics by name or label.
- **Explore → Jaeger**: search traces by service `inventory-service`.
