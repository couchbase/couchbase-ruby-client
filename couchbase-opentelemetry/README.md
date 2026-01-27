# Couchbase Ruby Client OpenTelemetry Integration

## Installation

The library has been tested with MRI 3.2, 3.3, 3.4 and 4.0. Supported platforms are Linux and MacOS.

Add this line to your application's Gemfile:

```ruby
gem "couchbase-opentelemetry", "0.1.0"
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install couchbase-opentelemetry

## Usage

Here is an example on how to set up Tracing and Metrics with OpenTelemetr:

```ruby
require "couchbase"
require "couchbase/opentelemetry"

require "opentelemetry-sdk"
require "opentelemetry-metrics-sdk"

# Initialize a tracer provider
tracer_provider = OpenTelemetry::SDK::Trace::TracerProvider.new
tracer_provider.add_span_processor(
    OpenTelemetry::SDK::Trace::Export::BatchSpanProcessor.new(
        OpenTelemetry::Exporter::OTLP::Exporter.new(endpoint: "https://<hostname>:<port>/v1/traces")
    )
)

# Initialize the Couchbase OpenTelemetry Request Tracer
tracer = Couchbase::OpenTelemetry::RequestTracer.new(tracer_provider)

# Initialize a meter provider
meter_provider = OpenTelemetry::SDK::Metrics::MeterProvider.new
meter_provider.add_metric_reader(
    OpenTelemetry::SDk::Metrics::Export::PeriodicMetricReader.new(
        exporter: OpenTelemetry::Exporter::OTLP::Metrics::MetricsExporter.new(
            endpoint: "https://<hostname>:<port>/v1/metrics"
        )
    )
)

# Initialize the Couchbase OpenTelemetry Meter
meter = Couchbase::OpenTelemetry::Meter.new(meter_provider)

# Configure tracer and meter in cluster options
options = Couchbase::Options::Cluster.new(
    authenticator: Couchbase::PasswordAuthenticator.new("Administrator", "password")
    tracer: tracer,
    meter: meter
)

# Initialize cluster instance
cluster = Cluster.connect("couchbase://127.0.0.1", options)
```

## License

The gem is available as open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

    Copyright 2025-Present Couchbase, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
