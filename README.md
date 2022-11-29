# Couchbase::StellarNebula

## Development

Clone repository and switch to branch `stellar-nebula`:

    git clone https://github.com/couchbase/couchbase-ruby-client.git --branch stellar-nebula

Install development dependencies

    ./bin/setup

Display available rake tasks:

    bundle exec rake -T

When doing pull requests, ensure that target branch will be `stellar-nebula` and keep uniform style (see `rake rubocop`
family of tasks)

## Regenerate gRPC Stubs

Ensure that submodules are synchronized

    git submodule update --init

Generate rake stubs (`generate` task will also reformat generated code using rubocop)

    bundle exec rake generate
