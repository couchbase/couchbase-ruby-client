# Couchbase Ruby Client [![tests](https://github.com/couchbase/couchbase-ruby-client/workflows/tests/badge.svg)](https://github.com/couchbase/couchbase-ruby-client/actions?query=workflow%3Atests)

This repository contains the third generation of the official Couchbase SDK for Ruby (aka. SDKv3)

## Support and Feedback

If you find an issue, please file it in [our JIRA issue tracker](https://couchbase.com/issues/browse/RCBC).
Also you are always welcome on [our forum](https://forums.couchbase.com/c/ruby-sdk).

Please attach version information to ticket/post. To obtain this information use the following command:

    $ ruby -r couchbase -e 'p Couchbase::VERSION'

## Installation

The library tested with the MRI 2.5, 2.6 and 2.7. Supported platforms are Linux and MacOS.

Add this line to your application's Gemfile:

```ruby
gem "couchbase", "3.0.0.beta.1"
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install --pre couchbase

For some platforms we precompile binary packages. When, for some reason, binary package cannot be used, pass
`--platform=ruby` to `gem install` command (or check `specific_platform` and `force_ruby_platform` options of Bundler).
In latter case, see [Development](#development) section for build dependencies.

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive
prompt that will allow you to experiment.

Part of the library is written in C++, and requires both C and C++ compilers installed on the system. To configure build
environment, it uses `cmake`. When these tools installed, the build process is easy:

    $ rake compile

## Generate Documentation

To generate documentation `yard` library is highly recommended.

    $ gem install yard

The following steps will generate API reference for selected git tag:

    $ rake doc

Now the API reference is accessible using web browser (where `VERSION` is current version of the SDK)

    $ firefox doc/couchbase-ruby-client-VERSION/index.html

## License

The gem is available as open source under the terms of the [Apache2 License](https://opensource.org/licenses/Apache-2.0).
