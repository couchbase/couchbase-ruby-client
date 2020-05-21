# Couchbase Ruby Client

This repository contains the third generation of the Couchbase SDK for Ruby (aka. SDKv3)

## Generate Documentation

To generate documentation `yard` library is highly recommended.

    gem install yard

The following steps will generate API reference for selected git tag:

    VERSION=3.0.0.alpha.1
    git checkout $VERSION
    yard doc -o /tmp/couchbase-ruby-client-$VERSION lib/

Now the API reference is accessible using web browser

    firefox /tmp/couchbase-ruby-client-$VERSION

## License

The gem is available as open source under the terms of the [Apache2 License](https://opensource.org/licenses/Apache-2.0).
