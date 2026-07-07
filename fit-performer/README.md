# Couchbase Ruby FIT performer

This is the FIT performer for the Couchbase Ruby Client.

## Setup

### Generated code

To generate the Ruby code corresponding to the Protocol Buffers and gRPC definitions:

```shell
bundle exec rake generate
```

### Updating the protocol

The FIT protocol is hosted in https://github.com/couchbaselabs/fit-protocol. There is a copy of the protocol in this repository (in `/fit-performer/proto`) the performer is built against.

The latest version of the protocol can be fetched by running:
```shell
bundle exec rake update_protocol
```

## Starting the performer

```shell
bundle exec ruby server.rb
```

## Docker

We provide a dockerfile to build and run the performer with Docker. You can build the image from the root of this repository as follows:
```shell
docker build -f fit-performer/Dockerfile .
```
