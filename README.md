# Couchbase Ruby Client

[![license](https://img.shields.io/github/license/couchbase/couchbase-ruby-client?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)
[![gem](https://img.shields.io/gem/v/couchbase?color=brightgreen)](https://rubygems.org/gems/couchbase)
[![commits](https://img.shields.io/github/commits-since/couchbase/couchbase-ruby-client/latest?color=brightgreen)](https://github.com/couchbase/couchbase-ruby-client/commits/master)
[![tests](https://img.shields.io/github/workflow/status/couchbase/couchbase-ruby-client/tests?label=tests)](https://github.com/couchbase/couchbase-ruby-client/actions?query=workflow%3Atests)
[![linters](https://img.shields.io/github/workflow/status/couchbase/couchbase-ruby-client/linters?label=linters)](https://github.com/couchbase/couchbase-ruby-client/actions?query=workflow%3Alinters)

This repository contains the third generation of the official Couchbase SDK for Ruby (aka. SDKv3)

## Support and Feedback

If you find an issue, please file it in [our JIRA issue tracker](https://couchbase.com/issues/browse/RCBC). Also you are
always welcome on [our forum](https://forums.couchbase.com/c/ruby-sdk).

Please attach version information to ticket/post. To obtain this information use the following command:

    $ ruby -r couchbase -e 'p Couchbase::VERSION'

## Installation

The library tested with the MRI 2.5, 2.6, 2.7 and 3.0. Supported platforms are Linux and MacOS.

Add this line to your application's Gemfile:

```ruby
gem "couchbase", "3.0.3"
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install couchbase

For some platforms we precompile binary packages. When, for some reason, binary package cannot be used, pass
`--platform=ruby` to `gem install` command (or check `specific_platform` and `force_ruby_platform` options of Bundler).
In latter case, see [Development](#development) section for build dependencies.

## Usage

Here is a basic steps demonstrating basic operations with data:

```ruby
require "couchbase"
include Couchbase # include Couchbase module for brevity

# initialize library
cluster = Cluster.connect("couchbase://127.0.0.1", "Administrator", "password")

# open bucket and collection for data operations
bucket = cluster.bucket("my_bucket")
collection = bucket.default_collection

# update or insert the document
res = collection.upsert("foo", {"bar" => 42})
res.cas
#=> 22120998714646

# retrieve document from the collection
res = collection.get("foo")
res.cas
#=> 22120998714646
res.content
#=> {"bar"=>42}

# remove document
res = collection.remove("foo")
res.cas
#=> 47891154812182

# fetch top-3 cities by number of hotels in the collection
res = cluster.query("
          SELECT city, COUNT(*) AS cnt FROM `travel-sample`
          WHERE type = $type
          GROUP BY city
          ORDER BY cnt DESC
          LIMIT 3",
        Options::Query(named_parameters: {type: "hotel"}, metrics: true))
res.rows.each do |row|
  p row
end
#=> {"city"=>"San Francisco", "cnt"=>132}
#   {"city"=>"London", "cnt"=>67}
#   {"city"=>"Paris", "cnt"=>64}
```

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

    Copyright 2011-2021 Couchbase, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
