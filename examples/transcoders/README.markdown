# Gzip Transcoder

This example demonstrates advanced usage of client-side transcoders.
In particular it uses GzipWriter and GzipReader classes from ruby
standard library to keep documents compressed. The example shows basic
technique, so that it can be used as a starting point to write your
custom data filters and adaptors.

## Usage

1. Clone the repository. Navigate to directory `examples/transcoders`.

2. Install libcouchbase (http://www.couchbase.com/develop/c/current).
   For MacOS users it will look like:

        brew update
        brew install libcouchbase

3. Install all ruby dependencies. This demo has been tested on latest
   stable version of ruby (`ruby 2.0.0p0 (2013-02-24 revision 39474)
   [x86_64-linux]`)

        gem install bundler
        bundle install

4. Setup your Couchbase server
   (http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-getting-started.html).
   It should have `default` bucket. Or you can specify different
   bucket using CLI options.

5. Store some file to the cluster:

        $ ./cb-zcp Gemfile
        Run with arguments: {:bucket=>"default", :hostname=>"127.0.0.1", :port=>8091, :username=>nil, :password=>nil}
        store "Gemfile" ... ok

6. Get the data back:

        $ ./cb-zcat Gemfile
        Run with arguments: {:bucket=>"default", :hostname=>"127.0.0.1", :port=>8091, :username=>nil, :password=>nil}
        Gemfile:
        source 'https://rubygems.org'

        gem 'couchbase'

7. To make sure that the data is in compressed state, you can check it in `irb`:

        $ bundle exec irb
        2.0.0p0 (main):001:0> require 'couchbase'
        true
        2.0.0p0 (main):002:0> conn = Couchbase.connect(:transcoder => nil)
        #<Couchbase::Bucket:0x007fa344886fc8 "http://localhost:8091/pools/default/buckets/default/" transcoder=nil, default_flags=0x0, quiet=false, connected=true, timeout=2500000>
        2.0.0p0 (main):003:0> File.open("Gemfile.gz", "w+"){|f| f.write(conn.get("Gemfile"))}
        65

        $ zcat Gemfile.gz
        source 'https://rubygems.org'

        gem 'couchbase'
