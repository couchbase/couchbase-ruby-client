# Couchbase Ruby Client [![Build Status](https://secure.travis-ci.org/avsej/couchbase-ruby-client.png?branch=master)](http://travis-ci.org/avsej/couchbase-ruby-client)

This is the official client library for use with Couchbase Server.

## INSTALL

This gem depends on a couple of external libraries to work with JSON and
Couchbase Server. For JSON it uses [yajl-ruby][1] which is built atop
[yajl][2]. For Couchbase iteraction it uses [libcouchbase][3]. The first
dependency shouldn't cause any issues because it will bundle yajl in the c
extensions. To install yajl-ruby use following command:

    $ gem install yajl-ruby

In most cases installing libcouchbase is just as simple.

### MacOS (Homebrew)

    $ brew install libcouchbase

Or if our pull request isn't yet merged:

    $ brew install http://packages.couchbase.com/clients/c/homebrew/libvbucket.rb
    $ brew install http://packages.couchbase.com/clients/c/homebrew/libcouchbase.rb

### Debian (Ubuntu)

Download packages depending on your architecture:

    $ wget http://packages.couchbase.com/clients/c/libvbucket{1,1-dbg,-dev}_1.8.0.1-1_amd64.deb
    $ wget http://packages.couchbase.com/clients/c/libcouchbase{1,1-dbg,-dev}_1.0.0-1_amd64.deb

or

    $ wget http://packages.couchbase.com/clients/c/libvbucket{1,1-dbg,-dev}_1.8.0.1-1_i386.deb
    $ wget http://packages.couchbase.com/clients/c/libcouchbase{1,1-dbg,-dev}_1.0.0-1_i386.deb

Then install them using dpkg tool

    $ sudo dpkg -i lib{vbucket,couchbase}*.deb

### Centos (Redhat and rpm-based systems)

Download packages depending on your architecture:

    $ wget http://packages.couchbase.com/clients/c/libvbucket{1,-debuginfo,-devel}-1.8.0.1-1.x86_64.rpm
    $ wget http://packages.couchbase.com/clients/c/libcouchbase{1,-debuginfo,-devel}-1.0.0-1.x86_64.rpm

or

    $ wget http://packages.couchbase.com/clients/c/libvbucket{1,-debuginfo,-devel}-1.8.0.1-1.i386.rpm
    $ wget http://packages.couchbase.com/clients/c/libcouchbase{1,-debuginfo,-devel}-1.0.0-1.i386.rpm

Then install them using rpm tool

    $ sudo rpm -ivh lib{vbucket,couchbase}*.rpm

### Couchbase gem

Now install the couchbase gem itself

    $ gem install couchbase


## USAGE

First of all you need to load library:

    require 'couchbase'

There are several ways to establish new connection to Couchbase Server.
By default it uses the `http://localhost:8091/pools/default/buckets/default`
as the endpoint. The client will automatically adjust configuration when
the cluster will rebalance its nodes when nodes are added or deleted
therefore this client is "smart".

    c = Couchbase.new

This is equivalent to following forms:

    c = Couchbase.new("http://localhost:8091/pools/default/buckets/default")
    c = Couchbase.new("http://localhost:8091/pools/default")
    c = Couchbase.new("http://localhost:8091")
    c = Couchbase.new(:host => "localhost")
    c = Couchbase.new(:host => "localhost", :port => 8091)
    c = Couchbase.new(:pool => "default", :bucket => "default")

The hash parameters take precedence on string URL.

The library supports both synchronous and asynchronous mode. In
asynchronous mode all operations will return control to caller
without blocking current thread. You can pass the block to method and it
will be called with result when the operation will be completed. You
need to run event loop when you scheduled your operations:

    c = Couchbase.new
    c.run do |conn|
      conn.get("foo") {|ret| puts ret.value}
      conn.set("bar", "baz")
    end

The handlers could be nested

    c.run do |conn|
      conn.get("foo") do |ret|
        conn.incr(ret.value, :initial => 0)
      end
    end

The asynchronous callback receives instance of `Couchbase::Result` which
responds to several methods to figure out what was happened:

  * `success?`. Returns `true` if operation succed.

  * `error`. Returns `nil` or exception object (subclass of
    `Couchbase::Error::Base`) if something went wrong.

  * `key`

  * `value`

  * `flags`

  * `cas`. The CAS version tag.

  * `node`. Node address. It is used in flush and stats commands.

  * `operation`. The symbol, representing an operation.


To handle global errors in async mode `#on_error` callback should be
used. It can be set in following fashions:

    c.on_error do |opcode, key, exc|
      # ...
    end

    handler = lambda {|opcode, key, exc| }
    c.on_error = handler

By default connection uses `:quiet` mode. This mean it won't raise
exceptions when the given key is not exists:

    c.get("missing-key")            #=> nil

It could be useful when you are trying to make you code a bit efficient
by avoiding exception handling. (See `#add` and `#replace` operations).
You can turn on these exception by passing `:quiet => false` when you
are instantiating the connection or change corresponding attribute:

    c.quiet = false
    c.get("missing-key")                    #=> raise Couchbase::Error::NotFound
    c.get("missing-key", :quiet => true)    #=> nil

The library supports three different formats for representing values:

* `:document` (default) format supports most of ruby types which could
  be mapped to JSON data (hashes, arrays, string, numbers). A future
  version will be able to run map/reduce queries on the values in the
  document form (hashes)

* `:marshal` This format avoids any conversions to be applied to your
  data, but your data should be passed as String. This is useful for
  building custom algorithms or formats. For example to implement a set:
  http://dustin.github.com/2011/02/17/memcached-set.html

* `:plain` Use this format if you'd like to transparently serialize your
  ruby object with standard `Marshal.dump` and `Marshal.load` methods

The couchbase API is the superset of [Memcached binary protocol][4], so
you can use its operations.

### Get

    val = c.get("foo")
    val, flags, cas = c.get("foo", :extended => true)

Get and touch

    val = c.get("foo", :ttl => 10)

Get multiple values. In quiet mode will put `nil` values on missing
positions:

    vals = c.get("foo", "bar", "baz")
    val_foo, val_bar, val_baz = c.get("foo", "bar", "baz")
    c.run do
      c.get("foo") do |ret|
        ret.success?
        ret.error
        ret.key
        ret.value
        ret.flags
        ret.cas
      end
    end

Get multiple values with extended information. The result will
represented by hash with tuples `[value, flags, cas]` as a value.

    vals = c.get("foo", "bar", "baz", :extended => true)
    vals.inspect    #=> {"baz"=>["3", 0, 4784582192793125888],
                         "foo"=>["1", 0, 8835713818674332672],
                         "bar"=>["2", 0, 10805929834096100352]}

Hash-like syntax

    c["foo"]
    c["foo", "bar", "baz"]
    c["foo", {:extended => true}]
    c["foo", :extended => true]         # for ruby 1.9.x only

### Touch

    c.touch("foo")                      # use :default_ttl
    c.touch("foo", 10)
    c.touch("foo", :ttl => 10)
    c.touch("foo" => 10, "bar" => 20)
    c.touch("foo" => 10, "bar" => 20){|key, success|  }

### Set

    c.set("foo", "bar")
    c.set("foo", "bar", :flags => 0x1000, :ttl => 30, :format => :plain)
    c["foo"] = "bar"
    c["foo", {:flags => 0x1000, :format => :plain}] = "bar"
    c["foo", :flags => 0x1000] = "bar"          # for ruby 1.9.x only
    c.set("foo", "bar", :cas => 8835713818674332672)
    c.set("foo", "bar"){|cas, key, operation|  }

### Add

Add command will fail if the key already exists. It accepts the same
options as set command above.

    c.add("foo", "bar")
    c.add("foo", "bar", :flags => 0x1000, :ttl => 30, :format => :plain)

### Replace

The replace command will fail if the key already exists. It accepts the same
options as set command above.

    c.replace("foo", "bar")

### Prepend/Append

These commands are meaningful when you are using the `:plain` value format,
because the concatenation is performed by server which has no idea how
to merge to JSON values or values in ruby Marshal format. You may receive
an `Couchbase::Error::ValueFormat` error.

    c.set("foo", "world")
    c.append("foo", "!")
    c.prepend("foo", "Hello, ")
    c.get("foo")                    #=> "Hello, world!"

### Increment/Decrement

These commands increment the value assigned to the key. It will raise
Couchbase::Error::DeltaBadval if the delta or value is not a number.

    c.set("foo", 1)
    c.incr("foo")                   #=> 2
    c.incr("foo", :delta => 2)      #=> 4
    c.incr("foo", 4)                #=> 8
    c.incr("foo", -1)               #=> 7
    c.incr("foo", -100)             #=> 0
    c.run do
      c.incr("foo") do |ret|
        ret.success?
        ret.value
        ret.cas
      end
    end

    c.set("foo", 10)
    c.decr("foo", 1)                #=> 9
    c.decr("foo", 100)              #=> 0
    c.run do
      c.decr("foo") do |ret|
        ret.success?
        ret.value
        ret.cas
      end
    end

    c.incr("missing1", :initial => 10)      #=> 10
    c.incr("missing1", :initial => 10)      #=> 11
    c.incr("missing2", :create => true)     #=> 0
    c.incr("missing2", :create => true)     #=> 1

Note that it isn't the same as increment/decrement in ruby, which is
performed on client side with following `set` operation:

    c["foo"] = 10
    c["foo"] -= 20                  #=> -10

### Delete

    c.delete("foo")
    c.delete("foo", :cas => 8835713818674332672)
    c.delete("foo", 8835713818674332672)
    c.run do
      c.delete do |ret|
        ret.success?
        ret.key
      end
    end

### Flush

Flush the items in the cluster.

    c.flush
    c.run do
      c.flush do |ret|
        ret.success?
        ret.node
      end
    end

### Stats

Return statistics from each node in the cluster

    c.stats
    c.stats(:memory)
    c.run do
      c.stats do |ret|
        ret.success?
        ret.node
        ret.key
        ret.value
      end
    end

The result is represented as a hash with the server node address as
the key and stats as key-value pairs.

    {
      "172.16.16.76:12008"=>
        {
          "threads"=>"4",
          "connection_structures"=>"22",
          "ep_max_txn_size"=>"10000",
          # ...
        },
      "172.16.16.76:12000"=>
        {
          "threads"=>"4",
          "connection_structures"=>"447",
          "ep_max_txn_size"=>"10000",
          # ...
        },
      # ...
    }

[1]: https://github.com/brianmario/yajl-ruby/
[2]: http://lloyd.github.com/yajl/
[3]: http://www.couchbase.com/develop/c/current
[4]: http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped
