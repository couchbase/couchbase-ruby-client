# Couchbase Ruby Client

This is the official client library for use with Couchbase Server. There
are related libraries available:

* [couchbase-model][6] the ActiveModel implementation, git repository:
  [https://github.com/couchbase/couchbase-ruby-model][7]

## SUPPORT

If you find an issue, please file it in our [JIRA][1]. Also you are
always welcome on the `#libcouchbase` channel at [freenode.net IRC
servers][2]. Checkout [library overview][overview] and [API
documentation][api].


## INSTALL

This gem depends [libcouchbase][3]. In most cases installing
libcouchbase doesn't take much effort.

### MacOS (Homebrew)

    $ brew install libcouchbase

The official homebrew repository contains only stable versions of
libvbucket and libcouchbase, if you need preview, take a look at
Couchbase's fork: https://github.com/couchbase/homebrew

    $ brew install https://raw.github.com/couchbase/homebrew/preview/Library/Formula/libcouchbase.rb

If you are experience issues with installing using system ruby, you
might try to use [rbenv][rbenv], and install couchbase there. Here are
the steps:

    $ brew install rbenv ruby-build

Add the following line to the end of your .bashrc:

    if which rbenv > /dev/null; then eval "$(rbenv init -)"; fi

Then install ruby and make it global:

    $ rbenv install 2.1.2
    $ rbenv global 2.1.2

Now you are ready to install and use couchbase ruby gem as usual:

    $ gem install couchbase

### Debian (Ubuntu)

Add the appropriate line to `/etc/apt/sources.list.d/couchbase.list` for
your OS release:

    # Ubuntu 11.10 Oneiric Ocelot (Debian unstable)
    deb http://packages.couchbase.com/ubuntu oneiric oneiric/main

    # Ubuntu 10.04 Lucid Lynx (Debian stable or testing)
    deb http://packages.couchbase.com/ubuntu lucid lucid/main

Import the Couchbase PGP key:

    wget -O- http://packages.couchbase.com/ubuntu/couchbase.key | sudo apt-key add -

Then install them

    $ sudo apt-get update && sudo apt-get install libcouchbase-dev

Again, if you need a preview of a future version, just use another repository in
your `couchbase.list`

    # Ubuntu 11.10 Oneiric Ocelot (Debian unstable)
    deb http://packages.couchbase.com/preview/ubuntu oneiric oneiric/main

    # Ubuntu 10.04 Lucid Lynx (Debian stable or testing)
    deb http://packages.couchbase.com/preview/ubuntu lucid lucid/main

### Centos (Redhat and rpm-based systems)

Add these lines to /etc/yum.repos.d/couchbase.repo using the correct architecture

    [couchbase]
    name = Couchbase package repository
    baseurl = http://packages.couchbase.com/rpm/5.5/i386

    [couchbase]
    name = Couchbase package repository
    baseurl = http://packages.couchbase.com/rpm/5.5/x86_64

Then to install libcouchbase itself, run:

    $ sudo yum update && sudo yum install libcouchbase-devel

We have preview repositories for RPMs too, use them if you want to try
the latest version of libcouchbase:

    [couchbase]
    name = Couchbase package repository
    baseurl = http://packages.couchbase.com/preview/rpm/5.5/i386

    [couchbase]
    name = Couchbase package repository
    baseurl = http://packages.couchbase.com/preview/rpm/5.5/x86_64

### Windows

There are no additional dependencies for Windows systems. The gem carries
a prebuilt binary for it.

### Couchbase gem

Now install the couchbase gem itself

    $ gem install couchbase

The library verified with all major ruby versions: 1.8.7, 1.9.3, 2.0,
2.1.

## USAGE

First, you need to load the library:

    require 'couchbase'

There are several ways to establish a new connection to Couchbase Server.
By default it uses `http://localhost:8091/pools/default/buckets/default`
as the endpoint. The client will automatically adjust configuration when
the cluster will rebalance its nodes when nodes are added or deleted
therefore this client is "smart".

    c = Couchbase.connect

This is equivalent to following forms:

    c = Couchbase.connect("http://localhost:8091/pools/default/buckets/default")
    c = Couchbase.connect("http://localhost:8091/pools/default")
    c = Couchbase.connect("http://localhost:8091")
    c = Couchbase.connect(:hostname => "localhost")
    c = Couchbase.connect(:hostname => "localhost", :port => 8091)
    c = Couchbase.connect(:pool => "default", :bucket => "default")

The hash parameters take precedence on string URL.

If you worry about state of your nodes or not sure what node is alive,
you can pass the list of nodes and the library will iterate over it
until finds the working one. From that moment it won't use **your**
list, because node list from cluster config carries more detail.

    c = Couchbase.connect(:bucket => "mybucket",
                          :node_list => ['example.com:8091', example.net'])

There is also a handy method `Couchbase.bucket` which uses thread local
storage to keep a reference to a connection. You can set the
connection options via `Couchbase.connection_options`:

    Couchbase.connection_options = {:bucket => 'blog'}
    Couchbase.bucket.name                   #=> "blog"
    Couchbase.bucket.set("foo", "bar")      #=> 3289400178357895424

The library supports both synchronous and asynchronous mode. In
asynchronous mode all operations will return control to caller
without blocking current thread. You can pass a block to the method and it
will be called with result when the operation will be completed. You
need to run the event loop once you've scheduled your operations:

    c = Couchbase.connect
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

The asynchronous callback receives an instance of `Couchbase::Result` which
responds to several methods to figure out what was happened:

  * `success?`. Returns `true` if operation succed.

  * `error`. Returns `nil` or exception object (subclass of
    `Couchbase::Error::Base`) if something went wrong.

  * `key`

  * `value`

  * `flags`

  * `cas`. The CAS version tag.

  * `node`. Node address. This is used in the flush and stats commands.

  * `operation`. The symbol, representing an operation.


To handle global errors in async mode `#on_error` callback should be
used. It can be set in following fashions:

    c.on_error do |opcode, key, exc|
      # ...
    end

    handler = lambda {|opcode, key, exc| }
    c.on_error = handler

By default connections use `:quiet` mode. This mean it won't raise
exceptions when the given key does not exist:

    c.get("missing-key")            #=> nil

It could be useful when you are trying to make you code a bit efficient
by avoiding exception handling. (See `#add` and `#replace` operations).
You can turn on these exceptions by passing `:quiet => false` when you
are instantiating the connection or change corresponding attribute:

    c.quiet = false
    c.get("missing-key")                    #=> raise Couchbase::Error::NotFound
    c.get("missing-key", :quiet => true)    #=> nil

The library supports three different formats for representing values:

* `:document` (default) format supports most of ruby types which could
  be mapped to JSON data (hashes, arrays, string, numbers). A future
  version will be able to run map/reduce queries on the values in the
  document form (hashes)

* `:plain` This format avoids any conversions to be applied to your
  data, but your data should be passed as String. This is useful for
  building custom algorithms or formats. For example to implement a set:
  http://dustin.github.com/2011/02/17/memcached-set.html

* `:marshal` Use this format if you'd like to transparently serialize your
  ruby object with standard `Marshal.dump` and `Marshal.load` methods

The couchbase API is the superset of [Memcached binary protocol][5], so
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

The add command will fail if the key already exists. It accepts the same
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

Note that it isn't the same as increment/decrement in ruby. A
Couchbase increment is atomic on a distributed system.  The
Ruby incement could ovewrite intermediate values with multiple
clients, as shown with following `set` operation:

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
      "threads"=>
        {
          "172.16.16.76:12008"=>"4",
          "172.16.16.76:12000"=>"4",
          # ...
        },
      "connection_structures"=>
        {
          "172.16.16.76:12008"=>"22",
          "172.16.16.76:12000"=>"447",
          # ...
        },
      "ep_max_txn_size"=>
        {
          "172.16.16.76:12008"=>"1000",
          "172.16.16.76:12000"=>"1000",
          # ...
        },
      # ...
    }

### Timers

It is possible to create timers to implement general purpose timeouts.
Note that timers are using microseconds for time intervals. For example,
following examples increment the keys value five times with 0.5 second
interval:

    c.set("foo", 100)
    n = 1
    c.run do
      c.create_periodic_timer(500000) do |tm|
        c.incr("foo") do
          if n == 5
            tm.cancel
          else
            n += 1
          end
        end
      end
    end

### Views (Map/Reduce queries)

If you store structured data, they will be treated as documents and you
can handle them in map/reduce function from Couchbase Views. For example,
store a couple of posts using memcached API:

    c['biking'] = {:title => 'Biking',
                   :body => 'My biggest hobby is mountainbiking. The other day...',
                   :date => '2009/01/30 18:04:11'}
    c['bought-a-cat'] = {:title => 'Bought a Cat',
                         :body => 'I went to the the pet store earlier and brought home a little kitty...',
                         :date => '2009/01/30 20:04:11'}
    c['hello-world'] = {:title => 'Hello World',
                        :body => 'Well hello and welcome to my new blog...',
                        :date => '2009/01/15 15:52:20'}

Now let's create design doc with sample view and save it in file
'blog.json':

    {
      "_id": "_design/blog",
      "language": "javascript",
      "views": {
        "recent_posts": {
          "map": "function(doc){if(doc.date && doc.title){emit(doc.date, doc.title);}}"
        }
      }
    }

This design document could be loaded into the database like this (also you can
pass the ruby Hash or String with JSON encoded document):

    c.save_design_doc(File.open('blog.json'))

To execute view you need to fetch it from design document `_design/blog`:

    blog = c.design_docs['blog']
    blog.views                    #=> ["recent_posts"]
    blog.recent_posts             #=> [#<Couchbase::ViewRow:9855800 @id="hello-world" @key="2009/01/15 15:52:20" @value="Hello World" @doc=nil @meta={} @views=[]>, ...]

The gem uses a streaming parser to access view results so you can iterate them
easily. If your code doesn't keep links to the documents the GC might free
them as soon as it decides they are unreachable, because the parser doesn't
store global JSON tree.

    blog.recent_posts.each do |doc|
      # do something
      # with doc object
      doc.key   # gives the key argument of the emit()
      doc.value # gives the value argument of the emit()
    end

Load with documents

    blog.recent_posts(:include_docs => true).each do |doc|
      doc.doc       # gives the document which emitted the item
      doc['date']   # gives the argument of the underlying document
    end


You can also use Enumerator to iterate view results

    require 'date'
    posts_by_date = Hash.new{|h,k| h[k] = []}
    enum = c.recent_posts(:include_docs => true).each  # request hasn't issued yet
    enum.inject(posts_by_date) do |acc, doc|
      acc[date] = Date.strptime(doc['date'], '%Y/%m/%d')
      acc
    end

Couchbase Server could generate errors during view execution with
`200 OK` and partial results. By default the library raises exception as
soon as errors detected in the result stream, but you can define the
callback `on_error` to intercept these errors and do something more
useful.

    view = blog.recent_posts(:include_docs => true)
    logger = Logger.new(STDOUT)

    view.on_error do |from, reason|
      logger.warn("#{view.inspect} received the error '#{reason}' from #{from}")
    end

    posts = view.each do |doc|
      # do something
      # with doc object
    end

Note that errors object in view results usually goes *after* the rows,
so you will likely receive a number of view results successfully before
the error is detected.

## Engines

As far as couchbase gem uses [libcouchbase][8] as the backend, you can
choose from several asynchronous IO options:

* `:default` this one is used by default and implemented as the part
  of the ruby extensions (this mean you don't need any dependencies
  apart from libcouchbase2-core and libcouchbase-dev to build and use
  it). This engine honours ruby GVL, so when it comes to waiting for
  IO operations from kernel it release the GVL allowing interpreter to
  run your code. This technique isn't available on windows, but down't
  worry `:default` engine still accessible and will pick up statically
  linked on that platform `:libevent` engine.

* `:libev` and `:libevent`, these two engines require installed
  libcouchbase2-libev and libcouchbase2-libevent packages
  correspondingly. Currently they aren't so friendly to GVL but still
  useful.

* `:eventmachine` engine. From version 1.2.2 it is possible to use
  great [EventMachine][9] library as underlying IO backend and
  integrate couchbase gem to your current asynchronous application.
  This engine will be only accessible on the MRI ruby 1.9+. Checkout
  simple example of usage:

        require 'eventmachine'
        require 'couchbase'

        EM.epoll = true  if EM.epoll?
        EM.kqueue = true  if EM.kqueue?
        EM.run do
          con = Couchbase.connect :engine => :eventmachine, :async => true
          con.on_connect do |res|
            puts "connected: #{res.inspect}"
            if res.success?
              con.set("emfoo", "bar") do |res|
                puts "set: #{res.inspect}"
                con.get("emfoo") do |res|
                  puts "get: #{res.inspect}"
                  EM.stop
                end
              end
            else
              EM.stop
            end
          end
        end

## HACKING

Clone the repository. For starters, you can use github mirror, but
make sure you have read and understand [CONTRIBUTING.markdown][10] if
you are going to send us patches.

    $ git clone git://github.com/couchbase/couchbase-ruby-client.git
    $ cd couchbase-ruby-client

Install all development dependencies. You can use any ruby version
since 1.8.7, but make sure your changes work at least on major
releases (1.8.7, 1.9.3, 2.0.0 and 2.1.0 at the moment):

    $ gem install bundler
    $ bundle install

Don't forget to write the tests. You can find examples in the `tests/`
directory. To run tests with a mock just compile extension and run the
`test` task, it will download a test mock of couchbase cluster as a
part of the process (the mock is generally slower, but easier to
setup):

    $ rake compile test

If you have real Couchbase server installed somewhere, you can pass
its address using environment variable `COUCHBASE_SERVER` like this:

    $ COUCHBASE_SERVER=localhost:8091 rake compile test

And finally, you can package the gem with your awesome changes. For
UNIX-like systems a regular source-based package will be enough, so the
command below will produce `pkg/couchbase-VERSION.gem`, where
`VERSION` is the current version from file `lib/couchbase/version.rb`:

    $ rake package

The Windows operating system usually doesn't have a build environment
installed. This is why we are cross-compiling blobs for Windows from
UNIX-like boxes. To do it you need to install mingw and the
[rake-compiler][11] and then build a variety of ruby versions currently
supported on Windows. An example config looks like this:

    $ rake-compiler update-config
    Updating /home/avsej/.rake-compiler/config.yml
    Found Ruby version 1.8.7 for platform i386-mingw32 (/home/avsej/.rake-compiler/ruby/i686-w64-mingw32/ruby-1.8.7-p374/lib/ruby/1.8/i386-mingw32/rbconfig.rb)
    Found Ruby version 1.9.3 for platform i386-mingw32 (/home/avsej/.rake-compiler/ruby/i686-w64-mingw32/ruby-1.9.3-p448/lib/ruby/1.9.1/i386-mingw32/rbconfig.rb)
    Found Ruby version 2.0.0 for platform i386-mingw32 (/home/avsej/.rake-compiler/ruby/i686-w64-mingw32/ruby-2.0.0-p247/lib/ruby/2.0.0/i386-mingw32/rbconfig.rb)
    Found Ruby version 2.1.0 for platform i386-mingw32 (/home/avsej/.rake-compiler/ruby/i686-w64-mingw32/ruby-2.1.0/lib/ruby/2.1.0/i386-mingw32/rbconfig.rb)
    Found Ruby version 1.9.3 for platform x64-mingw32 (/home/avsej/.rake-compiler/ruby/x86_64-w64-mingw32/ruby-1.9.3-p448/lib/ruby/1.9.1/x64-mingw32/rbconfig.rb)
    Found Ruby version 2.0.0 for platform x64-mingw32 (/home/avsej/.rake-compiler/ruby/x86_64-w64-mingw32/ruby-2.0.0-p247/lib/ruby/2.0.0/x64-mingw32/rbconfig.rb)
    Found Ruby version 2.1.0 for platform x64-mingw32 (/home/avsej/.rake-compiler/ruby/x86_64-w64-mingw32/ruby-2.1.0/lib/ruby/2.1.0/x64-mingw32/rbconfig.rb)

To install all versions needed for `rake package:windows` use these
commands:

    $ rake-compiler cross-ruby HOST=i386-mingw32 VERSION=1.8.7-p374
    $ rake-compiler cross-ruby HOST=i386-mingw32 VERSION=1.9.3-p448
    $ rake-compiler cross-ruby HOST=i386-mingw32 VERSION=2.0.0-p247
    $ rake-compiler cross-ruby HOST=i386-mingw32 VERSION=2.1.0
    $ rake-compiler cross-ruby HOST=x64-mingw32 VERSION=1.9.3-p448
    $ rake-compiler cross-ruby HOST=x64-mingw32 VERSION=2.0.0-p247
    $ rake-compiler cross-ruby HOST=x64-mingw32 VERSION=2.1.0

Before you build, check relevant ruby and libcouchbase versions in
`tasks/compile.rake`. After that you can run the `package:windows`
task and you will find all artifacts in `pkg/` directory:

    $ rake package:windows
    $ ls -1 pkg/*.gem
    pkg/couchbase-1.3.4.gem
    pkg/couchbase-1.3.4-x64-mingw32.gem
    pkg/couchbase-1.3.4-x86-mingw32.gem


[api]: http://www.couchbase.com/autodocs/couchbase-ruby-client-latest/index.html
[overview]: http://docs.couchbase.com/couchbase-sdk-ruby-1.3/index.html
[1]: http://couchbase.com/issues/browse/RCBC
[2]: http://freenode.net/irc_servers.shtml
[3]: http://www.couchbase.com/develop/c/current
[4]: https://github.com/mxcl/homebrew/pulls/avsej
[5]: http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped
[6]: https://rubygems.org/gems/couchbase-model
[7]: https://github.com/couchbase/couchbase-ruby-model
[8]: http://www.couchbase.com/develop/c/current
[9]: http://rubygems.org/gems/eventmachine
[10]: https://github.com/couchbase/couchbase-ruby-client/blob/master/CONTRIBUTING.markdown
[11]: https://github.com/luislavena/rake-compiler
[rbenv]: https://github.com/sstephenson/rbenv#homebrew-on-mac-os-x
