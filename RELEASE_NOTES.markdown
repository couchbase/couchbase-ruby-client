# Release Notes

This document is a list of user visible feature changes and important
bugfixes. Do not forget to update this doc in every important patch.

## 2.0.0 (2018-XX-YY)
* Reduce API surface to improve maintainability:
  * removed EventMachine plugin
  * removed asynchronous mode
  * removed key prefix support
  * use Result instances instead of primitives and tuples
  * remove timer API
  * update internal HTTP APIs, replace yaji with jsonsl.rb

## 1.3 End-of-life Notice
* [major] Couchbase Ruby Client 1.3 has gone to end of life for official
  support (Nov 2016).
* Further improvements to the library
  from the community are welcome and will be handled via pull requests on a
  case-by-case basis.

## 1.3.13 (2015-05-16)

* [major] RCBC-196 Improved integration with libcouchbase IO API

## 1.3.12 (2015-04-21)

* [major] Experimental support of N1QL querires

## 1.3.11 (2015-01-08)

* [minor] Update crossbuild system to use cmake for libcouchbase

* [minor] Update to libcouchbase 2.4.5 and wire IO functions at
  runtime

## 1.3.10 (2014-09-30)

* [major] Updates regarding libcouchbase 2.4.2. Bundle "bsdio-inl.c"
  which is not part of libcouchbase distribution anymore. This fixes
  installation recent ruby gem versions with libcouchbase 2.4.2.

* [minor] Allow both `:hostname` and `:host` options to constructor

## 1.3.9 (2014-08-26)

* [major] Updates regarding libcouchbase 2.4.0. Deprecations cleanup

## 1.3.8 (2014-06-11)

* [major] RCBC-176 Known Issue: When the couchbase client is used in
  an 'em-synchrony' context, it is possible that connection errors may
  not be propagated to the client. This leads to runtime assertion in
  eventmachine.

* [minor] Update runtime (connection_pool) and build (libcouchbase)
  dependencies to the latest versions.

## 1.3.7 (2014-04-18)

* [major] Allow the selection of bootstrap providers. Since libcouchbase
  2.3.0 there is a new bootstrapping transport available: Cluster
  Configuration Carrier Publication (CCCP). It is more efficient way
  to keep cluster configuration up to date using Carrier Publication
  instead of HTTP connection.

        nodes = ["example.com", "example.org"]
        Couchbase.connect(node_list: nodes, bootstrap_transports: [:cccp, :http])

  Read more about it here:
  http://www.couchbase.com/wiki/display/couchbase/Cluster+Configuration+Carrier+Publication

* [major] RCBC-168 An experimental DNS SRV helper for connection
  constructor. The DNS SRV records need to be configured on a reachable
  DNS server. An example configuration could look like the following
  (note that the service ids might change):

        _cbmcd._tcp.example.com.  0  IN  SRV  20  0  11210 node2.example.com.
        _cbmcd._tcp.example.com.  0  IN  SRV  10  0  11210 node1.example.com.
        _cbmcd._tcp.example.com.  0  IN  SRV  30  0  11210 node3.example.com.

        _cbhttp._tcp.example.com.  0  IN  SRV  20  0  8091 node2.example.com.
        _cbhttp._tcp.example.com.  0  IN  SRV  10  0  8091 node1.example.com.
        _cbhttp._tcp.example.com.  0  IN  SRV  30  0  8091 node3.example.com.

  Now if "example.com" is passed in as the argument, the three nodes
  configured will be parsed and put in the returned URI list. Note
  that the priority is respected (in this example, node1 will be the
  first one in the list, followed by node2 and node3). As of now,
  weighting is not supported. This is how it could be used to
  bootstrap the connection:

        transport = :http
        nodes = Couchbase::DNS.locate('example.com', transport)
        if nodes.empty?
          nodes = ["example.com:8091"]
        end
        Couchbase.connect(node_list: nodes, bootstrap_transports: [transport])

  NOTE: This is experiemental and subject to change at any time. Watch
  the release notes for changes in future releases.

* [major] RCBC-166 Fix a crash with eventmachine. In eventmachine event
  handlers are separated and run seprately and in the following order:
  [READ, WRITE]. So it was possible to cancel WRITE event handler from
  the READ handler which cause crash when the reactor run it in next
  turn.

* [minor] Fixed a typo which doesn't allow to use bundler in the project
  directory.

## 1.3.6 (2014-02-17)

* [major] Fix linkage issue which blocks library installation on
  windows.

## 1.3.5 (2014-02-05)

* [major] RCBC-159, RCBC-152 Honor the :environment constructor argument

* [major] Allow inheritance from `Couchbase::Bucket`. It wasn't
  possible to create a view with subclass of the `Couchbase::Bucket`.

* [major] Ensure that an exception raised early will not prevent the
  finalizer from being called in the underlying client being constructed.
  One example situation where this could occur:

        class Couchbase::Bucket
          def initialize(*args)
            raise "something wrong"
            super
          end
        end

## 1.3.4 (2014-01-08)

* [major] Build 64-bit versions of the extensions for Windows
  platform. Also support ruby 2.0 and 2.1.

* [minor] Update hacking section in README

* [minor] Fix gemspec warnings regarding versions of the dependencies.
  Now it honours semantic versioning and doesn't use strict versions.

* [major] RCBC-151 Return CAS in extended mode for incr/decr

* [minor] RCBC-150 Update list of options on `Cluster.create_bucket`.
  Added new options: `:replica_index`, `:flush_enabled`,
  `:parallel_db_and_view_compaction`.

* [major] Allow retries on Couchbase::Bucket#cas collisions.  Now it
  takes a `:retry` Fixnum option that specifies the maximum number of
  times the method should retry the entire get/update/set operation
  when a `Couchbase::Error::KeyExists` error is encountered due to a
  concurrent update from another writer between its `#get` and `#set`
  calls.

* [major] MD5 and truncate ActiveSupport::Cache keys that are longer
  than 250 characters.

## 1.3.3 (2013-09-12)

* [major] RCBC-134 Allow application to use several connections with
  thread-local singleton.

* [major] RCBC-135 Fixed invalid memory access which was detected by
  using 'GC.stress = true' in tests.

* [major] RCBC-141 Initialize event indexes correctly. The plugin
  didn't trace event callbacks, which might lead to invalid memory
  access during rebalance, where libcouchbase creates/removes a lot of
  events because of a fast-changing topology.

* [major] RCBC-137 Add selection options for new IO engines: select
  and iocp.

* [major] When setting the username field, check for password
  presence. Fixes segmentation fault in this code:

        Couchbase.connect(:username => "default", :bucket => "default")

* [minor] Allow to determine the version of libcouchbase:

        Couchbase.libcouchbase_version

* [major] RCBC-136 Build shared object for ruby 2.0 on windows. Also
  fixes build script when using latest rake and rake-compiler.

* [minor] Fix deprecation warning on ruby 2.x. On newer versions it
  should use `rb_thread_call_without_gvl()`.

        ext/couchbase_ext/multithread_plugin.c: In function ‘loop_run_poll’:
        ext/couchbase_ext/multithread_plugin.c:772:5: warning: ‘rb_thread_blocking_region’ is deprecated (declared at .../2.0.0-p247-dbg/include/ruby-2.0.0/ruby/intern.h:839) [-Wdeprecated-declarations]
             rb_thread_blocking_region(loop_blocking_poll, args, RUBY_UBF_PROCESS, NULL);

* [major] Do not try to compile with plugins for Windows platform.

* [major] Force handle to be NULL on `lcb_create()` failure.
  `lcb_create()` can leave garbage in the pointer even if the call
  itself failed.  This behaviour could lead to illegal memory access
  on GC.

* [minor] Remove usage of `RARRAY_PTR` in favor of `rb_ary_entry`.
  This improves performance significantly on Rubinius and also
  improves compatibility with future CRuby 2.1 which introduces
  generational garbage collection. This results in these arrays not
  having to be rescanned in Rubinius and not marked as shady in
  RBGCENC in CRuby 2.1.
  For more discussion, also see: https://bugs.ruby-lang.org/issues/8399

## 1.3.2 (2013-07-10)

* [major] RCBC-133 Allow application to select the strategy of reading
  from replica nodes. **This version requires libcouchbase >= 2.0.7.**
  Now three strategies are available:

    * `:first` - synonym to `true`, previous behaviour now the
      default. It means that the library will sequentially iterate
      over all replicas in the configuration supplied by the cluster
      and will return as soon as it finds a successful response, or
      report an error.

            c.get("foo", :replica => true)
            c.get("foo", :replica => :first)
            #=> "bar"
            c.get("foo", :replica => :first, :extended => true)
            #=> ["bar", 0, 11218368683493556224]

    * `:all` - query all replicas in parallel. In this case the method
      will return the array of the values on the all replica nodes without
      a particular order. Also if the key isn't on the node, it will be
      skipped in the result array.

            c.get("foo", :replica => :all)
            #=> ["bar", "bar", "bar"]
            c.get("foo", :replica => :all, :extended => true)
            #=> [["bar", 0, 11218368683493556224],
            #    ["bar", 0, 11218368683493556224],
            #    ["bar", 0, 11218368683493556224]]

    * `Fixnum` - you can also select specific replica node by its
      index in the cluster configuration. It should be in interval
      `0...c.num_replicas`

            0...c.num_replicas
            #=> 0...3
            c.get("foo", :replica => 1)
            #=> "bar"
            c.get("foo", :replica => 42)
            #=> ArgumentError: replica index should be in interval 0...3

  Note that applications should not assume the order of the
  replicas indicates more recent data is at a lower index number.
  It is up to the application to determine which version of a
  document/item it may wish to use in the case of retrieving data
  from a replica.


## 1.3.1 (2013-06-05)

* [major] RCBC-131 Couchbase::Cluster instance shouldn't require
  persistent connection.

* Fix compatibility with multi_json 1.7.5. They removed VERSION
  constant

## 1.3.0 (2013-05-07)

* [major] RCBC-46 implement Couchbase::ConnectionPool to allow
  applications (and ActiveSupport::Cache::CouchbaseStore) use it in
  multi-threaded environment

* [major] Introduce Transcoders. This mechanism is more flexible, and
  similar to how other clients encode values.

* [minor] Deprecate numeric argument to 'default_format'. Instead
  of this style:

        Couchbase.connect(:default_format => Couchbase::Bucket::FMT_MARSHAL)

  Symbol notation or explicit transcoder entity should be used

        Couchbase.connect(:default_format => :marshal)
        Couchbase.connect(:transcoder => Couchbase::Transcoder::Marshal)

## 1.2.3 (2013-04-02)

* [major] Make ActiveSupport::Cache::CouchbaseStore threadsafe

* [minor] Check for gethrtime. Needed for solaris/smartos

* [minor] Update documentation bits regarding SET operations

## 1.2.2 (2013-02-11)

* [minor] Bucket#design_docs will return a Hash with DesignDoc
  instances as a values.

* [critical] RCBC-104 Data corruption on intensive store operations.
  The issue could also lead to segfaults.

* [major] RCBC-118 Alias #total_rows as #total_entries on view result
  set to match documentation.

* [minor] View#fetch_all - async method for fetching all records

        conn.run do
          doc.recent_posts.fetch_all do |posts|
            do_something_with_all_posts(posts)
          end
        end

* [major] Allow to use Bucket instance in completely asynchronous
  environment like this, without blocking on connect:

        conn = Couchbase.new(:async => true)
        conn.run do
          conn.on_connect do |res|
            if res.success?
              #
              # schedule async requests
              #
            end
          end
        end

* [major] RCBC-27 EventMachine plugin to integrate with EventMachine
  library. Note that the plugin is experimental at this stage.
  Example:

        require 'eventmachine'
        require 'couchbase'

        EM.epoll = true  if EM.epoll?
        EM.kqueue = true  if EM.kqueue?
        EM.run do
          con = Couchbase.connect(:engine => :eventmachine, :async => true)
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


## 1.2.1 (2012-12-28)

* [major] RCBC-101 Persistence constraints wasn't passed to mutation
  methods, so they haven't been applied properly.

* [major] RCBC-102 Inconsistent return values in case of storage
  functions with persistence constraints. It always return a Hash like
  in case of multi-set, even if there is only one document is being
  set.

* [minor] Improve internal structures of multi-threaded IO plugin to
  protect it from memory leaks when the Fiber object is forgotten.

## 1.2.0 (2012-12-12)

 30 files changed, 2079 insertions(+), 662 deletions(-)

* Specialized io plugin for releasing Ruby GVL (thanks to
  Sokolov Yura aka funny_falcon).

  * Ruby 1.9.x uses global lock for ensuring integrity, and blocking
    calls should be called inside rb_thread_blocking_region to allow
    other threads to be runned.

  * Ruby 1.8.7 have only green threads, so that rb_thread_schedule
    should be called manually.

* RCBC-42 Catch exceptions from ruby callbacks

* RCBC-99 read out the StringIO contents in json gem monkey patch

* Use marshal serializer by default for session store

* Remove debugger development dependency

* Fix memory leaks and performance improvements

## 1.2.0.z.beta5 (2012-11-29)

 25 files changed, 1419 insertions(+), 1230 deletions(-)

* RCBC-95 Use response body to clarify Couchbase::Error::HTTP

* Fix memory leaks: in async mode context wasn't freed

* Allow to setup default initial value for INCR/DECR on per connection
  level.

* Make error message about libcouchbase dependency more verbose

## 1.2.0.z.beta4 (2012-11-21)

 27 files changed, 311 insertions(+), 123 deletions(-)

* Increase default connection timeout for Views up to 75 seconds

* RCBC-94 Reset global exception after usage

* RCBC-89 Do not expose docs embedded in HTTP response. Use binary
  protocol for it.

* Remove all_docs mentions. It isn't recommended to use it because of
  performance issues

* Protect against non string values in :plain mode. Will raise error
  if the value given isn't a string.

* RCBC-90 Update documentation about session store

* Make rack session store adapter quiet

* Update to recent libcouchbase API

* Adjust version check for MultiJson monkeypatch (8098da1)

* Do not hide ValueFormat reason. It is accessible using
  Couchbase::Error::Value#inner_exception.

## 1.2.0.z.beta3 (2012-10-16)

 18 files changed, 241 insertions(+), 57 deletions(-)

* RCBC-52 Implement bucket create/delete operations. Examples:

        conn = Couchbase::Cluster.new(:hostname => "localhost",
                 :username => "Administrator", :password => "secret")
        conn.create_bucket("my_protected_bucket",
                 :ram_quota => 500, # megabytes
                 :sasl_password => "s3cr3tBuck3t")

* Propagate status code for HTTP responses

* RCBC-87 Fix build error on Mac OS X

* Use global scope to find Error classes (thanks to @wr0ngway)

* Fix memory leaks

* Update to recent libcouchbase API

## 1.2.0.z.beta2 (2012-09-21)

 3 files changed, 6 insertions(+), 2 deletions(-)

* RCBC-82 Not all rubies are fat on Mac OS X. Fixes build there

## 1.2.0.z.beta  (2012-09-18)

 2 files changed, 5 insertions(+), 1 deletion(-)

* Fix version ordering by using ".z" prefix before .beta. The problem
  is that DP (Developer Preview) should have lower precedence than
  Beta, but alphabetially ".beta" orders before ".dp". This is why
  further Beta versions have ".z".

## 1.2.0.beta (2012-09-18)

 51 files changed, 9301 insertions(+), 3364 deletions(-)

* RCBC-81 Protect against NoMethodError in extconf.rb. Fixes
  gem installation

* RCBC-79 Use RESTful flush

* Various build fixes

* Add attribute reader for Error::Base status code

* CCBC-98 Expose client temporary failure error

* RCBC-28 Implement Bucket#unlock

        # Unlock the single key
        val, _, cas = c.get("foo", :lock => true, :extended => true)
        c.unlock("foo", :cas => cas)

        # Unlock several keys
        c.unlock("foo" => cas1, :bar => cas2)
        #=> {"foo" => true, "bar" => true}

* Fix CAS conversion for Bucket#delete method for 32-bit systems

## 1.1.5 (2012-09-17)

 3 files changed, 9 insertions(+), 5 deletions(-)

* RCBC-81 Fixed installing issue on Mac OS X.

## 1.1.4 (2012-08-30)

 5 files changed, 64 insertions(+), 30 deletions(-)

* RCBC-37 Allow to pass intial list of nodes which will allow to
  iterate addresses until alive node will be found.

        Couchbase.connect(:node_list => ['example.com:8091', 'example.org:8091', 'example.net'])

* RCBC-70 Fixed UTF-8 in the keys. Original discussion
  https://groups.google.com/d/topic/couchbase/bya0lSf9uGE/discussion

## 1.2.0.dp6 (2012-06-28)

 21 files changed, 1520 insertions(+), 428 deletions(-)

* RCBC-47 Allow to skip username for protected buckets. The will use
  bucket name for credentials.

* Expose number of replicas to the user

* RCBC-6 Implement Bucket#observe command to query durable state.
  Examples:

        # Query state of single key
        c.observe("foo")
        #=> [#<Couchbase::Result:0x00000001650df0 ...>, ...]

        # Query state of multiple keys
        keys = ["foo", "bar"]
        stats = c.observe(keys)
        stats.size   #=> 2
        stats["foo"] #=> [#<Couchbase::Result:0x00000001650df0 ...>, ...]

* RCBC-49 Storage functions with durability requirements

        # Ensure that the key will be persisted at least on the one node
        c.set("foo", "bar", :observe => {:persisted => 1})

* RCBC-50 Allow to read keys from replica

* RCBC-57 Expose timers API from libcouchbase

* RCBC-59 Replicate flags in Bucket#cas operation

* Apply timeout value before connection. Currently libcouchbase shares
  timeouts for connection and IO operations. This patch allows to
  setup timeout on the instantiating the connection.

* RCBC-39 Allow to specify delta for incr/decr in options

* RCBC-40 Fix Bucket#cas operation behaviour in async mode. The
  callback of the Bucket#cas method is triggered only once, when it
  fetches old value, and it isn't possible to receive notification if
  the next store operation was successful. Example, append JSON
  encoded value asynchronously:

        c.default_format = :document
        c.set("foo", {"bar" => 1})
        c.run do
          c.cas("foo") do |val|
            case val.operation
            when :get
              val["baz"] = 2
              val
            when :set
              # verify all is ok
              puts "error: #{ret.error.inspect}" unless ret.success?
            end
          end
        end
        c.get("foo")      #=> {"bar" => 1, "baz" => 2}

* RCBC-43 More docs and examples on views

* RCBC-37 Bootstrapping using multiple nodes

        Couchbase.connect(:node_list => ['example.com:8091', 'example.org:8091', 'example.net'])

* Inherit StandardError instead RuntimeError for errors

## 1.2.0.dp5 (2012-06-15)

 12 files changed, 939 insertions(+), 20 deletions(-)

* Integrate with Rack and Rails session store

        # rack
        require 'rack/session/couchbase'
        use Rack::Session::Couchbase

        # rails
        require 'action_dispatch/middleware/session/couchbase_store'
        AppName::Application.config.session_store :couchbase_store

* Implement cache store adapter for Rails

        cache_options = {
          :bucket => 'protected',
          :username => 'protected',
          :password => 'secret',
          :expires_in => 30.seconds
        }
        config.cache_store = :couchbase_store, cache_options

* Implement key prefix (simple namespacing)

        Couchbase.connect(:key_prefix => "prefix:")

* Allow to force assembling result Hash for multi-get

        connection.get("foo", "bar")
        #=> [1, 2]
        connection.get("foo", "bar", :assemble_hash => true)
        #=> {"foo" => 1, "bar" => 2}

## 1.2.0.dp4 (2012-06-07)

 4 files changed, 34 insertions(+), 19 deletions(-)

* Update Bucket#replace documentation: it accepts :cas option

* RCBC-36 Fix segfault. Occasional segfault when accessing the
  results of a View. https://gist.github.com/2883925

## 1.2.0.dp3 (2012-06-06)

 4 files changed, 22 insertions(+), 4 deletions(-)

* Fix for multi_json < 1.3.3

* Break out from event loop for non-chunked responses. View results
  are chunked by default, so there no problems, but other requests
  like Bucket#save_design_doc() were "locking" awaiting forever.

## 1.2.0.dp2 (2012-06-06)

 22 files changed, 859 insertions(+), 253 deletions(-)

* RCBC-31 Make Bucket#get more consistent. The pattern of using more
  than one argument to determine if an array should be returned is not
  idiomatic. Consider the case of a multi-get in an application where
  I have n items to return. If there happens to be only one item it
  will be treated differently than if there happens to be 2 items.

        get(["foo"])  #=> ["bar"]
        get("foo")    #=> "bar"
        get(["x"], :extended => true) #=> {"x"=>["xval", 0, 18336939621176836096]}

* Use monotonic high resolution clock

* Implement threshold for outgoing commands

* Allow to stop event loop from ruby

* RCBC-35 Fix the View parameters escaping. More info at
  https://gist.github.com/2775050

* RCBC-34 Use multi_json gem. json_gem compatibility (require
  'yajl/json_gem') is notorious for causing all kinds of issues with
  various gems. The most compatible way to use yajl is to call
  Yajl::Parser and Yajl::Encoder directly.

* Allow to block and wait for part of the requests

* Fixed view iterator. It doesn't lock event loop anymore This used to
  cause "locks", memory leaks or even segmentation fault.

* Define views only if "views" key presented

* Require yajl as development dependency

* RCBC-76 Implement get with lock operation. Examples:

        # Get and lock key using default timeout
        c.get("foo", :lock => true)

        # Determine lock timeout parameters
        c.stats.values_at("ep_getl_default_timeout", "ep_getl_max_timeout")
        #=> [{"127.0.0.2:11210"=>"15"}, {"127.0.0.1:11210"=>"30"}]

        # Get and lock key using custom timeout
        c.get("foo", :lock => 3)

* Update documentation

## 1.1.3 (2012-07-27)

 5 files changed, 192 insertions(+), 101 deletions(-)

* RCBC-64 The Couchbase::Bucket class hasn't implemented the #dup
  method. So it caused SEGFAULT. The patch is implementing correct
  function, which copy the internals and initializes new connection.

* RCBC-59 The flags might be reset if caller will use
  Couchbase::Bucket#cas operation. Here is IRB session demonstrating
  the issue:

        irb> Couchbase.bucket.set("foo", "bar", :flags => 0x100)
        17982951084586893312
        irb> Couchbase.bucket.cas("foo") { "baz" }
        1712422461213442048
        irb> Couchbase.bucket.get("foo", :extended => true)
        ["baz", 0, 1712422461213442048]


* RCBC-60 Make object_space GC protector per-bucket object. Previous
  version provided not completely thread-safe bucket instance, because
  it was sharing global hash for protecting objects, created in
  extension, from garbage collecting.

## 1.1.2 (2012-06-05)

 5 files changed, 9 insertions(+), 4 deletions(-)

* Upgrade libcouchbase dependency to 1.0.4. Version 1.0.4 includes
  important stability fixes.

* Backport debugger patch. The gem used to require debugger as
  development dependency. Unfortunately ruby-debug19 isn't supported
  anymore for ruby 1.9.x. But there is new gem 'debugger'. This patch
  replaces this dependency.

## 1.2.0.dp (2012-04-10)

 19 files changed, 1606 insertions(+), 93 deletions(-)

* Properly handle hashes as Couchbase.connection_options. Fixed a bug
  when 'Couchbase.connection_options' for "default" connection, when
  there are several arguments to pass to the connect() function when
  establishing thread local connection as below:

        Couchbase.connection_options = {:port => 9000, :bucket => 'myapp'}

* Implement views. Couchbase Server Views are accessible using the
  view APIs. Please refer to
  http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views.html
  for getting started with views.

* Use verbose mode by default throwing exceptions on NOT_FOUND errors.
  This means that quiet attribute is false now on new connections.

* Documentation fixes

## 1.1.1 (2012-03-19)

 5 files changed, 83 insertions(+), 23 deletions(-)

* Flags are used differently in different clients for example between
  Python and Ruby. This fix will force the format to a known value
  irrespective of the flags.

* Calls between Ruby and C libraries for Couchbase which involved
  default arguments had an associated arity of -1 which was not being
  handled correctly. That is being handled correctly now.

## 1.1.0 (2012-03-07)

 27 files changed, 2460 insertions(+), 849 deletions(-)

* With the usage of the URI parser from stdlib it is possible to
  validate the bucket URI more strictly. Also, it is possible to
  specify credentials in the URI like:
  http://username:password@example.com:8091/pools/default/buckets/custom

* The "default" connection is available in thread local storage. This
  mean that using the Couchbase.bucket method it is possible to get
  access to current connection and there is no need to share
  connections when running in multi-thread environment. Each thread
  has its own connection reference.

* The direct dependency on libevent and libsasl has been removed. Now
  the library doesn't require libevent headers installed.

* The disconnect and reconnect interfaces are implemented which
  provide routines for explicit resource management. Connections were
  freed only when the Garbage Collector found that the connection was
  not being used. Now it's possible for the client to check if the
  bucket was connected using 'connected?' or 'disconnect' it manually
  or 'reconnect' using old settings.

* There were spurious timeout issues with a compound statement like
  below. No timeout will occur unless there is a problem with the
  connection.

        connection.run do
          connection.get("foo") {|ret| puts "got foo = #{ret.value}"}
          sleep(5)
        end

* It is not required to install libcouchbase or libvbucket on windows.

* It is possible to store nil as a value. It is possible to
  distinguish a nil value from a missing key by looking at at the
  value returned and the flags and CAS values as well.

* Based on the time out fix (CCBC-20), clients will be notified when
  the connection was dropped or host isn't available.

## 1.0.0 (2012-01-23)

 50 files changed, 4696 insertions(+), 2647 deletions(-)

* Port library to use libcouchbase instead of memcached gem.
  Implemented following operations:

  * get, []
  * set, []=
  * add
  * replace
  * append
  * prepend
  * compare-and-swap
  * arithmetic (incr/decr)
  * flush
  * stats
  * delete
  * touch

* Introduce support for three data formats:

  * document
  * marshal
  * plain

* Removed Views support

* Added benchmarks, couchbase vs. memcached vs. dalli

* Implement asynchronous protocol

## 0.9.8 (2011-12-16)

 3 files changed, 8 insertions(+), 3 deletions(-)

* RCBC-10 Always specify credentials for non-default buckets. It was
  impossible to store data in non-default buckets

## 0.9.7 (2011-10-05)

 7 files changed, 31 insertions(+), 19 deletions(-)

* Fix design doc removing

* Fix 'set' method signature: add missing options argument

* Rename gem to 'couchbase' for easy of use. The github project still
  is 'couchbase-ruby-client'

## 0.9.6 (2011-10-04)

 13 files changed, 609 insertions(+), 99 deletions(-)

* Fix bug with decoding multiget result

* Allow create design documents from IO and String

* Rename 'json' format to 'document', and describe possible formats

* Allow to handle errors in view result stream

* Remove dependency on libyajl library: it bundled with yaji now

## 0.9.5 (2011-08-24)

 4 files changed, 59 insertions(+), 28 deletions(-)

* Update README. Make it more human-friendly

* Removed dependency on will_paginate in development mode

## 0.9.4 (2011-08-01)

 24 files changed, 1240 insertions(+), 78 deletions(-)

* Use streaming json parser to iterate over view results

* Update memcached gem dependency to v1.3

* Proxy TOUCH command to memcached client

* Fix minor bugs in RestClient and Document classes

* Disable CouchDB API for nodes without 'couchApiBase' key provided.

* Fix bug with unicode parsing in config listener

* 61f394e RCBC-5 Add Dave's test case: ensure memcached client
  initialized. Fixes Timeout error on connecting to membase with
  Couchbase.new on Ruby 1.8.7

## 0.9.3 (2011-07-29)

 6 files changed, 167 insertions(+), 9 deletions(-)

* Use Latch (via Mutex and ConditionVariable) to wait until initial
  setup will be finished.

* Update prefix for development views (from '$dev_' to 'dev_')

## 0.9.2 (2011-07-28)

 5 files changed, 31 insertions(+), 20 deletions(-)

* Use zero TTL by default to store records forever

* Update documentation

* Wait until configuration is done

## 0.9.1 (2011-07-25)

 3 files changed, 5 insertions(+), 2 deletions(-)

* Minor bugfix for RestClient initialization

## 0.9.0 (2011-07-25)

 19 files changed, 1174 insertions(+)

* Initial public release. It supports most of the binary protocol
  commands through memcached gem and also is able to listen to bucket
  configuration and make View requests.
