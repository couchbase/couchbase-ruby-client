# Release Notes

This document is a list of user visible feature changes and important
bugfixes. Do not forget to update this doc in every important patch.

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

* Do not hide ValueFormat reason

## 1.2.0.z.beta3 (2012-10-16)

 18 files changed, 241 insertions(+), 57 deletions(-)

* RCBC-52 Implement bucket create/delete operations

* Propogate status code for HTTP responses

* RCBC-87 Fix build error on macos

* Use global scope to find Error classes (thanks to @wr0ngway)

* Fix memory leaks

* Update to recent libcouchbase API

## 1.2.0.z.beta2 (2012-09-21)

 3 files changed, 6 insertions(+), 2 deletions(-)

* RCBC-82 Not all rubies are fat on MacOS. Fixes build there

## 1.2.0.z.beta  (2012-09-18)

 2 files changed, 5 insertions(+), 1 deletion(-)

* Fix version ordering by using ".z" prefix before .beta

## 1.2.0.beta (2012-09-18)

 51 files changed, 9301 insertions(+), 3364 deletions(-)

* RCBC-81 Protect against NoMethodError in extconf.rb. Fixes
  gem installation

* RCBC-79 Use RESTful flush

* Various build fixes

* Add attribute reader for Error::Base status code

* CCBC-98 Expose client temporary failure error

* RCBC-28 Implement Bucket#unlock

* Fix CAS conversion for Bucket#delete method for 32-bit systems

## 1.1.5 (2012-09-17)

 3 files changed, 9 insertions(+), 5 deletions(-)

* RCBC-81 Fixed installing issue on MacOS.

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

* RCBC-6 Implement OBSERVE command

* RCBC-49 :observe option for storage functions

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

* Update replace documentation: it accepts :cas option

* RCBC-36 Fix segfault. Ocassional segfault when accessing the
  results of a View. https://gist.github.com/2883925

## 1.2.0.dp3 (2012-06-06)

 4 files changed, 22 insertions(+), 4 deletions(-)

* Fix for multi_json < 1.3.3

* Break out from event loop for non-chunked responses (fix creating
  design create)

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

* Fix view iterator. It doesn't lock event loop anymore

* Define views only if "views" key presented

* Require yajl as development dependency

* Implement get with lock operation

* Update documentation

## 1.1.3 (2012-07-27)

 5 files changed, 192 insertions(+), 101 deletions(-)

* RCBC-64 The Couchbase::Bucket class hasn't implemented the #dup
  method. So it caused SEGFAULT. The patch is implementing correct
  function, which copy the internals and initializes new connection.

* RCBC-59 The flags might be reset if caller will use
  Couchbase::Bucket#cas operation. Here is IRB session demostrating
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

* Properly handle hashes as Couchbase.connection_options

* Implement views

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

* The direct dependency on libevent and sasl has been removed. Now the
  library doesn't require libevent headers installed.

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

## 1.0.0 (2012-03-01)

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

* Implement basic commands

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

* Removed depency on will_paginate in development mode

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

* Initial public release. It suppors most of the binary protocol
  commands through memcached gem and also is able to listen to bucket
  configuration and make View requests.
