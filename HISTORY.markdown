## 1.1.5 / 2012-09-17

* RCBC-81 Protect against NoMethodError

## 1.1.4 / 2012-08-30

* RCBC-70 return binary keys using Encoding.external value (thanks to Alex Leverington)
* Switch to rbenv because RVM doesn't work with tclsh
* [backport] RCBC-37 Bootstrapping using multiple nodes

## 1.1.3 / 2012-07-27

* RCBC-59 Replicate flags in Bucket#cas operation
* calloc -> xcalloc, free -> xfree
* RCBC-64 Fix Couchbase::Bucket#dup
* Make object_space GC protector per-bucket object
* RCBC-60 Protect exceptions from GC

## 1.1.2 / 2012-06-05

* Upgrade libcouchbase dependency up to 1.0.4
* Backport debugger patch

## 1.1.1 / 2012-03-19

* Allow to force format for get operation (thanks to Darian Shimy)
* Use all arguments if receiver arity is -1 (couchbase_ext.c)
* Doc fixes

## 1.1.0 / 2012-03-07

* Timeout support (CCBC-20)
* Implement disconnect/reconnect interface
* Improve error handling code
* Use URI parser from stdlib
* Improve the documentation and the tests
* Remove direct dependency on libevent
* Remove sasl dependency
* Fix storing empty line and nil
* Allow running tests on real cluster
* Cross-build for windows
* Keep connections in thread local storage
* Add block execution time to timeout
* Implement VERSION command
* Configure Travis-CI

## 1.0.0 / 2011-12-23

* Implement all operations using libcouchbase as backend
* Remove views code. It will be re-added in 1.1 version

## 0.9.8 / 2011-12-16

* Fix RCBC-10: It was impossible to store data in non-default buckets

## 0.9.7 / 2011-10-04

* Fix design doc removing
* Fix 'set' method signature: add missing options argument
* Rename gem to 'couchbase' for easy of use. The github project still
  is 'couchbase-ruby-client'

## 0.9.6 / 2011-10-04

* Fix bug with decoding multiget result
* Allow create design documents from IO and String
* Rename json format to document, and describe possible formats
* Allow to handle errors in view result stream
* Remove dependency on libyajl library: it bundled with yaji now
* Update rake tasks: create zip- and tar-balls

## 0.9.5 / 2011-08-24

* Update installation notes in README

## 0.9.4 / 2011-08-24

* Use streaming json parser to iterate over view results
* Update memcached gem dependency to v1.3
* Proxy TOUCH command to memcached client
* Fix minor bugs in RestClient and Document classes
* Disable CouchDB API for nodes without 'couchApiBase' key provided.
* Fix bug with unicode parsing in config listener
* Add more unit tests

## 0.9.3 / 2011-07-29

* Use Latch (via Mutex and ConditionVariable) to wait until initial
  setup will be finished.
* Update prefix for development views (from '$dev_' to 'dev_')

## 0.9.2 / 2011-07-29

* Use zero TTL by default to store records forever
* Update documentation
* Sleep if setup isn't done

## 0.9.1 / 2011-07-25

* Minor bugfix for RestClient initialization

## 0.9.0 / 2011-07-25

* Initial public release
