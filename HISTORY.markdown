=== Development version

* Use streaming json parser to iterate over view results
* Update memcached gem dependency to v1.3
* Proxy TOUCH command to memcached client
* Fix minor bugs in RestClient and Document classes
* Disable CouchDB API for nodes without 'couchApiBase' key provided.
* Fix bug with unicode parsing in config listener
* Add more unit tests

=== 0.9.3 / 2011-07-29

* Use Latch (via Mutex and ConditionVariable) to wait until initial
  setup will be finished.
* Update prefix for development views (from '$dev_' to 'dev_')

=== 0.9.2 / 2011-07-29

* Use zero TTL by default to store records forever
* Update documentation
* Sleep if setup isn't done


=== 0.9.1 / 2011-07-25

* Minor bugfix for RestClient initialization

=== 0.9.0 / 2011-07-25

* Initial public release
