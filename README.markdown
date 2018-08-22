# Couchbase Ruby Client

This client was the officially developed Ruby client for Couchbase Server.

## SUPPORT

If you find an issue, please file it in our [JIRA][1]. Also you are always welcome on our forum:
[https://forums.couchbase.com/c/ruby-sdk][2].

## INSTALL

This gem depends [libcouchbase][3]. In most cases installing libcouchbase doesn't take much
effort. After that you can install the couchbase gem itself:

    $ gem install couchbase

The library verified with all major ruby versions: 2.3, 2.4, 2.5.

## USAGE

First, you need to load the library:

```ruby
require 'couchbase'
```

There are several ways to establish a new connection to Couchbase Server.  By default it uses
`couchbase://localhost/default` as the connection string.

```ruby
c = Couchbase.connect
c = Couchbase.connect('couchbase://my.server/mybucket')
```

Note, that from version Couchbase Server 5.0, it is no longer possible to connect to bucket without
credentials, so you have to specify them as options.

```ruby
c = Couchbase.connect(:username => 'Administrator', :password => 'password')
```

There is also a handy method `Couchbase.bucket` which uses thread local storage to keep a reference
to a connection. You can set the connection options via `Couchbase.connection_options`:

```ruby
Couchbase.connection_options = [
  'couchbase://127.0.0.1/beer-sample',
  :username => 'Administrator', :password => 'password'
]

Couchbase.bucket.name                  # => "beer-sample"
Couchbase.bucket.set('foo', 'bar').cas # => 1535020645641158656
```

All operations return `Result` object, which carries errors in `#error` member, which might be
raised if necessary.

```ruby
res = c.get('missing-key')
res.success?                    # => false
res.error
# => #<Couchbase::LibraryError: failed to get key: missing-key: LCB_KEY_ENOENT (0x0D) [ext/couchbase_ext/get.c:33]>
```

The library supports three different formats for representing values:

* `:document` (default) format supports most of ruby types which could be mapped to JSON data
  (hashes, arrays, string, numbers).

* `:plain` This format avoids any conversions to be applied to your data, but your data should be
  passed as String. This is useful for building custom algorithms or formats. For example to
  implement a set: http://dustin.github.com/2011/02/17/memcached-set.html

* `:marshal` Use this format if you'd like to transparently serialize your ruby object with standard
  `Marshal.dump` and `Marshal.load` methods.

In addition to that, custom transcoder might be defined, see
[GzipTranscoder](examples/transcoders/gzip_transcoder.rb) as an example.

The library provides basic set of CRUD operations with ability to query data using View and N1QL
engines.

### Get

```ruby
res = c.get('foo')
res.value                       # => "bar"
res.cas                         # => 1535021121569292288
```

Get and touch

```ruby
res = c.get('foo', :ttl=> 10)
res.value                       # => "bar"
sleep(11)
res = c.get('foo')
res.success?                    # => false
res.error
# => #<Couchbase::LibraryError: failed to get key: foo: LCB_KEY_ENOENT (0x0D) [ext/couchbase_ext/get.c:33]>
```

Get multiple values

```ruby
c.get(['foo', 'bar', 'baz']).each do |key, res|
  p [key, res.value, res.success?]
end
# >> ["foo", {"num"=>3}, true]
# >> ["bar", {"num"=>2}, true]
# >> ["baz", nil, false]
```

Hash-like syntax

```ruby
c["foo"]
# => #<Couchbase::Result:0x0000564cfc9d8470 operation=:get key="foo" cas=1535021687309205504>
```

### Touch

```ruby
c.touch("foo")                      # use Bucket#default_ttl
c.touch("foo", 10)
c.touch("foo", :ttl => 10)
c.touch("foo" => 10, "bar" => 20)
```

### Set

``` ruby
c.set('foo', {:num => 42})
# => #<Couchbase::Result:0x0000563e8a56f080 operation=:set key="foo" cas=1535022223755182080>

c.set('foo', {:num => 42}, :cas => 8835713818674332672)
# => #<Couchbase::Result:0x0000563e8a56d7f8 operation=:set error=#<Couchbase::LibraryError: failed to store key: foo: LCB_KEY_EEXISTS (0x0C) [ext/couchbase_ext/store.c:53]> key="foo" cas=0>

User = Struct.new(:name, :age)
obj = User.new('John', 42)
c.set('foo', obj, :ttl => 30, :format => :marshal)

c.get('foo', :format => :plain).value
# => "\u0004\bS:\tUser\a:\tnameI\"\tJohn\u0006:\u0006ET:\bagei/"

c['foo'] = {:num => 42}
c['foo', :format => :plain] = 'bar'
```

### Fetch

``` ruby
c.fetch('key') { 'bar' }
# => "bar" will be cached with unlimited ttl
c.fetch('key', :ttl => 10) { 'bar' }
# => "bar" will be cached on 10 seconds
```

### Add

The add command will fail if the key already exists. It accepts the same options as set command
above.

```ruby
c.add('foo', 'bar')
c.add('foo', 'bar', :ttl => 30, :format => :plain)
```

### Replace

The replace command will fail if the key already exists. It accepts the same options as set command
above.

```ruby
c.replace('foo', 'bar')
```

### Prepend/Append

These commands are meaningful when you are using the `:plain` value format, because the
concatenation is performed by server which has no idea how to merge to JSON values or values in ruby
Marshal format. You may receive an `Couchbase::Error::ValueFormat` error.

``` ruby
c.set("foo", "world", :format => :plain)
c.append("foo", "!")
c.prepend("foo", "Hello, ")
c.get("foo", :format => :plain).value
# => "Hello, world!"
```

### Increment/Decrement

These commands increment the value assigned to the key. It will raise Couchbase::Error::DeltaBadval
if the delta or value is not a number.

``` ruby
c.set('foo', 1)
c.incr('foo').value                   # => 2
c.incr('foo', :delta => 2).value      # => 4
c.incr('foo', 4).value                # => 8

c.set('foo', 10)
c.decr('foo', 1).value                # => 9
c.decr('foo', 100).value              # => 0

c.incr('missing1', :initial => 10).value      # => 10
c.incr('missing1', :initial => 10).value      # => 11
c.incr('missing2', :create => true).value     # => 0
c.incr('missing2', :create => true).value     # => 1

c.set('foo' => 1, 'bar' => 2)
c.incr('foo' => 3, 'bar' => 4).map { |k, r| [k, r.value] }
# => [["foo", 4], ["bar", 6]]

```

### Delete

``` ruby
c.delete('foo')
c.delete('foo', :cas => 8835713818674332672)
c.delete('foo', 8835713818674332672)

c.delete(['foo', 'bar'])
```

### Flush

Flush the items in the cluster. It raises an error if flush is not enabled for the bucket.

``` ruby
c.flush
# => true
```

### Stats

Return statistics from each node in the cluster. The result is represented as an array with of
result objects which has `node`, `key` and `value` attributes set.

``` ruby
c.stats

c.stats(:memory)
# => [#<Couchbase::Result:0x0000560951784398 operation=:stats key="bytes" value="30630880" node="127.0.0.1:11210">,
#     #<Couchbase::Result:0x00005609517842d0 operation=:stats key="mem_used" value="30630880" node="127.0.0.1:11210">,
#     #<Couchbase::Result:0x0000560951784208 operation=:stats key="mem_used_estimate" value="30630880" node="127.0.0.1:11210">,
#     #<Couchbase::Result:0x0000560951784140 operation=:stats key="mem_used_merge_threshold" value="65536" node="127.0.0.1:11210">,
# ...
```

### Views (Map/Reduce queries)

If you store structured data, they will be treated as documents and you can handle them in
map/reduce function from Couchbase Views. For example, store a couple of posts using memcached API:

``` ruby
c['biking'] = {:title => 'Biking',
               :body => 'My biggest hobby is mountainbiking. The other day...',
               :date => '2009/01/30 18:04:11'}
c['bought-a-cat'] = {:title => 'Bought a Cat',
                     :body => 'I went to the the pet store earlier and brought home a little kitty...',
                     :date => '2009/01/30 20:04:11'}
c['hello-world'] = {:title => 'Hello World',
                    :body => 'Well hello and welcome to my new blog...',
                    :date => '2009/01/15 15:52:20'}
```

Now let's create design doc with sample view and save it in file 'blog.json':

```json
{
  "_id": "_design/blog",
  "language": "javascript",
  "views": {
    "recent_posts": {
      "map": "function(doc){if(doc.date && doc.title){emit(doc.date, doc.title);}}"
    }
  }
}
```

This design document could be loaded into the database like this (also you can pass the ruby Hash or
String with JSON encoded document):

``` ruby
c.save_design_doc(File.open('blog.json'))
```

To execute view you need to fetch it from design document `_design/blog`:

``` ruby
blog = c.design_docs['blog']
blog.views
# => ["recent_posts"]
blog.recent_posts
# => #<Couchbase::View:46921636363220 @ddoc="blog" @view="recent_posts" @params={:connection_timeout=>75000}>
```

The rows of the views could be iterated as an array:

``` ruby
blog.recent_posts.each do |doc|
  # do something
  # with doc object
  doc.key                       # => "2009/01/15 15:52:20" (gives the key argument of the emit())
  doc.value                     # => "Hello World" (gives the value argument of the emit())
  doc.id                        # => "hello-world" (if available, i.e. reduced views might won't have it)
end
```


Load with documents

```ruby
blog.recent_posts(:include_docs => true).each do |doc|
  doc.doc       # gives the document which emitted the item
  doc['date']   # gives the argument of the underlying document
end
```

You can also use Enumerator to iterate view results

```ruby
require 'date'
posts_by_date = Hash.new{|h,k| h[k] = []}
enum = blog.recent_posts(:include_docs => true).each  # request hasn't issued yet
enum.each_with_object(posts_by_date) do |doc, acc|
  acc[Date.strptime(doc['date'], '%Y/%m/%d')] = doc
end
```

Couchbase Server could generate errors during view execution with `200 OK` and partial results. By
default the library raises exception as soon as errors detected in the result stream, but you can
define the callback `on_error` to intercept these errors and do something more useful.

``` ruby
view = blog.recent_posts(:include_docs => true)
logger = Logger.new(STDOUT)

view.on_error do |from, reason|
  logger.warn("#{view.inspect} received the error '#{reason}' from #{from}")
end

posts = view.each do |doc|
  # do something
  # with doc object
end
```

## HACKING

Clone the repository. For starters, you can use github mirror, but make sure you have read and
understand [CONTRIBUTING.markdown][10] if you are going to send us patches.

    $ git clone git://github.com/couchbase/couchbase-ruby-client.git
    $ cd couchbase-ruby-client

Install all development dependencies:

    $ gem install bundler
    $ bundle install

Don't forget to write the tests. You can find examples in the `tests/` directory. To run tests with
a mock just compile extension and run the `test` task, it will download a test mock of couchbase
cluster as a part of the process (the mock is generally slower, but easier to setup):

    $ bundle exec rake compile test

If you have real Couchbase server installed somewhere, you can pass its address using environment
variable `COUCHBASE_SERVER` like this:

    $ COUCHBASE_SERVER=localhost:8091 rake compile test

And finally, you can package the gem with your awesome changes. For UNIX-like systems a regular
source-based package will be enough, so the command below will produce `pkg/couchbase-VERSION.gem`,
where `VERSION` is the current version from file `lib/couchbase/version.rb`:

    $ bundle exec rake package

The Windows operating system usually doesn't have a build environment installed. This is why we are
cross-compiling blobs for Windows from UNIX-like boxes. To do it you need to install mingw and the
[rake-compiler][11] and then build a variety of ruby versions currently supported on Windows. An
example config looks like this:

    $ gem install rake-compiler-dock
    $ rake-compiler-dock

Before you build, check relevant ruby and libcouchbase versions in `tasks/compile.rake`. After that
you can run the `package:windows` task and you will find all artifacts in `pkg/` directory:

    $ rake package:windows
    $ ls -1 pkg/*.gem
    pkg/couchbase-2.0.0.gem
    pkg/couchbase-2.0.0-x64-mingw32.gem
    pkg/couchbase-2.0.0-x86-mingw32.gem


[1]: http://couchbase.com/issues/browse/RCBC
[2]: https://forums.couchbase.com/c/ruby-sdk
[3]: https://developer.couchbase.com/server/other-products/release-notes-archives/c-sdk
[5]: http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped
[10]: https://github.com/couchbase/couchbase-ruby-client/blob/master/CONTRIBUTING.markdown
[11]: https://github.com/luislavena/rake-compiler
