# Couchbase Ruby Client

This is the official client library for use with Couchbase Server.

## SUPPORT

If you find an issue, please file it in our [JIRA][1]. Also you are
always welcome on the `#libcouchbase` channel at [freenode.net IRC
servers][2]. Checkout [library overview][overview] and [API
documentation][api].


## INSTALL

At the moment it only support [jruby](http://jruby.org/), so
installation is as simple as:

    $ gem install couchbase

## USAGE

Since it is 2.0 release, it introduces API changes comparing to 1.x
versions.

    require 'couchbase'

Cluster object accepts list of nodes to connect to:

    c = Couchbase::Cluster.new(['localhost'])


To open bucket, specify name and password (optional):

    b = c.open_bucket('default', '')

SDK 2.0 operates with documents, so the API expose
`Couchbase::Document` to wrap your data:

    doc = Couchbase::Document.new(:id => 'mydoc', :content => {'foo' => 'bar'})

Now you can put it into the bucket:

    b.upsert(doc)
    # => #<Couchbase::Document:0x6ada79a9
    #     @cas=247387287270276,
    #     @content={"foo"=>"bar"},
    #     @expiry=0,
    #     @id="mydoc",
    #     @transcode=true>

And retrieve it later:

    b.get('mydoc')
    # => #<Couchbase::Document:0x789052f6
    #     @cas=247387287270276,
    #     @content={"foo"=>"bar"},
    #     @expiry=0,
    #     @id="mydoc",
    #     @transcode=true>

More complex example with `beer-sample` bucket, which comes with the
server distribution.

    require 'couchbase'
    c = Couchbase::Cluster.new(['localhost'])
    b = c.open_bucket('beer-sample')
    res = b.query('beer', 'by_location', :group_level => 1)

    puts 'First 4 countries: '
    res.rows.take(4).each do |row|
      puts "#{row['key'].first}: #{row['value']}"
    end

    res.rows.each do |row|
      b.counter('beer_lovers', +1, initial: 1) if row['value'] > 10
    end
    puts "There are #{b.get('beer_lovers').content} countries with more than 10 breweries"
    # >> First 4 countries:
    # >> Argentina: 2
    # >> Aruba: 1
    # >> Australia: 14
    # >> Austria: 10
    # >> There are 18 countries with more than 10 breweries
