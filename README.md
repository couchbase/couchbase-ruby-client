Couchbase Ruby Client
=====================

This is the official client library for use with Couchbase Server.

EXAMPLE
=======

    gem 'couchbase-ruby-client'
    require 'couchbase'

    # establish connection with couchbase default pool and default bucket
    c = Couchbase.new("http://localhost:8091/pools/default")
    # select custom bucket
    c = Couchbase.new("http://localhost:8091/pools/default", :bucket_name => 'blog')
    # specify password for bucket (and SASL auth for memcached client)
    c = Couchbase.new("http://localhost:8091/pools/default", :bucket_name => 'blog',
                      :bucket_password => 'secret')

    # use memcached API
    c.set('password', 'secret')
    c.set('password')             #=> "secret"
    c['password'] = 'secret'
    c['password']                 #=> "secret"
    c['counter'] = 1              #=> 1
    c['counter'] += 1             #=> 2
    c['counter']                  #=> 2
    # more efficient version
    c.increment('counter', 10)

    # cleanup the storage
    c.flush

    # store a couple of posts using memcached API
    c['biking'] = {:title => 'Biking',
                   :body => 'I went to the the pet store earlier and brought home a little kitty...',
                   :date => '2009/01/30 18:04:11'}
    c['bought-a-cat'] = {:title => 'Biking',
                         :body => 'My biggest hobby is mountainbiking. The other day...',
                         :date => '2009/01/30 18:04:11'}
    c['hello-world'] = {:title => 'Hello World',
                        :body => 'Well hello and welcome to my new blog...',
                        :date => '2009/01/15 15:52:20'}
    c.all_docs.count    #=> 3

    # now let's create design doc with sample view
    c.save_design_doc('blog', 'recent_posts' => {'map' => 'function(doc){if(doc.date && doc.title){emit(doc.date, doc.title);}}'})

    # fetch design document _design/blog
    blog = c.design_docs['blog']

    # check that it really has our view defined
    blog.views                            #=> ["recent_posts"]

    # execute view and fetch all docs
    blog.recent_posts                     #=> [#<Couchbase::Document:14244860 {"id"=>"hello-world", "key"=>"2009/01/15 15:52:20", "value"=>"Hello World"}>,...]

    # when you specify :page parameter view results will be fetched with
    # pagination (it uses :skip and :limit parameters, so it can be slow
    # on large datasets)
    c.per_page = 5
    posts = blog.recent_posts.each do |doc|
      # do something
      # with doc object
    end

    # Couchbase::Bucket#all_docs also supports pagination
    [:each_page, :each].map{|method| c.all_docs.respond_to?(method)}    #=> [true, true]

    # You can also you Enumerator to iterate view results
    require 'date'
    posts_by_date = Hash.new{|h,k| h[k] = []}
    enum = c.all_docs.each  # request hasn't issued yet
    enum.inject(posts_by_date) do |acc, doc|
      acc[date] = Date.strptime(doc['date'], '%Y/%m/%d')
      acc
    end
