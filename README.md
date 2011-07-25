EXAMPLE
=======

``` ruby
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

    # store a couple of posts using memcached API
    c['biking'] = {:title => 'Biking',
                   :body => 'My biggest hobby is mountainbiking. The other day...',
                   :date => '2009/01/30 18:04:11'}
    c['bought-a-cat'] = {:title => 'Biking',
                         :body => 'My biggest hobby is mountainbiking. The other day...',
                         :date => '2009/01/30 18:04:11'}
    c['hello-world'] = {:title => 'Hello World',
                        :body => 'Well hello and welcome to my new blog...',
                        :date => '2009/01/15 15:52:20'}

    # now let's create design doc with sample view
    c.save_design_doc('blog', 'recent_posts' => {'map' => 'function(doc){if(doc.date && doc.title){emit(doc.date, doc.title);}}'})

    # fetch design document _design/blog
    blog = c.design_docs['blog']

    # check that it really has our view defined
    blog.views                            #=> ["recent_posts"]

    # execute view and fetch all docs
    blog.recent_posts                     #=> [#<Couchbase::Document:14244860 {"id"=>"hello-world", "key"=>"2009/01/15 15:52:20", "value"=>"Hello World"}>,...]

    # when you specify :page parameter view results will be fetched with pagination (it uses :skip
    # and :limit parameters, so it can be slow on large datasets)
    c.per_page = 5
    posts = blog.recent_posts(:page => 1) #=> [#<Couchbase::Document:8718590...>, ...]
    # or use :per_page params to override defaults
    posts = blog.recent_posts(:page => 1, :per_page => 10) #=> [#<Couchbase::Document:8718590...>, ...]
    posts.current_page                    #=> 1
```
