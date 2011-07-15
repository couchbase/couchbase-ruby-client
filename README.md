EXAMPLE
=======

``` ruby
    require 'couchbase'

    # establish connection with couchbase default pool and default bucket
    c = Couchbase.new("http://localhost:8091/pools/default")
    # fetch design document _design/blog
    blog = c.design_docs['blog']
    # get list of views
    blog.views                            #=> ["recent_posts"]
    # execute view and fetch docs
    blog.recent_posts
    # with pagination (it uses :skip and :limit parameters, so it can be slow on large datasets
    c.per_page = 5
    posts = blog.recent_posts(:page => 1) #=> [#<Couchbase::Document:8718590...>, ...]
    posts.current_page                    #=> 1

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
```
