EXAMPLE
=======

``` ruby
require 'couchbase'

# establish connection with couchbase default pool and default bucket
c = Couchbase.new("http://localhost:8091/pools/default")
# fetch design document _design/blog
blog = c.design_docs['blog']
# get list of views
blog.views  #=> ["recent_posts"]
# execute view and fetch docs
blog.recent_posts
```
