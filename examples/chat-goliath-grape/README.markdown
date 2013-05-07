# Couchbase + Grape + Goliath = <3

This demo shows how to integrate these cool things to build
asynchronous REST service

# Usage

1. Clone the repository. Navigate to `examples/chat-goliath-grape/`
   directory

2. Install libcouchbase (http://www.couchbase.com/develop/c/current).
   For MacOS users it will look like:

        brew update
        brew install libcouchbase

3. Install all ruby dependencies. This demo has been tested on latest
   stable version of ruby (`ruby 2.0.0p0 (2013-02-24 revision 39474)
   [x86_64-linux]`)

        gem install bundler
        bundle install

4. Setup your Couchbase server
   (http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-getting-started.html).
   It should have `default` bucket. Or you can edit connection options
   in `config/app.rb` file.

5. Start the server up

        ruby app.rb -sv

6. Create an message:

        $ curl -X POST -Fmessage="Hello world" http://localhost:9000/messages
        {"ok":true,"id":"msg:1","cas":11880713153673363456}

7. If you create a design document called `messages` and put a view
   named `all` with this map function:

        function(doc, meta) {
          if (doc.timestamp) {
           emit(meta.id, doc)
          }
        }

   You can fetch all messages with this command:

        curl -X GET http://localhost:9000/messages
        {"ok":true,"messages":[{"id":"msg:1","key":"msg:1","value":{"timestamp":"2013-04-11T12:43:42+03:00","message":"Hello world"},"cas":11880713153673363456}]}
