# Chat Demo

This is simple demo of the chat built on EventMachine, and using
Couchbase to store logs.

# Quick Start and Usage

Navigate to the example directory and install dependencies:

    $ cd examples/chat-em
    $ bundle install

Execute the server

    $ ruby ./server.rb
    Hi, this is simple chat server based on EventMachine.
    To join, just use your telnet or netcat clients to connect to
    port 9999 on this machine. Press Ctrl-C to stop it.

Use telnet to join the chat

    $ telnet localhost 9999
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.
    *** What is your name?
    avsej
    *** Hi, avsej!
    Hi everyone in this chat

The server will broadcast all your messages and record any event to
the Couchbase server. If your server hosted not on the localhost or
using bucket different from "default" you might want to change the
connection options at the bottom of the `server.rb`, for example in
this case it will connect to the bucket "protected" with password
"secret".

    Couchbase.connection_options = {
                :async => true,
                :engine => :eventmachine,
                :bucket => "protected",
                :password => "secret"
              }

Happy hacking!
