#!/usr/bin/env ruby
["/../../lib", "/.."].each do |path|
  $LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + path))
end

require 'bundler'
Bundler.require

require 'couchbase'

class ChatServer < EM::Connection

  @@clients = []

  def log(message, author = nil)
    Couchbase.bucket.incr("log:key", :initial => 1) do |res|
      entry = {
        'time' => Time.now.utc,
        'author' => author || "[system]",
        'message' => message
      }
      Couchbase.bucket.set("log:#{res.value}", entry)
    end
  end

  def post_init
    @username = nil
    send_data("*** What is your name?\n")
  end

  def receive_data(data)
    if @username
      broadcast(data.strip, @username)
    else
      name = data.gsub(/\s+|[\[\]]/, '').strip[0..20]
      if name.empty?
        send_data("*** What is your name?\n")
      else
        @username = name
        @@clients.push(self)
        broadcast("#{@username} has joined")
        send_data("*** Hi, #{@username}!\n")
      end
    end
  end

  def unbind
    @@clients.delete(self)
    broadcast("#{@username} has left") if @username
  end

  def broadcast(message, author = nil)
    prefix = author ? "<#{@username}>" : "***"
    log(message, author)
    @@clients.each do |client|
      unless client == self
        client.send_data("#{prefix} #{message}\n")
      end
    end
  end

end

EventMachine.run do
  # hit Control + C to stop
  Signal.trap("INT")  { EventMachine.stop }
  Signal.trap("TERM") { EventMachine.stop }

  Couchbase.connection_options = {:async => true, :engine => :eventmachine}
  Couchbase.bucket.on_connect do |res|
    if res.success?
      puts(<<-MOTD.gsub(/^\s+/, ''))
        Hi, this is simple chat server based on EventMachine.
        To join, just use your telnet or netcat clients to connect to
        port 9999 on this machine. Press Ctrl-C to stop it.
      MOTD
      EventMachine.start_server("0.0.0.0", 9999, ChatServer)
    else
      puts "Cannot connect to Couchbase Server: #{res.error}"
    end
  end
end
