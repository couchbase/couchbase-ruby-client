# Build steps for Windows

Ruby SDK on windows is using MSYS toolchain.

This document describes steps to build module for Windows OS.

Download Windows VM image (or use another method to get running machine with Windows OS):

https://developer.microsoft.com/en-us/windows/downloads/virtual-machines/

Install scoop package manager (https://scoop.sh/):

    Set-ExecutionPolicy RemoteSigned -scope CurrentUser
    iwr -useb get.scoop.sh | iex

Install tools and dependencies:

    scoop install git cmake ruby msys2

Install `ridk` dependencies

    ridk install 1
    ridk install 3

Install OpenSSL

    ridk exec pacman -S --noconfirm mingw-w64-ucrt-x86_64-openssl

Clone Couchbase SDK source code (note `--recurse-submodules`):

    git clone --recurse-submodules https://github.com/couchbaselabs/couchbase-ruby-client c:\users\user\couchbase-ruby-client

Navigate to source tree:

    c:\users\user\couchbase-ruby-client

Install SDK dependencies

    bundle install

Buld the module

    bundle exec rake compile

# Verification steps for Windows

Navigate to source tree:

    c:\users\user\couchbase-ruby-client

Run the tests (it will download `gocaves` and might ask for firewall permissions)

    bundle exec rake test

To run against Couchbase Server

    bundle exec rake test TEST_CONNECTION_STRING=couchbase://127.0.0.1 TEST_USERNAME=Administrator TEST_PASSWORD=password
