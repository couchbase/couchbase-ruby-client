FROM ruby:4.0-trixie

WORKDIR /app
COPY ./ ./

RUN apt-get update && apt-get install -y cmake build-essential libssl-dev protobuf-compiler

RUN bundle install
RUN bundle exec rake compile

WORKDIR /app/fit-performer

RUN bundle exec rake generate

ENTRYPOINT ["bundle", "exec", "ruby", "server.rb"]
