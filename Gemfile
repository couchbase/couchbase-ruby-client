#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

source "https://rubygems.org"
git_source(:github) { |repo_name| "https://github.com/#{repo_name}" }

gemspec

gem "rake"

group :development do
  gem "activesupport", "~> 7.0.3"
  gem "faker"
  gem "flay"
  gem "flog"
  gem "gem-compiler"
  gem "grpc-tools", "~> 1.59"
  gem "heckle"
  gem "minitest"
  gem "minitest-reporters"
  gem "rack"
  gem "reek"
  gem "rubocop"
  gem "rubocop-minitest"
  gem "rubocop-packaging"
  gem "rubocop-performance"
  gem "rubocop-rake"
  gem "rubocop-thread_safety"
  gem "simplecov-cobertura"
  gem "yard"
  platforms :mri do
    gem "byebug"
  end
end
