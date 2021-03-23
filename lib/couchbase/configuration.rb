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

require "yaml"
require "erb"

module Couchbase
  class Configuration
    attr_accessor :connection_string
    attr_accessor :username
    attr_accessor :password

    def initialize
      @connection_string = "couchbase://localhost"
    end

    def load!(path, environment = default_environment_name)
      settings = load_yaml(path, environment)
      load_configuration(settings)
    end

    private

    def load_configuration(settings)
      configuration = settings.with_indifferent_access
      @connection_string = configuration[:connection_string]
      @username = configuration[:username]
      @password = configuration[:password]
    end

    def load_yaml(path, environment)
      file_content = ERB.new(File.read(path)).result
      YAML.safe_load(file_content, aliases: :default)[environment]
    end

    def default_environment_name
      if defined?(::Rails)
        ::Rails.env
      elsif defined?(::Sinatra)
        ::Sinatra::Base.environment.to_s
      else
        ENV["RACK_ENV"] || ENV["COUCHBASE_ENV"] or raise ::Couchbase::Error::NoEnvironment
      end
    end
  end
end
