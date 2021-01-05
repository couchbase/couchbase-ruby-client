#    Copyright 2020 Couchbase, Inc.
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

require "rails"

require "couchbase/configuration"

module Rails
  module Couchbase
    class Railtie < ::Rails::Railtie
      def self.rescue_responses
        {
          "Couchbase::Error::DocumentNotFound" => :not_found,
        }
      end

      config.action_dispatch.rescue_responses&.merge!(rescue_responses)

      config.couchbase = ::Couchbase::Configuration.new

      initializer "couchbase.load-config" do
        config_path = Rails.root.join("config", "couchbase.yml")
        if config_path.file?
          begin
            config.couchbase.load!(config_path)
          rescue ::Couchbase::Error::CouchbaseError => e
            puts "There is a configuration error with the current couchbase.yml"
            puts e.message
          end
        end
      end
    end
  end
end
