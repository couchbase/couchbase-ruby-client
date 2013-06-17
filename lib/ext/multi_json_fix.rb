# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This patch fixing loading primitives like strings, numbers and booleans
# using json_gem. It amends behaviour of MultiJson::Engines::JsonGem#load
# when it receives JSON string which neither Array nor Object.

if MultiJson.respond_to?(:engine)
  multi_json_engine = MultiJson.send(:engine)
else
  multi_json_engine = MultiJson.send(:adapter)
end

# Patch for MultiJson versions < 1.3.3
require 'multi_json/version'
version = begin
            MultiJson::VERSION
          rescue NameError
            MultiJson::Version.to_s
          end.dup # because Gem::Version modifies it
if Gem::Version.new(version) < Gem::Version.new('1.3.3')
  class << MultiJson
    alias :dump :encode
    alias :load :decode
  end
end

if multi_json_engine.name =~ /JsonGem$/
  class << multi_json_engine
    alias _load_object load
    def load(string, options = {})
      if string.respond_to?(:read)
        string = string.read
      end
      if string =~ /\A\s*[{\[]/
        _load_object(string, options)
      else
        _load_object("[#{string}]", options)[0]
      end
    end
  end
end
