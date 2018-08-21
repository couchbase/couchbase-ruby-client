# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2018 Couchbase, Inc.
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

module Couchbase
  class Utils
    def self.encode_params(params)
      params.map do |k, v|
        next if !v && k.to_s == "group"
        if %w(key keys startkey endkey start_key end_key).include?(k.to_s)
          v = MultiJson.dump(v)
        end
        if v.class == Array
          build_query(v.map { |x| [k, x] })
        else
          "#{escape(k)}=#{escape(v)}"
        end
      end.compact.join("&")
    end

    def self.build_query(uri, params = nil)
      uri = uri.dup
      return uri if params.nil? || params.empty?
      uri << "?" << encode_params(params)
    end

    def self.escape(s)
      s.to_s.gsub(/([^ a-zA-Z0-9_.-]+)/nu) do
	'%' + Regexp.last_match(1).unpack('H2' * Regexp.last_match(1).bytesize).join('%').upcase
      end.tr(' ', '+')
    end
  end
end
