# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011 Couchbase, Inc.
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
  module RestClient
    def get(uri, options = {})
      execute(:get, uri, options)
    end

    def head(uri, options = {})
      execute(:head, uri, options)
    end

    def delete(uri, options = {})
      execute(:delete, uri, options)
    end

    def post(uri, options = {}, payload = nil)
      execute(:post, uri, options, payload)
    end

    def put(uri, options = {}, payload = nil)
      execute(:put, uri, options, payload)
    end

    protected

    def execute(method, uri, options = {}, payload = nil)
      curl = Curl::Easy.new(build_query(uri, options[:params]))
      curl.verbose = true if Kernel.respond_to?(:debugger)
      curl.headers.update(options[:headers] || {})
      data = case payload
             when IO
               payload
             when Hash
               Yajl::Encoder.encode(payload)
             end
      case method
      when :put
        curl.http_put(data)
      when :post
        curl.http_post(data)
      when :delete
        curl.http_delete
      when :head
        curl.http_head
      when :get
        curl.http_get
      end
      data = Yajl::Parser.parse(curl.body_str)
      if error = HttpStatus::Errors[curl.response_code]
        raise error.new(data['error'], data['reason'])
      else
        data
      end
    end

    def build_query(uri, params = nil)
      return uri if params.nil? || params.empty?
      uri << "?"
      uri << params.map do |k, v|
                          if v.class == Array
                            build_query(v.map { |x| [k, x] })
                          else
                            if %w{key startkey endkey}.include?(k.to_s)
                              v = Yajl::Encoder.encode(v)
                            end
                            "#{escape(k)}=#{escape(v)}"
                          end
                        end.join("&")
    end

    def escape(s)
      s.to_s.gsub(/([^ a-zA-Z0-9_.-]+)/n) {
        '%'+$1.unpack('H2'*bytesize($1)).join('%').upcase
      }.tr(' ', '+')
    end

    # Return the bytesize of String; uses String#size under Ruby 1.8 and
    # String#bytesize under 1.9.
    if ''.respond_to?(:bytesize)
      def bytesize(string)
        string.bytesize
      end
    else
      def bytesize(string)
        string.size
      end
    end
  end
end
