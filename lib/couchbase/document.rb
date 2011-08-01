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

require 'will_paginate'

module Couchbase
  class Document
    # Undefine as much methods as we can to free names for views
    instance_methods.each do |m|
      undef_method(m) if m.to_s !~ /(?:^__|^nil\?$|^send$|^object_id$|^class$|)/
    end

    attr_accessor :data, :views

    def initialize(connection, data)
      @data = data
      @connection = connection
      @views = []
      begin
        if design_doc?
          data['views'].each do |name, funs|
            @views << name
              self.instance_eval <<-EOV, __FILE__, __LINE__ + 1
              def #{name}(params = {})
                endpoint = "\#{@connection.next_node.couch_api_base}/\#{@data['_id']}/_view/#{name}"
                if params[:page]
                  fetch_view_with_pagination(endpoint, params)
                else
                  fetch_view(endpoint, params)
                end
              end
            EOV
          end
        end
      rescue NoMethodError
      end
    end

    def self.wrap(connection, data)
      Document.new(connection, data['doc'] || data)
    end

    def [](key)
      @data[key]
    end

    def []=(key, value)
      @data[key] = value
    end

    def design_doc?
      !!(@data['_id'] =~ %r(_design/))
    end

    def has_views?
      !!(design_doc? && !@views.empty?)
    end

    def inspect
      %(#<#{self.class.name}:#{self.object_id} #{@data.inspect}>)
    end

    protected

    def fetch_view(endpoint, params = {})
      docs = @connection.http_get(endpoint, :params => params)
      docs['rows'].map{|d| Document.new(self, d)}
    end

    def fetch_view_with_pagination(endpoint, params = {})
      page = params[:page].to_i
      raise ArgumentError, ":page parameter should be a natural number" if page < 1
      per_page = (params[:per_page] || @connection.per_page).to_i
      skip = (page-1) * per_page
      docs = @connection.http_get(endpoint, :params => params.merge(:skip => skip, :limit => per_page))
      collection = docs['rows'].map{|d| Document.new(self, d)}
      total_entries = docs['total_rows']
      WillPaginate::Collection.create(page, per_page, total_entries) do |pager|
        pager.replace(collection)
      end
    end
  end
end
