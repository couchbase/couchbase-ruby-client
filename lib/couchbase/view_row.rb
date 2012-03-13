# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2012 Couchbase, Inc.
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
  # This class encapsulates structured JSON document
  #
  # @since 1.2.0
  #
  # It behaves like Hash, but also defines special methods for each view if
  # the documnent considered as Design document.
  #
  # @see http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-datastore.html
  class ViewRow

    # Undefine as much methods as we can to free names for views
    instance_methods.each do |m|
      undef_method(m) if m.to_s !~ /(?:^__|^nil\?$|^send$|^object_id$|^class$|)/
    end

    # The hash built from JSON document.
    #
    # @since 1.2.0
    #
    # This is complete response from the Couchbase
    #
    # @return [Hash]
    attr_accessor :data

    # The key which was emitted by map function
    #
    # @since 1.2.0
    #
    # @see http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-writing-map.html
    #
    # Usually it is String (the object +_id+) but it could be also any
    # compount JSON value.
    #
    # @return [Object]
    attr_accessor :key

    # The value which was emitted by map function
    #
    # @since 1.2.0
    #
    # @see http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-writing-map.html
    #
    # @return [Object]
    attr_accessor :value

    # The document hash.
    #
    # @since 1.2.0
    #
    # It usually available when view executed with +:include_doc+ argument.
    #
    # @return [Hash]
    attr_accessor :doc

    # The identificator of the document
    #
    # @since 1.2.0
    #
    # @return [String]
    attr_accessor :id

    # The list of views defined or empty array
    #
    # @since 1.2.0
    #
    # @return [Array<View>]
    attr_accessor :views

    # Initialize the document instance
    #
    # @since 1.2.0
    #
    # It takes reference to the bucket, data hash. It will define view
    # methods if the data object looks like design document.
    #
    # @param [Couchbase::Bucket] bucket the reference to connection
    # @param [Hash] data the data hash, which was built from JSON document
    #   representation
    def initialize(bucket, data)
      @bucket = bucket
      @data = data
      @key = data['key']
      @value = data['value']
      @doc = data['doc']
      @id = if @value.is_a?(Hash) && @value['_id']
              @value['_id']
            else
              @data['id']
            end
      @views = []
      if design_doc?
        @doc['views'].each do |name, _|
          @views << name
          self.instance_eval <<-EOV, __FILE__, __LINE__ + 1
            def #{name}(params = {})
              View.new(@bucket, "\#{@id}/_view/#{name}", params)
            end
          EOV
        end
      end
      @meta = {}
      if @doc
        @doc.keys.each do |key|
          if key.start_with?("$")
            @meta[key.sub(/^\$/, '')] = @doc.delete(key)
          end
        end
      end
    end

    # Wraps data hash into ViewRow instance
    #
    # @since 1.2.0
    #
    # @see ViewRow#initialize
    #
    # @param [Couchbase::Bucket] bucket the reference to connection
    # @param [Hash] data the data hash, which was built from JSON document
    #   representation
    #
    # @return [ViewRow]
    def self.wrap(bucket, data)
      ViewRow.new(bucket, data)
    end

    # Get attribute of the document
    #
    # @since 1.2.0
    #
    # Fetches attribute from underlying document hash
    #
    # @param [String] key the attribute name
    #
    # @return [Object] property value or nil
    def [](key)
      @doc[key]
    end

    # Check attribute existence
    #
    # @since 1.2.0
    #
    # @param [String] key the attribute name
    #
    # @return [true, false] +true+ if the given attribute is present in in
    #   the document.
    def has_key?(key)
      @doc.has_key?(key)
    end

    # Set document attribute
    #
    # @since 1.2.0
    #
    # Set or update the attribute in the document hash
    #
    # @param [String] key the attribute name
    # @param [Object] value the attribute value
    #
    # @return [Object] the value
    def []=(key, value)
      @doc[key] = value
    end

    # Check if the document is design
    #
    # @since 1.2.0
    #
    # @return [true, false]
    def design_doc?
      !!(@doc && @id =~ %r(_design/))
    end

    # Check if the document has views defines
    #
    # @since 1.2.0
    #
    # @see ViewRow#views
    #
    # @return [true, false] +true+ if the document have views
    def has_views?
      !!(design_doc? && !@views.empty?)
    end

    def inspect
      desc = "#<#{self.class.name}:#{self.object_id} "
      desc << [:@id, :@key, :@value, :@doc, :@meta, :@views].map do |iv|
        "#{iv}=#{instance_variable_get(iv).inspect}"
      end.join(' ')
      desc << ">"
      desc
    end
  end
end
