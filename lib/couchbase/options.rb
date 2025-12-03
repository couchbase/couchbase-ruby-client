# frozen_string_literal: true

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

require "couchbase/utils/time"
require "couchbase/config_profiles"
require "couchbase/json_transcoder"

module Couchbase
  # Definition of the Option classes for data APIs
  module Options # rubocop:disable Metrics/ModuleLength
    # Base class for most of the options
    class Base
      attr_accessor :timeout # @return [Integer, #in_milliseconds, nil]
      attr_accessor :retry_strategy # @return [Proc, nil]
      attr_accessor :client_context # @return [Hash, nil]
      attr_accessor :parent_span # @return [Span, nil]

      # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Base]
      def initialize(timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        @timeout = timeout
        @retry_strategy = retry_strategy
        @client_context = client_context
        @parent_span = parent_span
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end
    end

    # Options for {Collection#get}
    class Get < Base
      attr_accessor :with_expiry # @return [Boolean]
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String, Integer)]

      # Creates an instance of options for {Collection#get}
      #
      # @param [Array<String>] projections a list of paths that should be loaded if present.
      # @param [Boolean] with_expiry if +true+ the expiration will be also fetched with {Collection#get}
      # @param [JsonTranscoder, #decode(String, Integer)] transcoder used for decoding
      #
      # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Get] self
      def initialize(projections: [],
                     with_expiry: false,
                     transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @projections = projections
        @with_expiry = with_expiry
        @transcoder = transcoder
        @preserve_array_indexes = false
        yield self if block_given?
      end

      # Allows to specify a custom list paths to fetch from the document instead of the whole.
      #
      # Note that a maximum of 16 individual paths can be projected at a time due to a server limitation. If you need
      # more than that, think about fetching less-generic paths or the full document straight away.
      #
      # @param [String, Array<String>] paths a path that should be loaded if present.
      def project(*paths)
        @projections ||= []
        @projections |= paths.flatten # union with current projections
      end

      # @api private
      # @return [Boolean] whether to use sparse arrays (default +false+)
      attr_accessor :preserve_array_indexes

      # @api private
      # @return [Array<String>] list of paths to project
      attr_accessor :projections

      # @api private
      # @return [Boolean]
      def need_projected_get?
        @with_expiry || !@projections&.empty?
      end

      # @api private
      def to_backend
        options = {
          timeout: Utils::Time.extract_duration(@timeout),
        }
        options.update(with_expiry: true) if @with_expiry
        unless @projections&.empty?
          options.update({
            projections: @projections,
            preserve_array_indexes: @preserve_array_indexes,
          })
        end
        options
      end

      # @api private
      DEFAULT = Get.new.freeze
    end

    # Options for {Collection#get_multi}
    class GetMulti < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String, Integer)]

      # Creates an instance of options for {Collection#get_multi}
      #
      # @param [JsonTranscoder, #decode(String, Integer)] transcoder used for decoding
      #
      # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [GetMulti] self
      def initialize(transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end

      # @api private
      DEFAULT = GetMulti.new.freeze
    end

    # Options for {Collection#get_and_lock}
    class GetAndLock < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String, Integer)]

      # Creates an instance of options for {Collection#get_and_lock}
      #
      # @param [JsonTranscoder, #decode(String, Integer)] transcoder used for decoding
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [GetAndLock] self
      def initialize(transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end

      # @api private
      DEFAULT = GetAndLock.new.freeze
    end

    # Options for {Collection#get_and_touch}
    class GetAndTouch < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String, Integer)]

      # Creates an instance of options for {Collection#get_and_touch}
      #
      # @param [JsonTranscoder, #decode(String, Integer)] transcoder used for decoding
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [GetAndTouch] self
      def initialize(transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end

      # @api private
      DEFAULT = GetAndTouch.new.freeze
    end

    # Options for {Collection#get_all_replicas}
    class GetAllReplicas < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String, Integer)]
      attr_accessor :read_preference # @return [Symbol]

      # Creates an instance of options for {Collection#get_all_replicas}
      #
      # @param [JsonTranscoder, #decode(String, Integer)] transcoder used for decoding
      # @param [Symbol] read_preference decides how the replica nodes will be selected.
      #  +:no_preference+:: no preference and will select any available replica. This is the default
      #  +:selected_server_group+:: restrict to nodes in {Cluster#preferred_server_group}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [GetAllReplicas] self
      def initialize(transcoder: JsonTranscoder.new,
                     read_preference: :no_preference,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        @read_preference = read_preference
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          read_preference: @read_preference,
        }
      end

      # @api private
      DEFAULT = GetAllReplicas.new.freeze
    end

    # Options for {Collection#get_any_replica}
    class GetAnyReplica < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String, Integer)]
      attr_accessor :read_preference # @return [Symbol]

      # Creates an instance of options for {Collection#get_any_replica}
      #
      # @param [JsonTranscoder, #decode(String, Integer)] transcoder used for decoding
      # @param [Symbol] read_preference decides how the replica nodes will be selected.
      #  +:no_preference+:: no preference and will select any available replica. This is the default
      #  +:selected_server_group+:: restrict to nodes in {Cluster#preferred_server_group}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [GetAnyReplica] self
      def initialize(transcoder: JsonTranscoder.new,
                     read_preference: :no_preference,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        @read_preference = read_preference
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          read_preference: @read_preference,
        }
      end

      # @api private
      DEFAULT = GetAnyReplica.new.freeze
    end

    # Options for {Collection#exists}
    class Exists < Base
      # Creates an instance of options for {Collection#exists}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Exists self
      def initialize(timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end

      # @api private
      DEFAULT = Exists.new.freeze
    end

    # Options for {Collection#touch}
    class Touch < Base
      # Creates an instance of options for {Collection#touch}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [TouchOptions] self
      def initialize(timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end

      # @api private
      DEFAULT = Touch.new.freeze
    end

    # Options for {Collection#unlock}
    class Unlock < Base
      # Creates an instance of options for {Collection#unlock}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Unlock] self
      def initialize(timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
        }
      end

      # @api private
      DEFAULT = Unlock.new.freeze
    end

    # Options for {Collection#remove}
    class Remove < Base
      attr_accessor :cas # @return [Integer, nil]
      attr_accessor :durability_level # @return [Symbol]

      # Creates an instance of options for {Collection#remove}
      #
      # @param [Integer, nil] cas CAS value for optimistic locking
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Remove]
      def initialize(cas: nil,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @cas = cas
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
          cas: @cas,
        }
      end

      # @api private
      DEFAULT = Remove.new.freeze
    end

    # Options for {Collection#remove_multi}
    class RemoveMulti < Base
      attr_accessor :durability_level # @return [Symbol]

      # Creates an instance of options for {Collection#remove}
      #
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Remove]
      def initialize(durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = RemoveMulti.new.freeze
    end

    # Options for {Collection#insert}
    class Insert < Base
      attr_accessor :expiry # @return [Integer, #in_seconds, nil]
      attr_accessor :transcoder # @return [JsonTranscoder, #encode(Object)]
      attr_accessor :durability_level # @return [Symbol]

      # Creates an instance of options for {Collection#insert}
      #
      # @param [Integer, #in_seconds, Time, nil] expiry expiration time to associate with the document
      # @param [JsonTranscoder, #encode(Object)] transcoder used for encoding
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Insert]
      def initialize(expiry: nil,
                     transcoder: JsonTranscoder.new,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @expiry = expiry
        @transcoder = transcoder
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          expiry: Utils::Time.extract_expiry_time(@expiry),
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = Insert.new.freeze
    end

    # Options for {Collection#upsert}
    class Upsert < Base
      attr_accessor :expiry # @return [Integer, #in_seconds, nil]
      attr_accessor :transcoder # @return [JsonTranscoder, #encode(Object)]
      attr_accessor :durability_level # @return [Symbol]
      attr_accessor :preserve_expiry # @return [Boolean]

      # Creates an instance of options for {Collection#upsert}
      #
      # @param [Integer, #in_seconds, Time, nil] expiry expiration time to associate with the document
      # @param [Boolean] preserve_expiry if true and the document exists, the server will preserve current expiration
      #  for the document, otherwise will use {expiry} from the operation.
      # @param [JsonTranscoder, #encode(Object)] transcoder used for encoding
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Upsert]
      def initialize(expiry: nil,
                     preserve_expiry: false,
                     transcoder: JsonTranscoder.new,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @expiry = expiry
        @preserve_expiry = preserve_expiry
        @transcoder = transcoder
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          expiry: Utils::Time.extract_expiry_time(@expiry),
          preserve_expiry: @preserve_expiry,
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = Upsert.new.freeze
    end

    # Options for {Collection#upsert_multi}
    class UpsertMulti < Base
      attr_accessor :expiry # @return [Integer, #in_seconds, nil]
      attr_accessor :transcoder # @return [JsonTranscoder, #encode(Object)]
      attr_accessor :durability_level # @return [Symbol]
      attr_accessor :preserve_expiry # @return [Boolean]

      # Creates an instance of options for {Collection#upsert}
      #
      # @param [Integer, #in_seconds, Time, nil] expiry expiration time to associate with the document
      # @param [Boolean] preserve_expiry if true and the document exists, the server will preserve current expiration
      #  for the document, otherwise will use {expiry} from the operation.
      # @param [JsonTranscoder, #encode(Object)] transcoder used for encoding
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Upsert]
      def initialize(expiry: nil,
                     preserve_expiry: false,
                     transcoder: JsonTranscoder.new,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @expiry = expiry
        @preserve_expiry = preserve_expiry
        @transcoder = transcoder
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          expiry: Utils::Time.extract_expiry_time(@expiry),
          preserve_expiry: @preserve_expiry,
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = UpsertMulti.new.freeze
    end

    # Options for {Collection#replace}
    class Replace < Base
      attr_accessor :expiry # @return [Integer, #in_seconds, nil]
      attr_accessor :transcoder # @return [JsonTranscoder, #encode(Object)]
      attr_accessor :cas # @return [Integer, nil]
      attr_accessor :durability_level # @return [Symbol]
      attr_accessor :preserve_expiry # @return [Boolean]

      # Creates an instance of options for {Collection#replace}
      #
      # @param [Integer, #in_seconds, nil] expiry expiration time to associate with the document
      # @param [Boolean] preserve_expiry if true and the document exists, the server will preserve current expiration
      #  for the document, otherwise will use {expiry} from the operation.
      # @param [JsonTranscoder, #encode(Object)] transcoder used for encoding
      # @param [Integer, nil] cas a CAS value that will be taken into account on the server side for optimistic concurrency
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Replace]
      def initialize(expiry: nil,
                     preserve_expiry: false,
                     transcoder: JsonTranscoder.new,
                     cas: nil,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @expiry = expiry
        @preserve_expiry = preserve_expiry
        @transcoder = transcoder
        @cas = cas
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          expiry: Utils::Time.extract_expiry_time(@expiry),
          preserve_expiry: @preserve_expiry,
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
          cas: @cas,
        }
      end

      # @api private
      DEFAULT = Replace.new.freeze
    end

    # Options for {Collection#mutate_in}
    class MutateIn < Base
      attr_accessor :expiry # @return [Integer, #in_seconds, nil]
      attr_accessor :store_semantics # @return [Symbol]
      attr_accessor :cas # @return [Integer, nil]
      attr_accessor :durability_level # @return [Symbol]
      attr_accessor :transcoder # @return [JsonTranscoder, #encode(Object)]
      attr_accessor :preserve_expiry # @return [Boolean]

      # Creates an instance of options for {Collection#mutate_in}
      #
      # @param [Integer, #in_seconds, Time, nil] expiry expiration time to associate with the document
      # @param [Boolean] preserve_expiry if true and the document exists, the server will preserve current expiration
      #  for the document, otherwise will use {expiry} from the operation.
      # @param [Symbol] store_semantics describes how the outer document store semantics on subdoc should act
      #  +:replace+:: replace the document, fail if it does not exist. This is the default
      #  +:upsert+:: replace the document or create if it does not exist
      #  +:insert+:: create the document, fail if it exists
      # @param [Integer, nil] cas a CAS value that will be taken into account on the server side for optimistic concurrency
      # @param [Boolean] access_deleted for internal use only: allows access to deleted documents that are in "tombstone" form
      # @param [Boolean] create_as_deleted for internal use only: allows creating documents in "tombstone" form
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      # @param [JsonTranscoder, #encode(Object)] transcoder used for encoding
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [MutateIn]
      def initialize(expiry: nil,
                     preserve_expiry: false,
                     store_semantics: :replace,
                     cas: nil,
                     access_deleted: false,
                     create_as_deleted: false,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @expiry = expiry
        @preserve_expiry = preserve_expiry
        @store_semantics = store_semantics
        @cas = cas
        @access_deleted = access_deleted
        @create_as_deleted = create_as_deleted
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        @transcoder = transcoder
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          expiry: Utils::Time.extract_expiry_time(@expiry),
          preserve_expiry: @preserve_expiry,
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
          cas: @cas,
          store_semantics: @store_semantics,
          access_deleted: @access_deleted,
          create_as_deleted: @create_as_deleted,
        }
      end

      # @api private
      # @return [Boolean]
      attr_accessor :access_deleted

      # @api private
      # @return [Boolean]
      attr_accessor :create_as_deleted

      # @api private
      DEFAULT = MutateIn.new.freeze
    end

    # Options for {Collection#lookup_in}
    class LookupIn < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]

      # Creates an instance of options for {Collection#lookup_in}
      #
      # @param [Boolean] access_deleted for internal use only: allows access to deleted documents that are in "tombstone" form
      # @param [JsonTranscoder, #decode(String)] transcoder used for encoding
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [LookupIn] self
      def initialize(access_deleted: false,
                     transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @access_deleted = access_deleted
        @transcoder = transcoder
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          access_deleted: @access_deleted,
        }
      end

      # @api private
      # @return [Boolean]
      attr_accessor :access_deleted

      # @api private
      DEFAULT = LookupIn.new.freeze
    end

    # Options for {Collection#lookup_in_any_replica}
    class LookupInAnyReplica < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]
      attr_accessor :read_preference # @return [Symbol]

      # Creates an instance of options for {Collection#lookup_in_any_replica}
      #
      # @param [JsonTranscoder, #decode(String)] transcoder used for encoding
      # @param [Symbol] read_preference decides how the replica nodes will be selected.
      #  +:no_preference+:: no preference and will select any available replica. This is the default
      #  +:selected_server_group+:: restrict to nodes in {Cluster#preferred_server_group}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [LookupIn] self
      def initialize(transcoder: JsonTranscoder.new,
                     read_preference: :no_preference,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        @read_preference = read_preference
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          read_preference: @read_preference,
        }
      end

      # @api private
      # @return [Boolean]
      attr_accessor :access_deleted

      # @api private
      DEFAULT = LookupInAnyReplica.new.freeze
    end

    # Options for {Collection#lookup_in_all_replicas}
    class LookupInAllReplicas < Base
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]
      attr_accessor :read_preference # @return [Symbol]

      # Creates an instance of options for {Collection#lookup_in_all_replicas}
      #
      # @param [JsonTranscoder, #decode(String)] transcoder used for encoding
      # @param [Symbol] read_preference decides how the replica nodes will be selected.
      #  +:no_preference+:: no preference and will select any available replica. This is the default
      #  +:selected_server_group+:: restrict to nodes in {Cluster#preferred_server_group}
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [LookupInAllReplicas] self
      def initialize(transcoder: JsonTranscoder.new,
                     read_preference: :no_preference,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @transcoder = transcoder
        @read_preference = read_preference
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          read_preference: @read_preference,
        }
      end

      # @api private
      DEFAULT = LookupInAllReplicas.new.freeze
    end

    # Options for {Collection#scan}
    class Scan < Base
      attr_accessor :ids_only # @return [Boolean]
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]
      attr_accessor :mutation_state # @return [MutationState, nil]
      attr_accessor :batch_byte_limit # @return [Integer, nil]
      attr_accessor :batch_item_limit # @return [Integer, nil]
      attr_accessor :concurrency # @return [Integer, nil]

      # Creates an instance of options for {Collection#scan}
      #
      # @param [Boolean] ids_only if set to true, the content of the documents is not included in the results
      # @param [JsonTranscoder, #decode(String)] transcoder used for decoding
      # @param [MutationState, nil] mutation_state sets the mutation tokens this scan should be consistent with
      # @param [Integer, nil] batch_byte_limit allows to limit the maximum amount of bytes that are sent from the server
      #   to the client on each partition batch, defaults to 15,000
      # @param [Integer, nil] batch_item_limit allows to limit the maximum amount of items that are sent from the server
      #   to the client on each partition batch, defaults to 50
      # @param [Integer, nil] concurrency specifies the maximum number of partitions that can be scanned concurrently,
      #   defaults to 1
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [LookupIn] self
      def initialize(ids_only: false,
                     transcoder: JsonTranscoder.new,
                     mutation_state: nil,
                     batch_byte_limit: nil,
                     batch_item_limit: nil,
                     concurrency: nil,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @ids_only = ids_only
        @transcoder = transcoder
        @mutation_state = mutation_state
        @batch_byte_limit = batch_byte_limit
        @batch_item_limit = batch_item_limit
        @concurrency = concurrency
        yield self if block_given?
      end

      # Sets the mutation tokens this query should be consistent with
      #
      # @note overrides consistency level set by {#scan_consistency=}
      #
      # @param [MutationState] mutation_state the mutation state containing the mutation tokens
      def consistent_with(mutation_state)
        @mutation_state = mutation_state
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          ids_only: @ids_only,
          mutation_state: @mutation_state.to_a,
          batch_byte_limit: @batch_byte_limit,
          batch_item_limit: @batch_item_limit,
          concurrency: @concurrency,
        }
      end

      DEFAULT = Scan.new.freeze
    end

    # Options for {BinaryCollection#append}
    class Append < Base
      attr_accessor :cas # @return [Integer]
      attr_accessor :durability_level # @return [Symbol]
      attr_accessor :replicate_to # @return [Symbol]
      attr_accessor :persist_to # @return [Symbol]

      # Creates an instance of options for {BinaryCollection#append}
      #
      # @param [Integer] cas The default CAS used (0 means no CAS in this context)
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Append] self
      def initialize(cas: nil,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @cas = cas

        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @durability_level = durability_level
        @replicate_to = replicate_to
        @persist_to = persist_to
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          cas: @cas,
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = Append.new.freeze
    end

    # Options for {BinaryCollection#prepend}
    class Prepend < Base
      attr_accessor :cas # @return [Integer]
      attr_accessor :durability_level # @return [Symbol]
      attr_accessor :replicate_to # @return [Symbol]
      attr_accessor :persist_to # @return [Symbol]

      # Creates an instance of options for {BinaryCollection#prepend}
      #
      # @param [Integer] cas The default CAS used (0 means no CAS in this context)
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Prepend] self
      def initialize(cas: nil,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @cas = cas

        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @durability_level = durability_level
        @replicate_to = replicate_to
        @persist_to = persist_to
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          cas: @cas,
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = Prepend.new.freeze
    end

    # Options for {BinaryCollection#increment}
    class Increment < Base
      attr_reader :delta # @return [Integer]
      attr_accessor :initial # @return [Integer]
      attr_accessor :expiry # @return [Integer, #in_seconds]
      attr_accessor :durability_level # @return [Symbol]

      # Creates an instance of options for {BinaryCollection#increment}
      #
      # @param [Integer] delta the delta for the operation
      # @param [Integer] initial if present, holds the initial value
      # @param [Integer, #in_seconds, Time, nil] expiry if set, holds the expiration for the operation
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Increment] self
      def initialize(delta: 1,
                     initial: nil,
                     expiry: nil,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        raise ArgumentError, "the delta cannot be less than 0" if delta.negative?

        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @delta = delta
        @initial = initial
        @expiry = expiry
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      # @param [Integer] value delta for the operation
      def delta=(value)
        raise ArgumentError, "the delta cannot be less than 0" if delta.negative?

        @delta = value
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          delta: @delta,
          initial_value: @initial,
          expiry: Utils::Time.extract_expiry_time(@expiry),
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = Increment.new.freeze
    end

    # Options for {BinaryCollection#decrement}
    class Decrement < Base
      attr_reader :delta # @return [Integer]
      attr_accessor :initial # @return [Integer]
      attr_accessor :expiry # @return [Integer, #in_seconds]
      attr_accessor :durability_level # @return [Symbol]

      # Creates an instance of options for {BinaryCollection#decrement}
      #
      # @param [Integer] delta the delta for the operation
      # @param [Integer] initial if present, holds the initial value
      # @param [Integer, #in_seconds, Time, nil] expiry if set, holds the expiration for the operation
      # @param [Symbol] durability_level level of durability
      #  +:none+::
      #     no enhanced durability required for the mutation
      #  +:majority+::
      #     the mutation must be replicated to a majority of the Data Service nodes
      #     (that is, held in the memory allocated to the bucket)
      #  +:majority_and_persist_to_active+::
      #     The mutation must be replicated to a majority of the Data Service nodes.
      #     Additionally, it must be persisted (that is, written and synchronised to disk) on the
      #     node hosting the active partition (vBucket) for the data.
      #  +:persist_to_majority+::
      #     The mutation must be persisted to a majority of the Data Service nodes.
      #     Accordingly, it will be written to disk on those nodes.
      # @param [Symbol] replicate_to number of nodes to replicate
      #  +:none+:: do not apply any replication requirements.
      #  +:one+::  wait for replication to at least one node.
      #  +:two+::  wait for replication to at least two nodes.
      #  +:three+:: wait for replication to at least three nodes.
      # @param [Symbol] persist_to number of nodes to persist
      #  +:none+:: do not apply any persistence requirements.
      #  +:active+::  wait for persistence to active node
      #  +:one+::  wait for persistence to at least one node.
      #  +:two+:: wait for persistence to at least two nodes.
      #  +:three+:: wait for persistence to at least three nodes.
      #  +:four+:: wait for persistence to four nodes (active and replicas).
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Decrement] self
      def initialize(delta: 1,
                     initial: nil,
                     expiry: nil,
                     durability_level: :none,
                     replicate_to: :none,
                     persist_to: :none,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        raise ArgumentError, "the delta cannot be less than 0" if delta.negative?

        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @delta = delta
        @initial = initial
        @expiry = expiry
        if durability_level != :none && (replicate_to != :none || persist_to != :none)
          raise ArgumentError, "durability_level conflicts with replicate_to and persist_to options"
        end

        @persist_to = persist_to
        @replicate_to = replicate_to
        @durability_level = durability_level
        yield self if block_given?
      end

      # @param [Integer] value delta for the operation
      def delta=(value)
        raise ArgumentError, "the delta cannot be less than 0" if delta.negative?

        @delta = value
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          delta: @delta,
          initial_value: @initial,
          expiry: Utils::Time.extract_expiry_time(@expiry),
          durability_level: @durability_level,
          persist_to: @persist_to,
          replicate_to: @replicate_to,
        }
      end

      # @api private
      DEFAULT = Decrement.new.freeze
    end

    # Options for {Datastructures::CouchbaseList#initialize}
    class CouchbaseList
      attr_accessor :get_options # @return [Get]
      attr_accessor :remove_options # @return [Remove]
      attr_accessor :lookup_in_options # @return [LookupIn]
      attr_accessor :mutate_in_options # @return [MutateIn]

      # Creates an instance of options for {CouchbaseList#initialize}
      #
      # @param [Get] get_options
      # @param [Remove] remove_options
      # @param [LookupIn] lookup_in_options
      # @param [MutateIn] mutate_in_options
      #
      # @yieldparam [CouchbaseList]
      def initialize(get_options: Get.new,
                     remove_options: Remove.new,
                     lookup_in_options: LookupIn.new,
                     mutate_in_options: MutateIn.new(store_semantics: :upsert))
        @get_options = get_options
        @remove_options = remove_options
        @lookup_in_options = lookup_in_options
        @mutate_in_options = mutate_in_options
        yield self if block_given?
      end
    end

    # Options for {Datastructures::CouchbaseMap#initialize}
    class CouchbaseMap
      attr_accessor :get_options # @return [Get]
      attr_accessor :remove_options # @return [Remove]
      attr_accessor :lookup_in_options # @return [LookupIn]
      attr_accessor :mutate_in_options # @return [MutateIn]

      # Creates an instance of options for {CouchbaseMap#initialize}
      #
      # @param [Get] get_options
      # @param [Remove] remove_options
      # @param [LookupIn] lookup_in_options
      # @param [MutateIn] mutate_in_options
      #
      # @yieldparam [CouchbaseMap]
      def initialize(get_options: Get.new,
                     remove_options: Remove.new,
                     lookup_in_options: LookupIn.new,
                     mutate_in_options: MutateIn.new(store_semantics: :upsert))
        @get_options = get_options
        @remove_options = remove_options
        @lookup_in_options = lookup_in_options
        @mutate_in_options = mutate_in_options
        yield self if block_given?
      end
    end

    # Options for {Datastructures::CouchbaseQueue#initialize}
    class CouchbaseQueue
      attr_accessor :get_options # @return [Get]
      attr_accessor :remove_options # @return [Remove]
      attr_accessor :lookup_in_options # @return [LookupIn]
      attr_accessor :mutate_in_options # @return [MutateIn]

      # Creates an instance of options for {CouchbaseQueue#initialize}
      #
      # @param [Get] get_options
      # @param [Remove] remove_options
      # @param [LookupIn] lookup_in_options
      # @param [MutateIn] mutate_in_options
      #
      # @yieldparam [CouchbaseQueue]
      def initialize(get_options: Get.new,
                     remove_options: Remove.new,
                     lookup_in_options: LookupIn.new,
                     mutate_in_options: MutateIn.new(store_semantics: :upsert))
        @get_options = get_options
        @remove_options = remove_options
        @lookup_in_options = lookup_in_options
        @mutate_in_options = mutate_in_options
        yield self if block_given?
      end
    end

    # Options for {Datastructures::CouchbaseSet#initialize}
    class CouchbaseSet
      attr_accessor :get_options # @return [Get]
      attr_accessor :remove_options # @return [Remove]
      attr_accessor :lookup_in_options # @return [LookupIn]
      attr_accessor :mutate_in_options # @return [MutateIn]

      # Creates an instance of options for {CouchbaseSet#initialize}
      #
      # @param [Get] get_options
      # @param [Remove] remove_options
      # @param [LookupIn] lookup_in_options
      # @param [MutateIn] mutate_in_options
      #
      # @yieldparam [CouchbaseSet]
      def initialize(get_options: Get.new,
                     remove_options: Remove.new,
                     lookup_in_options: LookupIn.new,
                     mutate_in_options: MutateIn.new(store_semantics: :upsert))
        @get_options = get_options
        @remove_options = remove_options
        @lookup_in_options = lookup_in_options
        @mutate_in_options = mutate_in_options
        yield self if block_given?
      end
    end

    # Options for {Couchbase::Cluster.connect}
    #
    # @example Pass authenticator object to Options
    #   Cluster.connect("couchbase://localhost",
    #     Options::Cluster(authenticator: PasswordAuthenticator.new("Administrator", "password")))
    #
    # @example Shorter version, more useful for interactive sessions
    #   Cluster.connect("couchbase://localhost", "Administrator", "password")
    #
    # @example Authentication with TLS client certificate (note +couchbases://+ schema)
    #   Cluster.connect("couchbases://localhost?trust_certificate=/tmp/ca.pem",
    #     Options::Cluster(authenticator: CertificateAuthenticator.new("/tmp/certificate.pem", "/tmp/private.key")))
    #
    # @see https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html
    #
    # @see .Cluster
    #
    class Cluster
      attr_accessor :authenticator # @return [PasswordAuthenticator, CertificateAuthenticator]

      attr_accessor :preferred_server_group # @return [String]

      attr_accessor :enable_metrics # @return [Boolean]
      attr_accessor :metrics_emit_interval # @return [nil, Integer, #in_milliseconds]
      attr_accessor :enable_tracing # @return [Boolean]
      attr_accessor :orphaned_emit_interval # @return [nil, Integer, #in_milliseconds]
      attr_accessor :orphaned_sample_size # @return [nil, Integer]
      attr_accessor :threshold_emit_interval # @return [nil, Integer, #in_milliseconds]
      attr_accessor :threshold_sample_size # @return [nil, Integer]
      attr_accessor :key_value_threshold # @return [nil, Integer, #in_milliseconds]
      attr_accessor :query_threshold # @return [nil, Integer, #in_milliseconds]
      attr_accessor :view_threshold # @return [nil, Integer, #in_milliseconds]
      attr_accessor :search_threshold # @return [nil, Integer, #in_milliseconds]
      attr_accessor :analytics_threshold # @return [nil, Integer, #in_milliseconds]
      attr_accessor :management_threshold # @return [nil, Integer, #in_milliseconds]

      attr_accessor :bootstrap_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :resolve_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :connect_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :key_value_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :view_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :query_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :analytics_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :search_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :management_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :dns_srv_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :tcp_keep_alive_interval # @return [nil, Integer, #in_milliseconds]
      attr_accessor :config_poll_interval # @return [nil, Integer, #in_milliseconds]
      attr_accessor :config_poll_floor # @return [nil, Integer, #in_milliseconds]
      attr_accessor :config_idle_redial_timeout # @return [nil, Integer, #in_milliseconds]
      attr_accessor :idle_http_connection_timeout # @return [nil, Integer, #in_milliseconds]

      # @return [ApplicationTelemetry]
      # @!macro volatile
      attr_accessor :application_telemetry

      attr_accessor :tracer # @return [nil, Tracing::RequestTracer]

      # Creates an instance of options for {Couchbase::Cluster.connect}
      #
      # @param [PasswordAuthenticator, CertificateAuthenticator] authenticator
      # @param [String] preferred_server_group the server group to use for replica APIs e.g. {Collection#get_all_replicas}
      # @param [nil, Integer, #in_milliseconds] key_value_timeout default timeout for Key/Value operations, e.g. {Collection#get}
      # @param [nil, Integer, #in_milliseconds] view_timeout default timeout for View query
      # @param [nil, Integer, #in_milliseconds] query_timeout default timeout for N1QL query
      # @param [nil, Integer, #in_milliseconds] analytics_timeout default timeout for Analytics query
      # @param [nil, Integer, #in_milliseconds] search_timeout default timeout for Search query
      # @param [nil, Integer, #in_milliseconds] management_timeout default timeout for management operations
      #
      # @see .Cluster
      #
      # @yieldparam [Cluster] self
      def initialize(authenticator: nil, # rubocop:disable Metrics/ParameterLists
                     preferred_server_group: nil,
                     enable_metrics: nil,
                     metrics_emit_interval: nil,
                     enable_tracing: nil,
                     orphaned_emit_interval: nil,
                     orphaned_sample_size: nil,
                     threshold_emit_interval: nil,
                     threshold_sample_size: nil,
                     key_value_threshold: nil,
                     query_threshold: nil,
                     view_threshold: nil,
                     search_threshold: nil,
                     analytics_threshold: nil,
                     management_threshold: nil,
                     bootstrap_timeout: nil,
                     resolve_timeout: nil,
                     connect_timeout: nil,
                     key_value_timeout: nil,
                     view_timeout: nil,
                     query_timeout: nil,
                     analytics_timeout: nil,
                     search_timeout: nil,
                     management_timeout: nil,
                     dns_srv_timeout: nil,
                     tcp_keep_alive_interval: nil,
                     config_poll_interval: nil,
                     config_poll_floor: nil,
                     config_idle_redial_timeout: nil,
                     idle_http_connection_timeout: nil,
                     tracer: nil,
                     application_telemetry: ApplicationTelemetry.new)
        @authenticator = authenticator
        @preferred_server_group = preferred_server_group
        @enable_metrics = enable_metrics
        @metrics_emit_interval = metrics_emit_interval
        @enable_tracing = enable_tracing
        @orphaned_emit_interval = orphaned_emit_interval
        @orphaned_sample_size = orphaned_sample_size
        @threshold_emit_interval = threshold_emit_interval
        @threshold_sample_size = threshold_sample_size
        @key_value_threshold = key_value_threshold
        @query_threshold = query_threshold
        @view_threshold = view_threshold
        @search_threshold = search_threshold
        @analytics_threshold = analytics_threshold
        @management_threshold = management_threshold
        @bootstrap_timeout = bootstrap_timeout
        @resolve_timeout = resolve_timeout
        @connect_timeout = connect_timeout
        @key_value_timeout = key_value_timeout
        @view_timeout = view_timeout
        @query_timeout = query_timeout
        @analytics_timeout = analytics_timeout
        @search_timeout = search_timeout
        @management_timeout = management_timeout
        @dns_srv_timeout = dns_srv_timeout
        @tcp_keep_alive_interval = tcp_keep_alive_interval
        @config_poll_interval = config_poll_interval
        @config_poll_floor = config_poll_floor
        @config_idle_redial_timeout = config_idle_redial_timeout
        @idle_http_connection_timeout = idle_http_connection_timeout
        @tracer = tracer
        @application_telemetry = application_telemetry

        yield self if block_given?
      end

      # @param [String] username
      # @param [String] password
      def authenticate(username, password)
        @authenticator = PasswordAuthenticator.new(username, password)
      end

      # @param [String] profile_name The name of the configuration profile to apply (e.g. "wan_development")
      def apply_profile(profile_name)
        ConfigProfiles::KNOWN_PROFILES.apply(profile_name, self)
      end

      # @api private
      def to_backend
        {
          enable_metrics: @enable_metrics,
          preferred_server_group: @preferred_server_group,
          metrics_emit_interval: Utils::Time.extract_duration(@metrics_emit_interval),
          enable_tracing: @enable_tracing,
          orphaned_emit_interval: Utils::Time.extract_duration(@orphaned_emit_interval),
          orphaned_sample_size: @orphaned_sample_size,
          threshold_emit_interval: Utils::Time.extract_duration(@threshold_emit_interval),
          threshold_sample_size: @threshold_sample_size,
          key_value_threshold: Utils::Time.extract_duration(@key_value_threshold),
          query_threshold: Utils::Time.extract_duration(@query_threshold),
          view_threshold: Utils::Time.extract_duration(@view_threshold),
          search_threshold: Utils::Time.extract_duration(@search_threshold),
          analytics_threshold: Utils::Time.extract_duration(@analytics_threshold),
          management_threshold: Utils::Time.extract_duration(@management_threshold),
          bootstrap_timeout: Utils::Time.extract_duration(@bootstrap_timeout),
          resolve_timeout: Utils::Time.extract_duration(@resolve_timeout),
          connect_timeout: Utils::Time.extract_duration(@connect_timeout),
          key_value_timeout: Utils::Time.extract_duration(@key_value_timeout),
          view_timeout: Utils::Time.extract_duration(@view_timeout),
          query_timeout: Utils::Time.extract_duration(@query_timeout),
          analytics_timeout: Utils::Time.extract_duration(@analytics_timeout),
          search_timeout: Utils::Time.extract_duration(@search_timeout),
          management_timeout: Utils::Time.extract_duration(@management_timeout),
          dns_srv_timeout: Utils::Time.extract_duration(@dns_srv_timeout),
          tcp_keep_alive_interval: Utils::Time.extract_duration(@tcp_keep_alive_interval),
          config_poll_interval: Utils::Time.extract_duration(@config_poll_interval),
          config_poll_floor: Utils::Time.extract_duration(@config_poll_floor),
          config_idle_redial_timeout: Utils::Time.extract_duration(@config_idle_redial_timeout),
          idle_http_connection_timeout: Utils::Time.extract_duration(@idle_http_connection_timeout),
          application_telemetry: @application_telemetry.to_backend,
        }
      end

      # Application Telemetry Options for {Couchbase::Cluster.connect}. Part of {Couchbase::Options::Cluster}
      #
      # @see Options::Cluster#application_telemetry
      #
      # @!macro volatile
      class ApplicationTelemetry
        attr_accessor :enable # @return [nil, Boolean]
        attr_accessor :override_endpoint # @return [nil, String]
        attr_accessor :backoff # @return [nil, Integer, #in_milliseconds]
        attr_accessor :ping_interval # @return [nil, Integer, #in_milliseconds]
        attr_accessor :ping_timeout # @return [nil, Integer, #in_milliseconds]

        # Creates an instance of app telemetry options for {Couchbase::Cluster.connect}.
        # Part of {Couchbase::Options::Cluster}.
        #
        # @param [nil, Boolean] enable whether to enable application telemetry capture.
        #   Application telemetry is enabled by default.
        # @param [nil, String] override_endpoint overrides the default endpoint used for application service telemetry
        #   The endpoint must use the WebSocket protocol and the string should start with `ws://`.
        # @param [nil, Integer, #in_milliseconds] backoff specifies the duration to wait between connection attempts
        #   to an application telemetry endpoint
        # @param [nil, Integer, #in_milliseconds] ping_interval specifies how often the SDK should ping the application
        #   service telemetry collector
        # @param [nil, Integer, #in_milliseconds] ping_timeout specifies how long the SDK should wait for a ping
        #   response (pong) from the application service collector, before closing the connection and attempting to reconnect
        def initialize(enable: nil,
                       override_endpoint: nil,
                       backoff: nil,
                       ping_interval: nil,
                       ping_timeout: nil)
          @enable = enable
          @override_endpoint = override_endpoint
          @backoff = backoff
          @ping_interval = ping_interval
          @ping_timeout = ping_timeout

          yield self if block_given?
        end

        # @api private
        def to_backend
          {
            enable: @enable,
            override_endpoint: @override_endpoint,
            backoff: Utils::Time.extract_duration(@backoff),
            ping_interval: Utils::Time.extract_duration(@ping_interval),
            ping_timeout: Utils::Time.extract_duration(@ping_timeout),
          }
        end
      end
    end

    # Options for {Couchbase::Cluster#diagnostics}
    class Diagnostics
      attr_accessor :report_id # @return [String]

      # Creates an instance of options for {Couchbase::Cluster#diagnostics}
      #
      # @param [String] report_id Holds custom report ID.
      #
      # @yieldparam [Diagnostics] self
      def initialize(report_id: nil)
        @report_id = report_id
        yield self if block_given?
      end

      # @api private
      DEFAULT = Diagnostics.new.freeze
    end

    # Options for {Couchbase::Bucket#ping}
    class Ping
      attr_accessor :report_id # @return [String]
      attr_accessor :service_types # @return [Array<Symbol>]
      attr_accessor :timeout # @return [Integer, #in_milliseconds]

      # Creates an instance of options for {Couchbase::Bucket#ping}
      #
      # @param [String] report_id Holds custom report id.
      # @@param [Array<Symbol>] service_types The service types to limit this diagnostics request
      # @param [Integer, #in_milliseconds] timeout
      #
      # @yieldparam [Ping] self
      def initialize(report_id: nil,
                     service_types: [:kv, :query, :analytics, :search, :views, :management],
                     timeout: nil)
        @report_id = report_id
        @service_types = service_types
        @timeout = timeout
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          service_types: @service_types,
          report_id: @report_id,
        }
      end

      # @api private
      DEFAULT = Ping.new.freeze
    end

    # Options for {Couchbase::Cluster#analytics_query}
    class Analytics < Base
      attr_accessor :client_context_id # @return [String]
      attr_accessor :scan_consistency # @return [Symbol]
      attr_accessor :readonly # @return [Boolean]
      attr_accessor :priority # @return [Boolean]
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]
      attr_accessor :scope_qualifier # @return [String]

      # Creates new instance of options for {Couchbase::Cluster#analytics_query}
      #
      # @param [String] client_context_id provides a custom client context ID for this query
      # @param [Symbol] scan_consistency specifies level of consistency for the query
      #  +:not_bounded+::
      #    The index will return whatever state it has to the analytics query engine at the time of query.
      #
      #    This is the default (for single-statement requests). No timestamp vector is used in the index scan.
      #    This is also the fastest mode, because we avoid the cost of obtaining the vector, and we also avoid
      #    any wait for the index
      #  +:request_plus+::
      #    The index will wait until all mutations have been processed at the time of request before being processed
      #    in the analytics query engine.
      #
      #    This implements strong consistency per request. Before processing the request, a current vector is obtained.
      #    The vector is used as a lower bound for the statements in the request.
      # @param [Boolean] readonly allows explicitly marking a query as being readonly and not mutating any documents on
      #   the server side.
      # @param [Boolean] priority allows to give certain requests higher priority than others
      # @param [JsonTranscoder] transcoder to decode rows
      # @param [Array<#to_json>, nil] positional_parameters parameters to be used as substitution for numbered macros
      #   like +$1+, +$2+ in query string
      # @param [Hash<String => #to_json>, nil] named_parameters parameters to be used as substitution for named macros
      #   like +$name+ in query string
      # @param [String, nil] scope_qualifier Associate scope qualifier (also known as +query_context+) with the query.
      #   The qualifier must be in form +{bucket_name}.{scope_name}+ or +default:{bucket_name}.{scope_name}+.
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @note Either +positional_parameters+ or +named_parameters+ may be specified.
      #
      # @yieldparam [Analytics] self
      def initialize(client_context_id: nil,
                     scan_consistency: nil,
                     readonly: false,
                     priority: nil,
                     transcoder: JsonTranscoder.new,
                     positional_parameters: nil,
                     named_parameters: nil,
                     scope_qualifier: nil,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        raise ArgumentError, "Cannot pass positional and named parameters at the same time" if positional_parameters && named_parameters

        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @client_context_id = client_context_id
        @scan_consistency = scan_consistency
        @readonly = readonly
        @priority = priority
        @transcoder = transcoder
        @positional_parameters = positional_parameters
        @named_parameters = named_parameters
        @scope_qualifier = scope_qualifier
        @raw_parameters = {}
        yield self if block_given?
      end

      # Sets positional parameters for the query
      #
      # @param [Array] positional the list of parameters that have to be substituted in the statement
      def positional_parameters(positional)
        @positional_parameters = positional
        @named_parameters = nil
      end

      # Sets named parameters for the query
      #
      # @param [Hash] named the key/value map of the parameters to substitute in the statement
      def named_parameters(named)
        @named_parameters = named
        @positional_parameters = nil
      end

      # Allows providing custom JSON key/value pairs for advanced usage
      #
      # @param [String] key the parameter name (key of the JSON property)
      # @param [Object] value the parameter value (value of the JSON property)
      def raw(key, value)
        @raw_parameters[key] = JSON.generate(value)
      end

      # @api private
      def to_backend(scope_name: nil, bucket_name: nil)
        {
          timeout: Utils::Time.extract_duration(@timeout),
          client_context_id: @client_context_id,
          scan_consistency: @scan_consistency,
          readonly: @readonly,
          priority: @priority,
          positional_parameters: export_positional_parameters,
          named_parameters: export_named_parameters,
          raw_parameters: @raw_parameters,
          scope_qualifier: @scope_qualifier,
          scope_name: scope_name,
          bucket_name: bucket_name,
        }
      end

      # @api private
      DEFAULT = Analytics.new.freeze

      private

      # @api private
      # @return [Array<String>, nil]
      def export_positional_parameters
        @positional_parameters&.map { |p| JSON.dump(p) }
      end

      # @api private
      # @return [Hash<String => String>, nil]
      def export_named_parameters
        @named_parameters&.each_with_object({}) { |(n, v), o| o[n.to_s] = JSON.dump(v) }
      end

      # @api private
      # @return [Hash<String => #to_json>]
      attr_reader :raw_parameters
    end

    # Options for {Couchbase::Cluster#query}
    class Query < Base
      attr_accessor :adhoc # @return [Boolean]
      attr_accessor :client_context_id # @return [String]
      attr_accessor :max_parallelism # @return [Integer]
      attr_accessor :readonly # @return [Boolean]
      attr_accessor :scan_wait # @return [Integer, #in_milliseconds]
      attr_accessor :scan_cap # @return [Integer]
      attr_accessor :pipeline_batch # @return [Integer]
      attr_accessor :pipeline_cap # @return [Integer]
      attr_accessor :metrics # @return [Boolean]
      attr_accessor :profile # @return [Symbol]
      attr_accessor :flex_index # @return [Boolean]
      attr_accessor :preserve_expiry # @return [Boolean]
      attr_accessor :use_replica # @return [Boolean, nil]
      attr_accessor :scope_qualifier # @return [String]
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]

      # Creates new instance of options for {Couchbase::Cluster#query}
      #
      # @param [Boolean] adhoc allows turning this request into a prepared statement query
      # @param [String, nil] client_context_id provides a custom client context ID for this query
      # @param [Integer, nil] max_parallelism allows overriding the default maximum parallelism for the query execution
      #   on the server side.
      # @param [Boolean, nil] readonly allows explicitly marking a query as being readonly and not mutating any
      #   documents on the server side.
      # @param [Integer, #in_milliseconds, nil] scan_wait The maximum duration (in milliseconds) the query engine
      #   is willing to wait before failing. Allows customizing how long (in milliseconds) the query engine is willing
      #   to wait until the index catches up to whatever scan consistency is asked for in this query. Note that if
      #   +:not_bounded+ consistency level is used, this method doesn't do anything at all. If no value is provided to
      #   this method, the server default is used.
      # @param [Integer, nil] scan_cap customize the maximum buffered channel size between the indexer and the query
      #   service
      # @param [Integer, nil] pipeline_cap customize the number of items execution operators can batch for fetch
      #   from the Key Value layer on the server.
      # @param [Integer, nil] pipeline_batch customize the maximum number of items each execution operator can buffer
      #   between various operators on the server.
      # @param [Boolean, nil] metrics enables per-request metrics in the trailing section of the query
      # @param [Symbol] profile customize server profile level for this query
      #   +:off+::
      #     No profiling information is added to the query response
      #   +:phases+::
      #     The query response includes a profile section with stats and details about various phases of the query plan
      #     and execution. Three phase times will be included in the +system:active_requests+ and
      #     +system:completed_requests+ monitoring keyspaces.
      #   +:timings+::
      #     Besides the phase times, the profile section of the query response document will include a full query plan
      #     with timing and information about the number of processed documents at each phase. This information will be
      #     included in the system:active_requests and system:completed_requests keyspaces.
      # @param [Symbol, nil] scan_consistency Sets the mutation tokens this query should be consistent with. Overrides
      #   +mutation_state+.
      #   +:not_bounded+::
      #     The indexer will return whatever state it has to the query engine at the time of query. This is the default
      #     (for single-statement requests).
      #   +:request_plus+::
      #     The indexer will wait until all mutations have been processed at the time of request before returning to
      #     the query engine.
      # @param [Boolean, nil] flex_index Tells the query engine to use a flex index (utilizing the search service)
      # @param [Boolean, nil] preserve_expiry Tells the query engine to preserve expiration values set on any documents
      #   modified by this query.
      # @param [Boolean, nil] use_replica Specifies that the query engine should use replica nodes for KV fetches if
      #   the active node is down. If not provided, the server default will be used
      # @param [String, nil] scope_qualifier Associate scope qualifier (also known as +query_context+) with the query.
      #   The qualifier must be in form +{bucket_name}.{scope_name}+ or +default:{bucket_name}.{scope_name}+.
      # @param [JsonTranscoder] transcoder to decode rows
      # @param [Array<#to_json>, nil] positional_parameters parameters to be used as substitution for numbered macros
      #   like +$1+, +$2+ in query string
      # @param [Hash<String => #to_json>, nil] named_parameters parameters to be used as substitution for named macros
      #   like +$name+ in query string.
      #
      # @param [MutationState, nil] mutation_state Sets the mutation tokens this query should be consistent with.
      #   Overrides +scan_consistency+.
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @note Either +positional_parameters+ or +named_parameters+ may be specified.
      #
      # @yieldparam [Query] self
      def initialize(adhoc: true,
                     client_context_id: nil,
                     max_parallelism: nil,
                     readonly: false,
                     scan_wait: nil,
                     scan_cap: nil,
                     pipeline_cap: nil,
                     pipeline_batch: nil,
                     metrics: nil,
                     profile: :off,
                     flex_index: nil,
                     preserve_expiry: nil,
                     use_replica: nil,
                     scope_qualifier: nil,
                     scan_consistency: :not_bounded,
                     mutation_state: nil,
                     transcoder: JsonTranscoder.new,
                     positional_parameters: nil,
                     named_parameters: nil,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        raise ArgumentError, "Cannot pass positional and named parameters at the same time" if positional_parameters && named_parameters

        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @adhoc = adhoc
        @client_context_id = client_context_id
        @max_parallelism = max_parallelism
        @readonly = readonly
        @scan_wait = scan_wait
        @scan_cap = scan_cap
        @pipeline_cap = pipeline_cap
        @pipeline_batch = pipeline_batch
        @metrics = metrics
        @profile = profile
        @flex_index = flex_index
        @preserve_expiry = preserve_expiry
        @use_replica = use_replica
        @scope_qualifier = scope_qualifier
        @scan_consistency = scan_consistency
        @mutation_state = mutation_state
        @transcoder = transcoder
        @positional_parameters = positional_parameters
        @named_parameters = named_parameters
        @raw_parameters = {}
        yield self if block_given?
      end

      # Allows providing custom JSON key/value pairs for advanced usage
      #
      # @param [String] key the parameter name (key of the JSON property)
      # @param [Object] value the parameter value (value of the JSON property)
      def raw(key, value)
        @raw_parameters[key] = JSON.generate(value)
      end

      # Customizes the consistency guarantees for this query
      #
      # @note overrides consistency level set by {#consistent_with}
      #
      # [+:not_bounded+] The indexer will return whatever state it has to the query engine at the time of query. This is the default (for
      #   single-statement requests).
      #
      # [+:request_plus+] The indexer will wait until all mutations have been processed at the time of request before returning to the query
      #   engine.
      #
      # @param [:not_bounded, :request_plus] level the index scan consistency to be used for this query
      def scan_consistency=(level)
        @mutation_state = nil if @mutation_state
        @scan_consistency = level
      end

      # Sets the mutation tokens this query should be consistent with
      #
      # @note overrides consistency level set by {#scan_consistency=}
      #
      # @param [MutationState] mutation_state the mutation state containing the mutation tokens
      def consistent_with(mutation_state)
        @scan_consistency = nil if @scan_consistency
        @mutation_state = mutation_state
      end

      # Sets positional parameters for the query
      #
      # @param [Array] positional the list of parameters that have to be substituted in the statement
      def positional_parameters(positional)
        @positional_parameters = positional
        @named_parameters = nil
      end

      # @api private
      # @return [Array<String>, nil]
      def export_positional_parameters
        @positional_parameters&.map { |p| JSON.dump(p) }
      end

      # Sets named parameters for the query
      #
      # @param [Hash] named the key/value map of the parameters to substitute in the statement
      def named_parameters(named)
        @named_parameters = named
        @positional_parameters = nil
      end

      # @api private
      # @return [Hash<String => String>, nil]
      def export_named_parameters
        @named_parameters&.each_with_object({}) { |(n, v), o| o[n.to_s] = JSON.dump(v) }
      end

      # @api private
      # @return [MutationState]
      attr_reader :mutation_state

      # @api private
      # @return [Hash<String => #to_json>]
      attr_reader :raw_parameters

      # @api private
      def to_backend(scope_name: nil, bucket_name: nil)
        if scope_name && bucket_name
          default_query_context = format("default:`%<bucket>s`.`%<scope>s`", bucket: bucket_name, scope: scope_name)
        end
        {
          timeout: Utils::Time.extract_duration(@timeout),
          adhoc: @adhoc,
          client_context_id: @client_context_id,
          max_parallelism: @max_parallelism,
          readonly: @readonly,
          flex_index: @flex_index,
          preserve_expiry: @preserve_expiry,
          use_replica: @use_replica,
          scan_wait: Utils::Time.extract_duration(@scan_wait),
          scan_cap: @scan_cap,
          pipeline_batch: @pipeline_batch,
          pipeline_cap: @pipeline_cap,
          metrics: @metrics,
          profile: @profile,
          positional_parameters: export_positional_parameters,
          named_parameters: export_named_parameters,
          raw_parameters: @raw_parameters,
          scan_consistency: @scan_consistency,
          mutation_state: @mutation_state&.to_a,
          query_context: @scope_qualifier || default_query_context,
        }
      end

      # @api private
      DEFAULT = Query.new.freeze
    end

    # Options for {Couchbase::Cluster#search_query} and {Couchbase::Cluster#search}
    class Search < Base
      attr_accessor :limit # @return [Integer]
      attr_accessor :skip # @return [Integer]
      attr_accessor :explain # @return [Boolean]
      attr_accessor :highlight_style # @return [Symbol]
      attr_accessor :highlight_fields # @return [Array<String>]
      attr_accessor :fields # @return [Array<String>]
      attr_accessor :disable_scoring # @return [Boolean]
      attr_accessor :include_locations # @return [Boolean]
      attr_accessor :collections # @return [Array<String>, nil]
      attr_accessor :sort # @return [Array<String, Cluster::SearchSort>]
      attr_accessor :facets # @return [Hash<String => Cluster::SearchFacet>]
      attr_accessor :transcoder # @return [JsonTranscoder, #decode(String)]

      # @param [Integer] limit limits the number of matches returned from the complete result set.
      # @param [Integer] skip indicates how many matches are skipped on the result set before starting to return the
      #   matches
      # @param [Boolean] explain triggers inclusion of additional search result score explanations.
      # @param [:html, :ansi, nil] highlight_style the style of highlighting in the result excerpts (if not specified,
      #   the server default will be used)
      # @param [Array<String>] highlight_fields list of the fields to highlight
      # @param [Array<String>] fields list of field values which should be retrieved for result documents, provided they
      #   were stored while indexing
      # @param [MutationState] mutation_state the mutation tokens this query should be consistent with
      # @param [Boolean] disable_scoring If set to true, the server will not perform any scoring on the hits
      # @param [Boolean] include_locations UNCOMMITTED: If set to true, will include the vector of search_location in rows
      # @param [Array<String>, nil] collections list of collections by which to filter the results
      # @param [Array<String, Cluster::SearchSort>] sort Ordering rules to apply to the results. The list might contain
      #   either strings or special objects, that derive from {Cluster::SearchSort}. In case of String, the value
      #   represents the name of the field with optional +-+ in front of the name, which will turn on descending mode
      #   for this field. One field is special is +"_score"+ which will sort results by their score. When nothing
      #   specified, the Server will order results by their score descending, which is equivalent of +"-_score"+.
      # @param [Hash<String => Cluster::SearchFacet>] facets facets allow to aggregate information collected on a
      #   particular result set
      # @param [JsonTranscoder, #decode(String)] transcoder to use for the results
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [Search] self
      def initialize(limit: nil,
                     skip: nil,
                     explain: false,
                     highlight_style: nil,
                     highlight_fields: nil,
                     fields: nil,
                     mutation_state: nil,
                     disable_scoring: false,
                     include_locations: false,
                     collections: nil,
                     sort: nil,
                     facets: nil,
                     transcoder: JsonTranscoder.new,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
        @limit = limit
        @skip = skip
        @explain = explain
        @highlight_style = highlight_style
        @highlight_fields = highlight_fields
        @fields = fields
        @disable_scoring = disable_scoring
        @include_locations = include_locations
        @collections = collections
        @sort = sort
        @facets = facets
        @transcoder = transcoder
        @scan_consistency = :not_bounded
        @mutation_state = mutation_state
        yield self if block_given?
      end

      # Sets the mutation tokens this query should be consistent with
      #
      # @note overrides consistency level set by {#scan_consistency=}
      #
      # @param [MutationState] mutation_state the mutation state containing the mutation tokens
      #
      # @return [void]
      def consistent_with(mutation_state)
        @scan_consistency = nil if @scan_consistency
        @mutation_state = mutation_state
      end

      # Customizes the consistency guarantees for this query
      #
      # @note overrides consistency level set by {#consistent_with}
      #
      # @param [:not_bounded] level the scan consistency to be used for this query
      #   +:not_bounded+:: The engine will return whatever state it has at the time of query
      #
      # @return [void]
      def scan_consistency=(level)
        @mutation_state = nil if @mutation_state
        @scan_consistency = level
      end

      # @api private
      # @return [MutationState]
      attr_reader :mutation_state

      # @api private
      # @return [Symbol]
      attr_reader :scan_consistency

      # @api private
      def to_backend(show_request: nil)
        {
          timeout: Utils::Time.extract_duration(@timeout),
          limit: @limit,
          skip: @skip,
          explain: @explain,
          disable_scoring: @disable_scoring,
          include_locations: @include_locations,
          collections: @collections,
          highlight_style: @highlight_style,
          highlight_fields: @highlight_fields,
          fields: @fields,
          sort: @sort&.map { |v| JSON.generate(v) },
          facets: @facets&.map { |(k, v)| [k, JSON.generate(v)] },
          scan_consistency: @scan_consistency,
          mutation_state: @mutation_state&.to_a,
          show_request: show_request,
        }
      end

      # @api private
      DEFAULT = Search.new.freeze
    end

    class VectorSearch
      # @return [:and, :or, nil]
      attr_accessor :vector_query_combination

      # @param [:and, :or, nil] vector_query_combination
      #
      # @yieldparam [Options::VectorSearch] self
      def initialize(vector_query_combination: nil)
        @vector_query_combination = vector_query_combination

        yield self if block_given?
      end

      def to_backend
        {
          vector_query_combination: @vector_query_combination,
        }
      end

      DEFAULT = VectorSearch.new.freeze
    end

    # Options for {Couchbase::Cluster#view_query}
    class View < Base
      attr_accessor :scan_consistency # @return [Symbol]
      attr_accessor :namespace # @return [Symbol]
      attr_accessor :skip # @return [Integer]
      attr_accessor :limit # @return [Integer]
      attr_accessor :start_key # @return [#to_json, nil]
      attr_accessor :end_key # @return [#to_json, nil]
      attr_accessor :start_key_doc_id # @return [String, nil]
      attr_accessor :end_key_doc_id # @return [String, nil]
      attr_accessor :inclusive_end # @return [Boolean, nil]
      attr_accessor :group # @return [Boolean, nil]
      attr_accessor :group_level # @return [Integer, nil]
      attr_accessor :key # @return [#to_json, nil]
      attr_accessor :keys # @return [Array<#to_json>, nil]
      attr_accessor :order # @return [Symbol, nil]
      attr_accessor :reduce # @return [Boolean, nil]
      attr_accessor :on_error # @return [Symbol, nil]
      attr_accessor :debug # @return [Boolean, nil]

      # @param [:not_bounded, :request_plus, :update_after] scan_consistency Specifies the level of consistency for the query
      # @param [:production, :development] namespace
      # @param [Integer, nil] skip Specifies the number of results to skip from the start of the result set
      # @param [Integer, nil] limit Specifies the maximum number of results to return
      # @param [#to_json, nil] start_key Specifies the key, to which the engine has to skip before result generation
      # @param [#to_json, nil] end_key Specifies the key, at which the result generation has to be stopped
      # @param [String, nil] start_key_doc_id Specifies the document id in case {#start_key} gives multiple results within the index
      # @param [String, nil] end_key_doc_id Specifies the document id in case {#end_key} gives multiple results within the index
      # @param [Boolean, nil] inclusive_end Specifies whether the {#end_key}/#{#end_key_doc_id} values should be inclusive
      # @param [Boolean, nil] group Specifies whether to enable grouping of the results
      # @param [Integer, nil] group_level Specifies the depth within the key to group the results
      # @param [#to_json, nil] key Specifies the key to fetch from the index
      # @param [Array<#to_json>, nil] keys Specifies set of the keys to fetch from the index
      # @param [:ascending, :descending, nil] order Specifies the order of the results that should be returned
      # @param [Boolean, nil] reduce Specifies whether to enable the reduction function associated with this particular
      #   view index
      # @param [:stop, :continue, nil] on_error Specifies the behaviour of the view engine should an error occur during
      #   the gathering of view index results which would result in only partial results being available
      # @param [Boolean, nil] debug allows to return debug information as part of the view response
      #
      # @param [Integer, #in_milliseconds, nil] timeout
      # @param [Proc, nil] retry_strategy the custom retry strategy, if set
      # @param [Hash, nil] client_context the client context data, if set
      # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
      #
      # @yieldparam [View] self
      def initialize(scan_consistency: :not_bounded,
                     namespace: :production,
                     skip: nil,
                     limit: nil,
                     start_key: nil,
                     end_key: nil,
                     start_key_doc_id: nil,
                     end_key_doc_id: nil,
                     inclusive_end: nil,
                     group: nil,
                     group_level: nil,
                     key: nil,
                     keys: nil,
                     order: nil,
                     reduce: nil,
                     on_error: nil,
                     debug: false,
                     timeout: nil,
                     retry_strategy: nil,
                     client_context: nil,
                     parent_span: nil)
        super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)

        @scan_consistency = scan_consistency
        @namespace = namespace
        @skip = skip
        @limit = limit
        @start_key = start_key
        @end_key = end_key
        @start_key_doc_id = start_key_doc_id
        @end_key_doc_id = end_key_doc_id
        @inclusive_end = inclusive_end
        @group = group
        @group_level = group_level
        @key = key
        @keys = keys
        @order = order
        @reduce = reduce
        @on_error = on_error
        @debug = debug
        yield self if block_given?
      end

      # Allows providing custom JSON key/value pairs for advanced usage
      #
      # @param [String] key the parameter name (key of the JSON property)
      # @param [Object] value the parameter value (value of the JSON property)
      def raw(key, value)
        @raw_parameters[key] = JSON.generate(value)
      end

      # @api private
      def to_backend
        {
          timeout: Utils::Time.extract_duration(@timeout),
          scan_consistency: @scan_consistency,
          skip: @skip,
          limit: @limit,
          start_key: (JSON.generate(@start_key) unless @start_key.nil?),
          end_key: (JSON.generate(@end_key) unless @end_key.nil?),
          start_key_doc_id: @start_key_doc_id,
          end_key_doc_id: @end_key_doc_id,
          inclusive_end: @inclusive_end,
          group: @group,
          group_level: @group_level,
          key: (JSON.generate(@key) unless @key.nil?),
          keys: @keys&.map { |key| JSON.generate(key) },
          order: @order,
          reduce: @reduce,
          on_error: @on_error,
          debug: @debug,
        }
      end

      # @api private
      DEFAULT = View.new.freeze
    end

    # @api private
    # TODO: deprecate in 3.1
    CommonOptions = ::Couchbase::Options::Base

    # rubocop:disable Naming/MethodName -- constructor shortcuts
    module_function

    # Construct {Get} options for {Collection#get}
    #
    # @example Get partial document using projections
    #   res = collection.get("customer123", Options::Get(projections: ["name", "addresses.billing"]))
    #   res.content
    #
    #   # {"addresses"=>
    #   #    {"billing"=>
    #   #      {"country"=>"United Kingdom",
    #   #       "line1"=>"123 Any Street",
    #   #       "line2"=>"Anytown"}},
    #   #   "name"=>"Douglas Reynholm"}
    #
    # @return [Get]
    def Get(**args)
      Get.new(**args)
    end

    # Construct {GetMulti} options for {Collection#get_multi}
    #
    # @example Fetch "foo" and "bar" in a batch
    #   res = collection.get(["foo", "bar"], Options::GetMulti(timeout: 3_000))
    #   res[0].content #=> content of "foo"
    #   res[1].content #=> content of "bar"
    #
    # @return [GetMulti]
    def GetMulti(**args)
      GetMulti.new(**args)
    end

    # Construct {GetAndLock} options for {Collection#get_and_lock}
    #
    # @example Retrieve document and lock for 10 seconds
    #   collection.get_and_lock("customer123", 10, Options::GetAndLock(timeout: 3_000))
    #
    # @return [GetAndLock]
    def GetAndLock(**args)
      GetAndLock.new(**args)
    end

    # Construct {GetAndTouch} options for {Collection#get_and_touch}
    #
    # @example Retrieve document and prolong its expiration for 10 seconds
    #   collection.get_and_touch("customer123", 10, Options::GetAndTouch(timeout: 3_000))
    #
    # @return [GetAndTouch]
    def GetAndTouch(**args)
      GetAndTouch.new(**args)
    end

    # Construct {GetAllReplicas} options for {Collection#get_any_replica}
    #
    # @return [GetAllReplicas]
    def GetAllReplicas(**args)
      GetAllReplicas.new(**args)
    end

    # Construct {GetAnyReplica} options for {Collection#get_all_replicas}
    #
    # @return [GetAnyReplica]
    def GetAnyReplica(**args)
      GetAnyReplica.new(**args)
    end

    # Construct {Exists} options for {Collection#exists}
    #
    # @example Check if the document exists without fetching its contents
    #   res = collection.exists("customer123", Options::Exists(timeout: 3_000))
    #   res.exists? #=> true
    #
    # @return [Exists]
    def Exists(**args)
      Exists.new(**args)
    end

    # Construct {Touch} options for {Collection#touch}
    #
    # @example Reset expiration timer for document to 30 seconds (and use custom operation timeout)
    #   res = collection.touch("customer123", 30, Options::Touch(timeout: 3_000))
    #
    # @return [Touch]
    def Touch(**args)
      Touch.new(**args)
    end

    # Construct {Unlock} options for {Collection#touch}
    #
    # @example Lock (pessimistically) and unlock document
    #   res = collection.get_and_lock("customer123", 10, Options::Unlock(timeout: 3_000))
    #   collection.unlock("customer123", res.cas)
    #
    # @return [Unlock]
    def Unlock(**args)
      Unlock.new(**args)
    end

    # Construct {Remove} options for {Collection#remove}
    #
    # @example Remove the document in collection, but apply optimistic lock
    #   res = collection.upsert("mydoc", {"foo" => 42})
    #   res.cas #=> 7751414725654
    #
    #   begin
    #     res = collection.remove("customer123", Options::Remove(cas: 3735928559))
    #   rescue Error::CasMismatch
    #     puts "Failed to remove the document, it might be changed by other application"
    #   end
    #
    # @return [Remove]
    def Remove(**args)
      Remove.new(**args)
    end

    # Construct {RemoveMulti} options for {Collection#remove_multi}
    #
    # @example Remove two documents in collection. For "mydoc" apply optimistic lock
    #   res = collection.upsert("mydoc", {"foo" => 42})
    #   res.cas #=> 7751414725654
    #
    #   res = collection.remove_multi(["foo", ["mydoc", res.cas]], Options::RemoveMulti(timeout: 3_000))
    #   if res[1].error.is_a?(Error::CasMismatch)
    #     puts "Failed to remove the document, it might be changed by other application"
    #   end
    #
    # @return [RemoveMulti]
    def RemoveMulti(**args)
      RemoveMulti.new(**args)
    end

    # Construct {Insert} options for {Collection#insert}
    #
    # @example Insert new document in collection
    #   res = collection.insert("mydoc", {"foo" => 42}, Options::Insert(expiry: 20))
    #   res.cas #=> 242287264414742
    #
    # @return [Insert]
    def Insert(**args)
      Insert.new(**args)
    end

    # Construct {Upsert} options for {Collection#upsert}
    #
    # @example Upsert new document in collection
    #   res = collection.upsert("mydoc", {"foo" => 42}, Options::Upsert(expiry: 20))
    #   res.cas #=> 242287264414742
    #
    # @return [Upsert]
    def Upsert(**args)
      Upsert.new(**args)
    end

    # Construct {UpsertMulti} options for {Collection#upsert_multi}
    #
    # @example Upsert two documents with IDs "foo" and "bar" into a collection with expiration 20 seconds.
    #   res = collection.upsert_multi([
    #     "foo", {"foo" => 42},
    #     "bar", {"bar" => "some value"}
    #   ], Options::UpsertMulti(expiry: 20))
    #   res[0].cas #=> 7751414725654
    #   res[1].cas #=> 7751418925851
    #
    # @return [UpsertMulti]
    def UpsertMulti(**args)
      UpsertMulti.new(**args)
    end

    # Construct {Replace} options for {Collection#replace}
    #
    # @example Replace new document in collection with optimistic locking
    #   res = collection.get("mydoc")
    #   res = collection.replace("mydoc", {"foo" => 42}, Options::Replace(cas: res.cas))
    #   res.cas #=> 242287264414742
    #
    # @return [Replace]
    def Replace(**args)
      Replace.new(**args)
    end

    # Construct {MutateIn} options for {Collection#mutate_in}
    #
    # @example Append number into subarray of the document
    #   mutation_specs = [
    #     MutateInSpec::array_append("purchases.complete", [42])
    #   ]
    #   collection.mutate_in("customer123", mutation_specs, Options::MutateIn(expiry: 10))
    #
    # @return [MutateIn]
    def MutateIn(**args)
      MutateIn.new(**args)
    end

    # Construct {LookupIn} options for {Collection#lookup_in}
    #
    # @example Get list of IDs of completed purchases
    #   lookup_specs = [
    #     LookupInSpec::get("purchases.complete")
    #   ]
    #   collection.lookup_in("customer123", lookup_specs, Options::LookupIn(timeout: 3_000))
    #
    # @return [LookupIn]
    def LookupIn(**args)
      LookupIn.new(**args)
    end

    # Construct {Append} options for {BinaryCollection#append}
    #
    # @example Append "bar" to the content of the existing document
    #   collection.upsert("mydoc", "foo")
    #   collection.binary.append("mydoc", "bar", Options::Append(timeout: 3_000))
    #   collection.get("mydoc", Options::Get(transcoder: nil)).content #=> "foobar"
    #
    # @return [Append]
    def Append(**args)
      Append.new(**args)
    end

    # Construct {Prepend} options for {BinaryCollection#prepend}
    #
    # @example Prepend "bar" to the content of the existing document
    #   collection.upsert("mydoc", "foo")
    #   collection.binary.prepend("mydoc", "bar", Options::Prepend(timeout: 3_000))
    #   collection.get("mydoc", Options::Get(transcoder: nil)).content #=> "barfoo"
    #
    # @return [Prepend]
    def Prepend(**args)
      Prepend.new(**args)
    end

    # Construct {Diagnostics} options for {Cluster#diagnostics}
    #
    # @return [Diagnostics]
    def Diagnostics(**args)
      Diagnostics.new(**args)
    end

    # Construct {Ping} options for {Bucket#ping}
    #
    # @return [Ping]
    def Ping(**args)
      Ping.new(**args)
    end

    # Construct {Cluster} options for {Cluster.connect}
    #
    # It forwards all its arguments to {Cluster#initialize}
    #
    # @return [Cluster]
    def Cluster(**args)
      Cluster.new(**args)
    end

    # Construct {Increment} options for {BinaryCollection#increment}
    #
    # @example Increment value by 10, and initialize to 0 if it does not exist
    #   res = collection.binary.increment("raw_counter", Options::Increment(delta: 10, initial: 0))
    #   res.content #=> 0
    #   res = collection.binary.increment("raw_counter", Options::Increment(delta: 10, initial: 0))
    #   res.content #=> 10
    #
    # @return [Increment]
    def Increment(**args)
      Increment.new(**args)
    end

    # Construct {Decrement} options for {BinaryCollection#decrement}
    #
    # @example Decrement value by 2, and initialize to 100 if it does not exist
    #   res = collection.binary.decrement("raw_counter", Options::Decrement(delta: 2, initial: 100))
    #   res.value #=> 100
    #   res = collection.binary.decrement("raw_counter", Options::Decrement(delta: 2, initial: 100))
    #   res.value #=> 98
    #
    # @return [Decrement]
    def Decrement(**args)
      Decrement.new(**args)
    end

    # Construct {Analytics} options for {Cluster#analytics_query}
    #
    # @example Select name of the given user
    #   cluster.analytics_query("SELECT u.name AS uname FROM GleambookUsers u WHERE u.id = $user_id ",
    #                           Options::Analytics(named_parameters: {user_id: 2}))
    #
    # @return [Analytics]
    def Analytics(**args)
      Analytics.new(**args)
    end

    # Construct {Query} options for {Cluster#query}
    #
    # @example Select first ten hotels from travel sample dataset
    #   cluster.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10",
    #                 Options::Query(named_parameters: {type: "hotel"}, metrics: true))
    #
    # @return [Query]
    def Query(**args)
      Query.new(**args)
    end

    # Construct {Search} options for {Cluster#search_query}
    #
    # @example Return first 10 results of "hop beer" query and request highlighting
    #   cluster.search_query("beer_index", Cluster::SearchQuery.match_phrase("hop beer"),
    #                        Options::Search(
    #                          limit: 10,
    #                          fields: %w[name],
    #                          highlight_style: :html,
    #                          highlight_fields: %w[name description]
    #                        ))
    #
    # @return [Search]
    def Search(**args)
      Search.new(**args)
    end

    # Construct {View} options for {Bucket#view_query}
    #
    # @example Make sure the view engine catch up with all mutations and return keys starting from +["random_brewery:"]+
    #   bucket.view_query("beer", "brewery_beers",
    #                     Options::View(
    #                       start_key: ["random_brewery:"],
    #                       scan_consistency: :request_plus
    #                     ))
    #
    # @return [View]
    def View(**args)
      View.new(**args)
    end

    # Construct {Scan} options for {Collection#scan}
    #
    # @return [Scan]
    def Scan(**args)
      Scan.new(**args)
    end

    # Construct {LookupInAnyReplica} options for {Collection#lookup_in_any_replica}
    #
    # @return [LookupInAnyReplica]
    def LookupInAnyReplica(**args)
      LookupInAnyReplica.new(**args)
    end

    # Construct {LookupInAllReplics} options for {Collection#lookup_in_all_replicas}
    #
    # @return [LookupInAllReplicas]
    def LookupInAllReplicas(**args)
      LookupInAllReplicas.new(**args)
    end

    # rubocop:enable Naming/MethodName
  end
end
