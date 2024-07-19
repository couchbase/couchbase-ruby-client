# frozen_string_literal: true

#  Copyright 2023. Couchbase, Inc.
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

module Couchbase
  # A scan term used to specify the bounds of a range scan
  class ScanTerm
    attr_accessor :term # @return [ScanTerm]
    attr_accessor :exclusive # @return [Boolean]

    # Creates an instance of a ScanTerm
    #
    # @param [String] term the key pattern of this term
    # @param [Boolean] exclusive specifies if this term is excluded while scanning, the bounds are included by default
    def initialize(term, exclusive: false)
      @term = term
      @exclusive = exclusive
    end

    # @api private
    def to_backend
      {
        term: @term,
        exclusive: @exclusive,
      }
    end
  end

  # A range scan performs a scan on a range of keys
  class RangeScan
    attr_accessor :from # @return [ScanTerm, nil]
    attr_accessor :to # @return [ScanTerm, nil]

    # Creates an instance of a RangeScan scan type
    #
    # @param [ScanTerm, String, nil] from the lower bound of the range, if set
    # @param [ScanTerm, String, nil] to the upper bound of the range, if set
    def initialize(from: nil, to: nil)
      @from =
        if from.nil? || from.instance_of?(ScanTerm)
          from
        else
          ScanTerm(from)
        end
      @to =
        if to.nil? || to.instance_of?(ScanTerm)
          to
        else
          ScanTerm(to)
        end
    end

    # @api private
    def to_backend
      {
        scan_type: :range,
        from: @from&.to_backend,
        to: @to&.to_backend,
      }
    end
  end

  # A prefix scan performs a scan that includes all documents whose keys start with the given prefix
  class PrefixScan
    attr_accessor :prefix # @return [String]

    # Creates an instance of a PrefixScan scan type
    #
    # @param [String, nil] prefix the prefix all document keys should start with
    def initialize(prefix)
      @prefix = prefix
    end

    # @api private
    def to_backend
      {
        scan_type: :prefix,
        prefix: @prefix,
      }
    end
  end

  # A sampling scan performs a scan that randomly selects documents up to a configured limit
  class SamplingScan
    attr_accessor :limit # @return [Integer]
    attr_accessor :seed # @return [Integer, nil]

    # Creates an instance of a SamplingScan scan type
    #
    # @param [Integer] limit the maximum number of documents the sampling scan can return
    # @param [Integer, nil] seed the seed used for the random number generator that selects the documents. If not set,
    #   a seed is generated at random
    def initialize(limit, seed = nil)
      @limit = limit
      @seed = seed
    end

    # @api private
    def to_backend
      {
        scan_type: :sampling,
        limit: @limit,
        seed: @seed,
      }
    end
  end
end
