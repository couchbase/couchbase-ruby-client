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

require 'thread'

module Couchbase
  class Latch
    attr_reader :state, :target

    # Takes initial pair of possible states and set latch in the first.
    #
    # @example Read an attribute.
    #   Latch.new(false, true)
    #   Latch.new(:closed, :opened)
    #
    # @param [ Object ] from Initial state
    #
    # @param [ Object ] to Target state
    def initialize(from, to)
      @state = from
      @target = to
      @lock = Mutex.new
      @condition = ConditionVariable.new
    end


    # Turn latch to target state.
    #
    # @example
    #   l = Latch.new(:opened, :closed)
    #   l.state   #=> :opened
    #   l.toggle  #=> :closed
    #
    # @return [ Object ] Target state
    def toggle
      @lock.synchronize do
        @state = @target
        @condition.broadcast
      end
      @state
    end

    # Perform blocking wait operation until state will be toggled.
    #
    # @example
    #   l = Latch.new(:opened, :closed)
    #   l.wait    #=> :closed
    #
    # @return [ Object ] Target state
    def wait
      @lock.synchronize do
        @condition.wait(@lock) while @state != @target
      end
      @state
    end
  end
end
