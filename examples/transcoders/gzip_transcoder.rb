# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2013-2017 Couchbase, Inc.
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

require 'zlib'
require 'stringio'

# This class wraps any other transcoder and performs compression
# using zlib
class GzipTranscoder
  FMT_GZIP = 0x04

  def initialize(base = nil)
    @base = base || Couchbase::Transcoder::Plain
  end

  def dump(obj, flags, options = {})
    obj, flags = @base.dump(obj, flags, options)
    io = StringIO.new
    gz = Zlib::GzipWriter.new(io)
    gz.write(obj)
    gz.close
    [io.string, flags | FMT_GZIP]
  end

  def load(blob, flags, options = {})
    # decompress value only if gzip flag set
    if (flags & FMT_GZIP) == FMT_GZIP
      io = StringIO.new(blob)
      gz = Zlib::GzipReader.new(io)
      blob = gz.read
      gz.close
    end
    @base.load(blob, flags, options)
  end
end
