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

# typed: strict
module Couchbase
  class Cluster
    sig {params().returns(UserManager)}
    def users
      UserManager.new(@io)
    end

    sig {params().returns(BucketManager)}
    def buckets
      BucketManager.new(@io)
    end

    sig {params().returns(QueryIndexManager)}
    def query_indexes
      QueryIndexManager.new(@io)
    end

    sig {params().returns(AnalyticsIndexManager)}
    def analytics_indexes
      AnalyticsIndexManager.new(@io)
    end

    sig {params().returns(SearchIndexManager)}
    def search_indexes
      SearchIndexManager.new(@io)
    end
  end

  class UserManager
    sig {params(io: Backend::IoCore).void}
    def initialize(io)
    end
  end

  class BucketManager
    sig {params(io: Backend::IoCore).void}
    def initialize(io)
    end
  end

  class QueryIndexManager
    sig {params(io: Backend::IoCore).void}
    def initialize(io)
    end
  end

  class AnalyticsIndexManager
    sig {params(io: Backend::IoCore).void}
    def initialize(io)
    end
  end

  class SearchIndexManager
    sig {params(io: Backend::IoCore).void}
    def initialize(io)
    end
  end

  # private
  module Backend
    class IoCore
    end
  end
end
