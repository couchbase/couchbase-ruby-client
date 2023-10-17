#!/usr/bin/env ruby
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

# do not patch version for releases
if ENV.fetch("IS_RELEASE", nil) && ENV["IS_RELEASE"].strip == "true"
  puts "Building release version, do not append snapshot number."
  exit
end

build_num = ARGV[0] || ENV.fetch("BUILD_NUMBER", nil) || 0
suffix = ARGV[1] || "snapshot"

ver_file = File.join(__dir__, "../../lib/couchbase/version.rb")

old_content = File.read(ver_file)
new_content = old_content.gsub(/sdk => "([^"]+?)(\.#{suffix}(\.\d+)?)?"/, %(sdk => "\\1.#{suffix}.#{build_num}"))
File.write(ver_file, new_content)
