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

require "couchbase"

include Couchbase # rubocop:disable Style/MixinUsage for brevity

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

bucket_name = "tiny_social"
dataverse_name = "TinySocial"

# Prepare dataset
begin
  cluster.buckets.drop_bucket(bucket_name)
rescue Error::BucketNotFound
  # ignore
end

settings = Management::BucketSettings.new
settings.name = bucket_name
settings.bucket_type = :couchbase
settings.ram_quota_mb = 100
cluster.buckets.create_bucket(settings)
loop do
  sleep 1
  break if cluster.buckets.get_all_buckets.find { |b| b.name == bucket_name }.healthy?
end

collection = cluster.bucket(bucket_name).default_collection

# Documents for GleambookUsers dataset
[
  {
    "id" => 1,
    "alias" => "Margarita",
    "name" => "MargaritaStoddard",
    "nickname" => "Mags",
    "userSince" => "2012-08-20T10:10:00",
    "friendIds" => [2, 3, 6, 10],
    "employment" => [
      {
        "organizationName" => "Codetechno",
        "start-date" => "2006-08-06",
      },
      {
        "organizationName" => "geomedia",
        "start-date" => "2010-06-17",
        "end-date" => "2010-01-26",
      },
    ],
    "gender" => "F",
  },
  {
    "id" => 2,
    "alias" => "Isbel",
    "name" => "IsbelDull",
    "nickname" => "Izzy",
    "userSince" => "2011-01-22T10:10:00",
    "friendIds" => [1, 4],
    "employment" => [
      {
        "organizationName" => "Hexviafind",
        "startDate" => "2010-04-27",
      },
    ],
  },
  {
    "id" => 3,
    "alias" => "Emory",
    "name" => "EmoryUnk",
    "userSince" => "2012-07-10T10:10:00",
    "friendIds" => [1, 5, 8, 9],
    "employment" => [
      {
        "organizationName" => "geomedia",
        "startDate" => "2010-06-17",
        "endDate" => "2010-01-26",
      },
    ],
  },
].each do |document|
  collection.upsert("user:#{document['id']}",
                    document.merge({"type" => "user"}))
end

# Documents for GleambookMessages dataset
[
  {
    "messageId" => 2,
    "authorId" => 1,
    "inResponseTo" => 4,
    "senderLocation" => [41.66, 80.87],
    "message" => " dislike x-phone its touch-screen is horrible",
  },
  {
    "messageId" => 3,
    "authorId" => 2,
    "inResponseTo" => 4,
    "senderLocation" => [48.09, 81.01],
    "message" => " like product-y the plan is amazing",
  },
  {
    "messageId" => 4,
    "authorId" => 1,
    "inResponseTo" => 2,
    "senderLocation" => [37.73, 97.04],
    "message" => " can't stand acast the network is horrible:(",
  },
  {
    "messageId" => 6,
    "authorId" => 2,
    "inResponseTo" => 1,
    "senderLocation" => [31.5, 75.56],
    "message" => " like product-z its platform is mind-blowing",
  },
  {
    "messageId" => 8,
    "authorId" => 1,
    "inResponseTo" => 11,
    "senderLocation" => [40.33, 80.87],
    "message" => " like ccast the 3G is awesome:)",
  },
  {
    "messageId" => 10,
    "authorId" => 1,
    "inResponseTo" => 12,
    "senderLocation" => [42.5, 70.01],
    "message" => " can't stand product-w the touch-screen is terrible",
  },
  {
    "messageId" => 11,
    "authorId" => 1,
    "inResponseTo" => 1,
    "senderLocation" => [38.97, 77.49],
    "message" => " can't stand acast its plan is terrible",
  },
].each do |document|
  collection.upsert("message:#{document['messageId']}",
                    document.merge({"type" => "message"}))
end

if cluster.analytics_indexes.get_all_datasets.any? { |ds| ds.dataverse_name == dataverse_name }
  # there are datasets on our dataverse, drop everything and re-create
  options = Management::AnalyticsIndexManager::DisconnectLinkOptions.new
  options.dataverse_name = dataverse_name
  cluster.analytics_indexes.disconnect_link(options)
  cluster.analytics_indexes.drop_dataverse(dataverse_name)
end

cluster.analytics_indexes.create_dataverse(dataverse_name)

options = Management::AnalyticsIndexManager::CreateDatasetOptions.new
options.dataverse_name = dataverse_name

options.condition = '`type` = "user"'
cluster.analytics_indexes.create_dataset("GleambookUsers", bucket_name, options)

options.condition = '`type` = "message"'
cluster.analytics_indexes.create_dataset("GleambookMessages", bucket_name, options)

options = Management::AnalyticsIndexManager::ConnectLinkOptions.new
options.dataverse_name = dataverse_name
cluster.analytics_indexes.connect_link(options)

sleep(1)

puts "---- inner join"
res = cluster.analytics_query(
  "SELECT * FROM #{dataverse_name}.GleambookUsers u, #{dataverse_name}.GleambookMessages m WHERE m.authorId = u.id"
)
res.rows.each do |row|
  puts "#{row['u']['name']}: #{row['m']['message'].inspect}"
end

# The query language supports SQL's notion of left outer join.
puts "---- left outer join"
res = cluster.analytics_query("
  USE #{dataverse_name};
  SELECT u.name AS uname, m.message AS message
  FROM GleambookUsers u
  LEFT OUTER JOIN GleambookMessages m ON m.authorId = u.id
")
res.rows.each do |row|
  puts "#{row['uname']}: #{row['message'].inspect}"
end

# Named parameters
puts "---- named parameters"
options = Cluster::AnalyticsOptions.new
options.named_parameters({user_id: 2})
res = cluster.analytics_query("
USE #{dataverse_name};

SELECT u.name AS uname
FROM GleambookUsers u
WHERE u.id = $user_id

UNION ALL

SELECT VALUE m.message
FROM GleambookMessages m
WHERE authorId = $user_id
", options)
res.rows.each_with_index do |row, index|
  puts "row #{index}: #{row.inspect}"
end

# Positional parameters
puts "---- positional parameters"
options = Cluster::AnalyticsOptions.new
options.positional_parameters([2])
res = cluster.analytics_query("
USE #{dataverse_name};
SELECT VALUE user
FROM GleambookUsers AS user
WHERE len(user.friendIds) > $1
", options)
res.rows.each do |row|
  puts "#{row['name']}: #{row['friendIds'].size}"
end

# More query examples at https://docs.couchbase.com/server/current/analytics/3_query.html
