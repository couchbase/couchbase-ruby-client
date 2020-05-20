/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <asio.hpp>

#include <cluster.hxx>

#include <errors.hxx>
#include <operations/get.hxx>
#include <operations/upsert.hxx>
#include <operations/lookup_in.hxx>
#include <operations/query.hxx>

static int num_ops = 4;

bool
done()
{
    --num_ops;
    spdlog::info("------ num_ops={}", num_ops);
    return num_ops == 0;
}

int
main()
{
    spdlog::set_level(spdlog::level::trace);
    spdlog::set_pattern("[%Y-%m-%d %T.%e] [%P,%t] [%^%l%$] %v");
    asio::io_context ctx;

    couchbase::cluster cluster{ ctx };

    couchbase::origin options;
    options.hostname = "192.168.42.103";
    options.hostname = "127.0.0.1";
    options.username = "Administrator";
    options.password = "password";

    //  std::string bucket_name = "default";
    //  std::string collection_name = "_default._default";

    cluster.open(options, [&](std::error_code ec1) {
        spdlog::info("cluster has been opened: {}", ec1.message());
        couchbase::operations::query_request req{ "select random()" };
        cluster.execute(req, [&](couchbase::operations::query_response resp) {
            spdlog::info("QUERY {}: ec={}, body={}",
                         couchbase::uuid::to_string(resp.client_context_id),
                         resp.ec.message(),
                         resp.payload.meta_data.metrics.result_size);
            for (auto &r : resp.payload.rows) {
                spdlog::info("ROW: {}", r);
            }
            if (resp.payload.meta_data.profile) {
                spdlog::info("PROFILE: {}", *resp.payload.meta_data.profile);
            }
            cluster.close();
        });
#if 0
      cluster.open_bucket(bucket_name, [&](std::error_code ec2) {
            if (!ec2) {
                couchbase::operations::mutate_in_request req{
                    couchbase::operations::document_id{ bucket_name, collection_name, "foo" },
                };
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "test", R"({"name":"sergey"})");
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false,  false, false,"num", 24);
                cluster.execute(req, [&](couchbase::operations::mutate_in_response resp) {
                    spdlog::info("MUTATE_IN {}: ec={}, cas={}, fields={}", resp.id, resp.ec.message(), resp.cas, resp.fields.size());
                    size_t idx = 0;
                    for (const auto& field : resp.fields) {
                        spdlog::info("  {}. {}: {} {}", idx++, field.path, field.status, field.value);
                    }
                    // if (done()) {
                    cluster.close();
                    //}
                });
                couchbase::operations::lookup_in_request req{
                    couchbase::operations::document_id{ bucket_name, collection_name, "foo" },
                };
                req.access_deleted = true;
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, true, "$document");
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::exists, false, "bar");
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "foo");
                cluster.execute(req, [&](couchbase::operations::lookup_in_response resp) {
                    spdlog::info("LOOKUP_IN {}: ec={}, cas={}, fields={}", resp.id, resp.ec.message(), resp.cas, resp.fields.size());
                    size_t idx = 0;
                    for (const auto &field : resp.fields) {
                        spdlog::info("  {}. {}{}: {}", idx++, field.path, field.exists ? "(hit) " : "(miss)", field.value);
                    }
                    //if (done()) {
                        cluster.close();
                    //}
                });
                cluster.execute(
                  couchbase::operations::get_request{
                    couchbase::operations::document_id{ bucket_name, collection_name, "foo" },
                  },
                  [&](couchbase::operations::get_response resp) {
                      spdlog::info("GET {}: ec={}, cas={}\n{}", resp.id, resp.ec.message(), resp.cas, resp.value);
                      if (done()) {
                          cluster.close();
                      }
                  });
                cluster.execute(
                  couchbase::operations::get_request{
                    couchbase::operations::document_id{ bucket_name, collection_name, "bar" },
                  },
                  [&](couchbase::operations::get_response resp) {
                      spdlog::info("GET {}: ec={}, cas={}\n{}", resp.id, resp.ec.message(), resp.cas, resp.value);
                      if (done()) {
                          cluster.close();
                      }
                  });
                cluster.execute(
                  couchbase::operations::upsert_request{ couchbase::operations::document_id{ bucket_name, collection_name, "foo" },
                                                         "{\"prop\":42}" },
                  [&](couchbase::operations::upsert_response resp) {
                      spdlog::info("UPSERT {}: ec={}, cas={}", resp.id, resp.ec.message(), resp.cas);
                      if (done()) {
                          cluster.close();
                      }
                  });
            } else {
                spdlog::info("unable to open the bucket: {}", ec2.message());
            }
        });
#endif
    });
    ctx.run();
}