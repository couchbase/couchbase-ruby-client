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

#include "test_helper_native.hxx"

#include <diagnostics.hxx>

using namespace std::literals::chrono_literals;

TEST_CASE("native: serializing diagnostics report", "[native]")
{
    couchbase::diag::diagnostics_result res{
        "0xdeadbeef",
        "ruby/1.0.0",
        {
          {
            {
              couchbase::service_type::search,
              {
                {
                  couchbase::service_type::search,
                  "0x1415F11",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8094",
                  "127.0.0.1:54669",
                  couchbase::diag::endpoint_state::connecting,
                  std::nullopt,
                  "RECONNECTING, backoff for 4096ms from Fri Sep  1 00:03:44 PDT 2017",
                },
              },
            },
            {
              couchbase::service_type::kv,
              {
                {
                  couchbase::service_type::kv,
                  "0x1415F12",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:11210",
                  "127.0.0.1:54670",
                  couchbase::diag::endpoint_state::connected,
                  "bucketname",
                },
              },
            },
            {
              couchbase::service_type::query,
              {
                {
                  couchbase::service_type::query,
                  "0x1415F13",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8093",
                  "127.0.0.1:54671",
                  couchbase::diag::endpoint_state::connected,
                },
                {
                  couchbase::service_type::query,
                  "0x1415F14",
                  1182000us,
                  "centos7-lx2.home.ingenthron.org:8095",
                  "127.0.0.1:54682",
                  couchbase::diag::endpoint_state::disconnected,
                },
              },
            },
            {
              couchbase::service_type::analytics,
              {
                {
                  couchbase::service_type::analytics,
                  "0x1415F15",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8095",
                  "127.0.0.1:54675",
                  couchbase::diag::endpoint_state::connected,
                },
              },
            },
            {
              couchbase::service_type::views,
              {
                {
                  couchbase::service_type::views,
                  "0x1415F16",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8092",
                  "127.0.0.1:54672",
                  couchbase::diag::endpoint_state::connected,
                },
              },
            },
          },
        },
    };

    auto expected = tao::json::from_string(R"(
{
  "version": 2,
  "id": "0xdeadbeef",
  "sdk": "ruby/1.0.0",
  "services": {
    "kv": [
      {
        "id": "0x1415F12",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:11210",
        "local": "127.0.0.1:54670",
        "state": "connected",
        "namespace": "bucketname"
      }
    ],
    "search": [
      {
        "id": "0x1415F11",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8094",
        "local": "127.0.0.1:54669",
        "state": "connecting",
        "details": "RECONNECTING, backoff for 4096ms from Fri Sep  1 00:03:44 PDT 2017"
      }
    ],
    "query": [
      {
        "id": "0x1415F13",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8093",
        "local": "127.0.0.1:54671",
        "state": "connected"
      },
      {
        "id": "0x1415F14",
        "last_activity_us": 1182000,
        "remote": "centos7-lx2.home.ingenthron.org:8095",
        "local": "127.0.0.1:54682",
        "state": "disconnected"
      }
    ],
    "analytics": [
      {
        "id": "0x1415F15",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8095",
        "local": "127.0.0.1:54675",
        "state": "connected"
      }
    ],
    "views": [
      {
        "id": "0x1415F16",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8092",
        "local": "127.0.0.1:54672",
        "state": "connected"
      }
    ]
  }
}
)");
    auto report = tao::json::value(res);
    REQUIRE(report == expected);
}

TEST_CASE("native: serializing ping report", "[native]")
{
    auto ctx = test_context::load_from_environment();
    native_init_logger();

    couchbase::diag::ping_result res{
        "0xdeadbeef",
        "ruby/1.0.0",
        {
          {
            {
              couchbase::service_type::search,
              {
                {
                  couchbase::service_type::search,
                  "0x1415F11",
                  877909us,
                  "centos7-lx1.home.ingenthron.org:8094",
                  "127.0.0.1:54669",
                  couchbase::diag::ping_state::ok,
                },
              },
            },
            {
              couchbase::service_type::kv,
              {
                {
                  couchbase::service_type::kv,
                  "0x1415F12",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:11210",
                  "127.0.0.1:54670",
                  couchbase::diag::ping_state::ok,
                  "bucketname",
                },
              },
            },
            {
              couchbase::service_type::query,
              {
                {
                  couchbase::service_type::query,
                  "0x1415F14",
                  2213us,
                  "centos7-lx2.home.ingenthron.org:8095",
                  "127.0.0.1:54682",
                  couchbase::diag::ping_state::timeout,
                },
              },
            },
            {
              couchbase::service_type::analytics,
              {
                {
                  couchbase::service_type::analytics,
                  "0x1415F15",
                  2213us,
                  "centos7-lx1.home.ingenthron.org:8095",
                  "127.0.0.1:54675",
                  couchbase::diag::ping_state::error,
                  std::nullopt,
                  "endpoint returned HTTP code 500!",
                },
              },
            },
            {
              couchbase::service_type::views,
              {
                {
                  couchbase::service_type::views,
                  "0x1415F16",
                  45585us,
                  "centos7-lx1.home.ingenthron.org:8092",
                  "127.0.0.1:54672",
                  couchbase::diag::ping_state::ok,
                },
              },
            },
          },
        },
    };

    auto expected = tao::json::from_string(R"(
{
  "version": 2,
  "id": "0xdeadbeef",
  "config_rev": 53,
  "sdk": "ruby/1.0.0",
  "services": {
    "search": [
      {
        "id": "0x1415F11",
        "latency_us": 877909,
        "remote": "centos7-lx1.home.ingenthron.org:8094",
        "local": "127.0.0.1:54669",
        "state": "ok"
      }
    ],
    "kv": [
      {
        "id": "0x1415F12",
        "latency_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:11210",
        "local": "127.0.0.1:54670",
        "state": "ok",
        "namespace": "bucketname"
      }
    ],
    "query": [
      {
        "id": "0x1415F14",
        "latency_us": 2213,
        "remote": "centos7-lx2.home.ingenthron.org:8095",
        "local": "127.0.0.1:54682",
        "state": "timeout"
      }
    ],
    "analytics": [
      {
        "id": "0x1415F15",
        "latency_us": 2213,
        "remote": "centos7-lx1.home.ingenthron.org:8095",
        "local": "127.0.0.1:54675",
        "state": "error",
        "error": "endpoint returned HTTP code 500!"
      }
    ],
    "views": [
      {
        "id": "0x1415F16",
        "latency_us": 45585,
        "remote": "centos7-lx1.home.ingenthron.org:8092",
        "local": "127.0.0.1:54672",
        "state": "ok"
      }
    ]
  }
}
)");
    auto report = tao::json::value(res);
    REQUIRE(report == expected);
}

TEST_CASE("native: fetch diagnostics after N1QL query", "[native]")
{
    auto ctx = test_context::load_from_environment();
    native_init_logger();

    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    auth.username = ctx.username;
    auth.password = ctx.password;

    asio::io_context io;

    couchbase::cluster cluster(io);
    auto io_thread = std::thread([&io]() { io.run(); });

    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open(couchbase::origin(auth, connstr), [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        auto rc = f.get();
        INFO(rc.message());
        REQUIRE_FALSE(rc);
    }
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open_bucket(ctx.bucket, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        auto rc = f.get();
        INFO(rc.message());
        REQUIRE_FALSE(rc);
    }
    {
        couchbase::operations::query_request req{ "SELECT 'hello, couchbase' AS greetings" };
        auto barrier = std::make_shared<std::promise<couchbase::operations::query_response>>();
        auto f = barrier->get_future();
        cluster.execute_http(req, [barrier](couchbase::operations::query_response&& resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        INFO(resp.ec.message());
        REQUIRE_FALSE(resp.ec);
        INFO("rows.size() =" << resp.payload.rows.size());
        REQUIRE(resp.payload.rows.size() == 1);
        INFO("row=" << resp.payload.rows[0]);
        REQUIRE(resp.payload.rows[0] == R"({"greetings":"hello, couchbase"})");
    }
    {
        auto barrier = std::make_shared<std::promise<couchbase::diag::diagnostics_result>>();
        auto f = barrier->get_future();
        cluster.diagnostics("my_report_id", [barrier](couchbase::diag::diagnostics_result&& resp) mutable { barrier->set_value(resp); });
        auto res = f.get();
        REQUIRE(res.id == "my_report_id");
        REQUIRE(res.sdk.find("ruby/") == 0);
        REQUIRE(res.services[couchbase::service_type::kv].size() > 1);
        REQUIRE(res.services[couchbase::service_type::query].size() == 1);
        REQUIRE(res.services[couchbase::service_type::query][0].state == couchbase::diag::endpoint_state::connected);
    }
    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        cluster.close([barrier]() { barrier->set_value(); });
        f.get();
    }

    io_thread.join();
}

TEST_CASE("native: ping", "[native]")
{
    auto ctx = test_context::load_from_environment();
    native_init_logger();

    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    auth.username = ctx.username;
    auth.password = ctx.password;

    asio::io_context io;

    couchbase::cluster cluster(io);
    auto io_thread = std::thread([&io]() { io.run(); });

    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open(couchbase::origin(auth, connstr), [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        auto rc = f.get();
        INFO(rc.message());
        REQUIRE_FALSE(rc);
    }
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open_bucket(ctx.bucket, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        auto rc = f.get();
        INFO(rc.message());
        REQUIRE_FALSE(rc);
    }
    {
        auto barrier = std::make_shared<std::promise<couchbase::diag::ping_result>>();
        auto f = barrier->get_future();
        cluster.ping("my_report_id", {}, [barrier](couchbase::diag::ping_result&& resp) mutable { barrier->set_value(resp); });
        auto res = f.get();
        REQUIRE(res.id == "my_report_id");
        REQUIRE(res.sdk.find("ruby/") == 0);

        auto report = tao::json::value(res);
        spdlog::critical("XXX {}", tao::json::to_string(report));
    }
    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        cluster.close([barrier]() { barrier->set_value(); });
        f.get();
    }

    io_thread.join();
}
