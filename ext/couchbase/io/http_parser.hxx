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

#pragma once

#include <http_parser.h>
#include <io/http_message.hxx>
#include <algorithm>

namespace couchbase::io
{
struct http_parser {

    enum class status { ok, failure };

    http_parser_settings settings_{};
    ::http_parser parser_{};
    http_response response;
    std::string header_field;
    bool complete{ false };

    http_parser()
    {
        ::http_parser_init(&parser_, HTTP_RESPONSE);
        parser_.data = this;
        settings_.on_message_begin = static_on_message_begin;
        settings_.on_url = static_on_url;
        settings_.on_status = static_on_status;
        settings_.on_header_field = static_on_header_field;
        settings_.on_header_value = static_on_header_value;
        settings_.on_headers_complete = static_on_headers_complete;
        settings_.on_body = static_on_body;
        settings_.on_message_complete = static_on_message_complete;
        settings_.on_chunk_header = static_on_chunk_header;
        settings_.on_chunk_complete = static_on_chunk_complete;
    }

    void reset()
    {
        complete = false;
        response = {};
        header_field = {};
        ::http_parser_init(&parser_, HTTP_RESPONSE);
    }

    status feed(const char* data, size_t data_len)
    {
        std::size_t bytes_parsed = ::http_parser_execute(&parser_, &settings_, data, data_len);
        if (bytes_parsed != data_len) {
            return status::failure;
        }
        return status::ok;
    }

    int on_headers_complete()
    {
        return 0;
    }

    int on_chunk_complete()
    {
        return 0;
    }

    int on_message_begin()
    {
        return 0;
    }

    int on_chunk_header()
    {
        return 0;
    }

    int on_message_complete()
    {
        complete = true;
        return 0;
    }

    int on_url(const char* /* at */, std::size_t /* length */)
    {
        return 0;
    }

    int on_status(const char* at, std::size_t length)
    {
        response.status_message.assign(at, length);
        response.status_code = parser_.status_code;
        return 0;
    }

    int on_header_field(const char* at, std::size_t length)
    {
        header_field.assign(at, length);
        std::transform(header_field.begin(), header_field.end(), header_field.begin(), [](unsigned char c) { return std::tolower(c); });
        return 0;
    }

    int on_header_value(const char* at, std::size_t length)
    {
        response.headers[header_field] = std::string(at, length);
        return 0;
    }

    int on_body(const char* at, std::size_t length)
    {
        response.body.append(at, length);
        return 0;
    }

    static inline int static_on_url(::http_parser* parser, const char* at, std::size_t length)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_url(at, length);
    }

    static inline int static_on_status(::http_parser* parser, const char* at, std::size_t length)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_status(at, length);
    }

    static inline int static_on_header_field(::http_parser* parser, const char* at, std::size_t length)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_header_field(at, length);
    }

    static inline int static_on_header_value(::http_parser* parser, const char* at, std::size_t length)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_header_value(at, length);
    }

    static inline int static_on_body(::http_parser* parser, const char* at, std::size_t length)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_body(at, length);
    }

    static inline int static_on_message_begin(::http_parser* parser)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_message_begin();
    }

    static inline int static_on_headers_complete(::http_parser* parser)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_headers_complete();
    }

    static inline int static_on_message_complete(::http_parser* parser)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_message_complete();
    }

    static inline int static_on_chunk_header(::http_parser* parser)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_chunk_header();
    }

    static inline int static_on_chunk_complete(::http_parser* parser)
    {
        return static_cast<couchbase::io::http_parser*>(parser->data)->on_chunk_complete();
    }
};
} // namespace couchbase::io
