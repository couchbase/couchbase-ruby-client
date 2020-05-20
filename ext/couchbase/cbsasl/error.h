/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <spdlog/spdlog.h>

namespace couchbase::sasl
{

/**
 * The error values used in CBSASL
 */
enum class error { OK, CONTINUE, FAIL, BAD_PARAM, NO_MEM, NO_MECH, NO_USER, PASSWORD_ERROR, NO_RBAC_PROFILE, AUTH_PROVIDER_DIED };

} // namespace couchbase::sasl

template<>
struct fmt::formatter<couchbase::sasl::error> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::sasl::error err, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (err) {
            case couchbase::sasl::error::OK:
                name = "ok";
                break;
            case couchbase::sasl::error::CONTINUE:
                name = "continue";
                break;
            case couchbase::sasl::error::FAIL:
                name = "fail";
                break;
            case couchbase::sasl::error::BAD_PARAM:
                name = "bad_param";
                break;
            case couchbase::sasl::error::NO_MEM:
                name = "no_mem";
                break;
            case couchbase::sasl::error::NO_MECH:
                name = "no_mech";
                break;
            case couchbase::sasl::error::NO_USER:
                name = "no_user";
                break;
            case couchbase::sasl::error::PASSWORD_ERROR:
                name = "password_error";
                break;
            case couchbase::sasl::error::NO_RBAC_PROFILE:
                name = "no_rbac_profile";
                break;
            case couchbase::sasl::error::AUTH_PROVIDER_DIED:
                name = "auth_provider_died";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
