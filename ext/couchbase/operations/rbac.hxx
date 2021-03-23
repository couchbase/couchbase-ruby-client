/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <string>
#include <optional>
#include <set>

namespace couchbase::operations::rbac
{

struct role {
    std::string name;
    std::optional<std::string> bucket{};
    std::optional<std::string> scope{};
    std::optional<std::string> collection{};
};

struct role_and_description : public role {
    std::string display_name{};
    std::string description{};
};

struct origin {
    std::string type;
    std::optional<std::string> name{};
};

struct role_and_origins : public role {
    std::vector<origin> origins{};
};

struct user {
    std::string username;
    std::optional<std::string> display_name{};
    // names of the groups
    std::set<std::string> groups{};
    // only roles assigned directly to the user (not inherited from groups)
    std::vector<role> roles{};
    // write only, it is not populated on reads
    std::optional<std::string> password{};
};

enum class auth_domain { unknown, local, external };

struct user_and_metadata : public user {
    auth_domain domain{ auth_domain::unknown };
    // all roles associated with the user, including information about whether each role is innate or inherited from a group
    std::vector<role_and_origins> effective_roles{};
    // timestamp of last password change
    std::optional<std::string> password_changed{};
    std::set<std::string> external_groups{};
};

struct group {
    std::string name;
    std::optional<std::string> description{};
    std::vector<role> roles{};
    std::optional<std::string> ldap_group_reference{};
};

} // namespace couchbase::operations::rbac

namespace tao::json
{
template<>
struct traits<couchbase::operations::rbac::user_and_metadata> {
    template<template<typename...> class Traits>
    static couchbase::operations::rbac::user_and_metadata as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::rbac::user_and_metadata result;
        std::string domain = v.at("domain").get_string();
        if (domain == "local") {
            result.domain = couchbase::operations::rbac::auth_domain::local;
        } else if (domain == "external") {
            result.domain = couchbase::operations::rbac::auth_domain::external;
        } else {
            spdlog::error(R"("unexpected domain for user with metadata: "{}")", domain);
        }
        result.username = v.at("id").get_string();
        {
            const auto* display_name = v.find("name");
            if (display_name != nullptr && !display_name->get_string().empty()) {
                result.display_name = display_name->get_string();
            }
        }
        result.password_changed = v.template optional<std::string>("password_change_date");
        {
            const auto* external_groups = v.find("external_groups");
            if (external_groups != nullptr) {
                for (const auto& group : external_groups->get_array()) {
                    result.external_groups.insert(group.get_string());
                }
            }
        }
        {
            const auto* groups = v.find("groups");
            if (groups != nullptr) {
                for (const auto& group : groups->get_array()) {
                    result.groups.insert(group.get_string());
                }
            }
        }
        {
            const auto* roles = v.find("roles");
            if (roles != nullptr) {
                for (const auto& entry : roles->get_array()) {
                    couchbase::operations::rbac::role_and_origins role{};
                    role.name = entry.at("role").get_string();
                    {
                        const auto* bucket = entry.find("bucket_name");
                        if (bucket != nullptr && !bucket->get_string().empty()) {
                            role.bucket = bucket->get_string();
                        }
                    }
                    {
                        const auto* scope = entry.find("scope_name");
                        if (scope != nullptr && !scope->get_string().empty()) {
                            role.scope = scope->get_string();
                        }
                    }
                    {
                        const auto* collection = entry.find("collection_name");
                        if (collection != nullptr && !collection->get_string().empty()) {
                            role.collection = collection->get_string();
                        }
                    }
                    {
                        const auto* origins = entry.find("origins");
                        if (origins != nullptr) {
                            bool has_user_origin = false;
                            for (const auto& ent : origins->get_array()) {
                                couchbase::operations::rbac::origin origin{};
                                origin.type = ent.at("type").get_string();
                                if (origin.type == "user") {
                                    has_user_origin = true;
                                }
                                const auto* name = ent.find("name");
                                if (name != nullptr) {
                                    origin.name = name->get_string();
                                }
                                role.origins.push_back(origin);
                            }
                            if (has_user_origin) {
                                result.roles.push_back(role);
                            }
                        } else {
                            result.roles.push_back(role);
                        }
                    }
                    result.effective_roles.push_back(role);
                }
            }
        }
        return result;
    }
};

template<>
struct traits<couchbase::operations::rbac::role_and_description> {
    template<template<typename...> class Traits>
    static couchbase::operations::rbac::role_and_description as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::rbac::role_and_description result;
        result.name = v.at("role").get_string();
        result.display_name = v.at("name").get_string();
        result.description = v.at("desc").get_string();
        {
            const auto* bucket = v.find("bucket_name");
            if (bucket != nullptr && !bucket->get_string().empty()) {
                result.bucket = bucket->get_string();
            }
        }
        {
            const auto* scope = v.find("scope_name");
            if (scope != nullptr && !scope->get_string().empty()) {
                result.scope = scope->get_string();
            }
        }
        {
            const auto* collection = v.find("collection_name");
            if (collection != nullptr && !collection->get_string().empty()) {
                result.collection = collection->get_string();
            }
        }
        return result;
    }
};

template<>
struct traits<couchbase::operations::rbac::group> {
    template<template<typename...> class Traits>
    static couchbase::operations::rbac::group as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::rbac::group result;
        result.name = v.at("id").get_string();
        {
            const auto* desc = v.find("description");
            if (desc != nullptr && !desc->get_string().empty()) {
                result.description = desc->get_string();
            }
        }
        {
            const auto* ldap_ref = v.find("ldap_group_ref");
            if (ldap_ref != nullptr && !ldap_ref->get_string().empty()) {
                result.ldap_group_reference = ldap_ref->get_string();
            }
        }
        {
            const auto* roles = v.find("roles");
            if (roles != nullptr) {
                for (const auto& entry : roles->get_array()) {
                    couchbase::operations::rbac::role role{};
                    role.name = entry.at("role").get_string();
                    {
                        const auto* bucket = entry.find("bucket_name");
                        if (bucket != nullptr && !bucket->get_string().empty()) {
                            role.bucket = bucket->get_string();
                        }
                    }
                    {
                        const auto* scope = entry.find("scope_name");
                        if (scope != nullptr && !scope->get_string().empty()) {
                            role.scope = scope->get_string();
                        }
                    }
                    {
                        const auto* collection = entry.find("collection_name");
                        if (collection != nullptr && !collection->get_string().empty()) {
                            role.collection = collection->get_string();
                        }
                    }
                    result.roles.push_back(role);
                }
            }
        }
        return result;
    }
};
} // namespace tao::json

template<>
struct fmt::formatter<couchbase::operations::rbac::auth_domain> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::operations::rbac::auth_domain domain, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (domain) {
            case couchbase::operations::rbac::auth_domain::unknown:
                name = "unknown";
                break;
            case couchbase::operations::rbac::auth_domain::local:
                name = "local";
                break;
            case couchbase::operations::rbac::auth_domain::external:
                name = "external";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
