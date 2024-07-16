/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "ext_build_info.hxx"
#include "ext_build_version.hxx"

#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>
#include <core/utils/json.hxx>

#include <ruby.h>
#if defined(HAVE_RUBY_VERSION_H)
#include <ruby/version.h>
#endif

#include "rcb_utils.hxx"
#include "rcb_version.hxx"

namespace couchbase::ruby
{
namespace
{
std::string
user_agent_with_extra()
{
    constexpr auto uuid{ "00000000-0000-0000-0000-000000000000" };
    auto hello = core::meta::user_agent_for_mcbp(uuid, uuid, user_agent_extra());
    auto json = core::utils::json::parse(hello.data(), hello.size());
    return json["a"].get_string();
}
} // namespace

auto
user_agent_extra() -> const std::string&
{
    static std::string user_agent_value;
    if (user_agent_value.empty()) {
        user_agent_value = fmt::format("ruby_sdk/{}", std::string(EXT_GIT_REVISION).substr(0, 8));
#if defined(HAVE_RUBY_VERSION_H)
        user_agent_value.append(fmt::format(";ruby_abi/{}.{}.{}", RUBY_API_VERSION_MAJOR, RUBY_API_VERSION_MINOR, RUBY_API_VERSION_TEENY));
#endif
    }
    return user_agent_value;
}

void
init_version(VALUE mCouchbase)
{
    VALUE cb_Version{};
    if (rb_const_defined(mCouchbase, rb_intern("VERSION")) != 0) {
        cb_Version = rb_const_get(mCouchbase, rb_intern("VERSION"));
    } else {
        cb_Version = rb_hash_new();
        rb_const_set(mCouchbase, rb_intern("VERSION"), cb_Version);
    }

#if defined(HAVE_RUBY_VERSION_H)
    rb_hash_aset(
      cb_Version,
      rb_id2sym(rb_intern("ruby_abi")),
      rb_str_freeze(cb_str_new(fmt::format("{}.{}.{}", RUBY_API_VERSION_MAJOR, RUBY_API_VERSION_MINOR, RUBY_API_VERSION_TEENY))));
#endif
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("revision")), rb_str_freeze(rb_str_new_cstr(EXT_GIT_REVISION)));

    VALUE version_info = rb_inspect(cb_Version);
    CB_LOG_DEBUG("couchbase backend has been initialized: {}",
                 std::string_view(RSTRING_PTR(version_info), static_cast<std::size_t>(RSTRING_LEN(version_info))));

    VALUE cb_BuildInfo = rb_hash_new();
    rb_const_set(mCouchbase, rb_intern("BUILD_INFO"), cb_BuildInfo);
#if defined(HAVE_RUBY_VERSION_H)
    rb_hash_aset(
      cb_BuildInfo,
      rb_id2sym(rb_intern("ruby_abi")),
      rb_str_freeze(cb_str_new(fmt::format("{}.{}.{}", RUBY_API_VERSION_MAJOR, RUBY_API_VERSION_MINOR, RUBY_API_VERSION_TEENY))));
#endif
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("revision")), rb_str_freeze(rb_str_new_cstr(EXT_GIT_REVISION)));
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("ruby_librubyarg")), rb_str_freeze(rb_str_new_cstr(RUBY_LIBRUBYARG)));
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("ruby_include_dir")), rb_str_freeze(rb_str_new_cstr(RUBY_INCLUDE_DIR)));
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("ruby_library_dir")), rb_str_freeze(rb_str_new_cstr(RUBY_LIBRARY_DIR)));
    const auto user_agent = user_agent_with_extra();
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("user_agent")), rb_str_freeze(cb_str_new(user_agent)));

    VALUE cb_CoreInfo = rb_hash_new();
    for (const auto& [name, value] : core::meta::sdk_build_info()) {
        if (name == "version_major" || name == "version_minor" || name == "version_patch" || name == "version_build" ||
            name == "__cplusplus" || name == "_MSC_VER" || name == "mozilla_ca_bundle_size") {
            rb_hash_aset(cb_CoreInfo, rb_id2sym(rb_intern(name.c_str())), INT2FIX(std::stoi(value)));
        } else if (name == "snapshot" || name == "static_stdlib" || name == "static_openssl" || name == "static_boringssl" ||
                   name == "mozilla_ca_bundle_embedded") {
            rb_hash_aset(cb_CoreInfo, rb_id2sym(rb_intern(name.c_str())), value == "true" ? Qtrue : Qfalse);
        } else {
            rb_hash_aset(cb_CoreInfo, rb_id2sym(rb_intern(name.c_str())), rb_str_freeze(rb_str_new_cstr(value.c_str())));
        }
    }
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("cxx_client")), cb_CoreInfo);
    VALUE build_info = rb_inspect(cb_BuildInfo);
    CB_LOG_DEBUG("couchbase backend build info: {}",
                 std::string_view(RSTRING_PTR(build_info), static_cast<std::size_t>(RSTRING_LEN(build_info))));
}

} // namespace couchbase::ruby
