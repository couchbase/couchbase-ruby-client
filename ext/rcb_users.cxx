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

#include <core/cluster.hxx>
#include <core/management/rbac.hxx>
#include <core/operations/management/change_password.hxx>
#include <core/operations/management/group_drop.hxx>
#include <core/operations/management/group_get.hxx>
#include <core/operations/management/group_get_all.hxx>
#include <core/operations/management/group_upsert.hxx>
#include <core/operations/management/role_get_all.hxx>
#include <core/operations/management/user_drop.hxx>
#include <core/operations/management/user_get.hxx>
#include <core/operations/management/user_get_all.hxx>
#include <core/operations/management/user_upsert.hxx>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <future>
#include <memory>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
void
cb_extract_role(const core::management::rbac::role_and_description& entry, VALUE role)
{
  rb_hash_aset(role, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
  rb_hash_aset(role, rb_id2sym(rb_intern("display_name")), cb_str_new(entry.display_name));
  rb_hash_aset(role, rb_id2sym(rb_intern("description")), cb_str_new(entry.description));
  if (entry.bucket) {
    rb_hash_aset(role, rb_id2sym(rb_intern("bucket")), cb_str_new(entry.bucket.value()));
  }
  if (entry.scope) {
    rb_hash_aset(role, rb_id2sym(rb_intern("scope")), cb_str_new(entry.scope.value()));
  }
  if (entry.collection) {
    rb_hash_aset(role, rb_id2sym(rb_intern("collection")), cb_str_new(entry.collection.value()));
  }
}

VALUE
cb_Backend_role_get_all(VALUE self, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::role_get_all_request req{};
    cb_extract_timeout(req, timeout);
    auto promise =
      std::make_shared<std::promise<core::operations::management::role_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to fetch roles");
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(resp.roles.size()));
    for (const auto& entry : resp.roles) {
      VALUE role = rb_hash_new();
      cb_extract_role(entry, role);
      rb_ary_push(res, role);
    }
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

void
cb_extract_user(const core::management::rbac::user_and_metadata& entry, VALUE user)
{
  rb_hash_aset(user, rb_id2sym(rb_intern("username")), cb_str_new(entry.username));
  switch (entry.domain) {
    case core::management::rbac::auth_domain::local:
      rb_hash_aset(user, rb_id2sym(rb_intern("domain")), rb_id2sym(rb_intern("local")));
      break;
    case core::management::rbac::auth_domain::external:
      rb_hash_aset(user, rb_id2sym(rb_intern("domain")), rb_id2sym(rb_intern("external")));
      break;
    case core::management::rbac::auth_domain::unknown:
      break;
  }
  VALUE external_groups = rb_ary_new_capa(static_cast<long>(entry.external_groups.size()));
  for (const auto& group : entry.external_groups) {
    rb_ary_push(external_groups, cb_str_new(group));
  }
  rb_hash_aset(user, rb_id2sym(rb_intern("external_groups")), external_groups);
  VALUE groups = rb_ary_new_capa(static_cast<long>(entry.groups.size()));
  for (const auto& group : entry.groups) {
    rb_ary_push(groups, cb_str_new(group));
  }
  rb_hash_aset(user, rb_id2sym(rb_intern("groups")), groups);
  if (entry.display_name) {
    rb_hash_aset(
      user, rb_id2sym(rb_intern("display_name")), cb_str_new(entry.display_name.value()));
  }
  if (entry.password_changed) {
    rb_hash_aset(
      user, rb_id2sym(rb_intern("password_changed")), cb_str_new(entry.password_changed.value()));
  }
  VALUE effective_roles = rb_ary_new_capa(static_cast<long>(entry.effective_roles.size()));
  for (const auto& er : entry.effective_roles) {
    VALUE role = rb_hash_new();
    rb_hash_aset(role, rb_id2sym(rb_intern("name")), cb_str_new(er.name));
    if (er.bucket) {
      rb_hash_aset(role, rb_id2sym(rb_intern("bucket")), cb_str_new(er.bucket.value()));
    }
    if (er.scope) {
      rb_hash_aset(role, rb_id2sym(rb_intern("scope")), cb_str_new(er.scope.value()));
    }
    if (er.collection) {
      rb_hash_aset(role, rb_id2sym(rb_intern("collection")), cb_str_new(er.collection.value()));
    }
    VALUE origins = rb_ary_new_capa(static_cast<long>(er.origins.size()));
    for (const auto& orig : er.origins) {
      VALUE origin = rb_hash_new();
      rb_hash_aset(origin, rb_id2sym(rb_intern("type")), cb_str_new(orig.type));
      if (orig.name) {
        rb_hash_aset(origin, rb_id2sym(rb_intern("name")), cb_str_new(orig.name.value()));
      }
      rb_ary_push(origins, origin);
    }
    rb_hash_aset(role, rb_id2sym(rb_intern("origins")), origins);
    rb_ary_push(effective_roles, role);
  }
  rb_hash_aset(user, rb_id2sym(rb_intern("effective_roles")), effective_roles);

  VALUE roles = rb_ary_new_capa(static_cast<long>(entry.roles.size()));
  for (const auto& er : entry.roles) {
    VALUE role = rb_hash_new();
    rb_hash_aset(role, rb_id2sym(rb_intern("name")), cb_str_new(er.name));
    if (er.bucket) {
      rb_hash_aset(role, rb_id2sym(rb_intern("bucket")), cb_str_new(er.bucket.value()));
    }
    if (er.scope) {
      rb_hash_aset(role, rb_id2sym(rb_intern("scope")), cb_str_new(er.scope.value()));
    }
    if (er.collection) {
      rb_hash_aset(role, rb_id2sym(rb_intern("collection")), cb_str_new(er.collection.value()));
    }
    rb_ary_push(roles, role);
  }
  rb_hash_aset(user, rb_id2sym(rb_intern("roles")), roles);
}

VALUE
cb_Backend_user_get_all(VALUE self, VALUE domain, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(domain, T_SYMBOL);

  try {
    core::operations::management::user_get_all_request req{};
    cb_extract_timeout(req, timeout);
    if (domain == rb_id2sym(rb_intern("local"))) {
      req.domain = core::management::rbac::auth_domain::local;
    } else if (domain == rb_id2sym(rb_intern("external"))) {
      req.domain = core::management::rbac::auth_domain::external;
    } else {
      throw ruby_exception(exc_invalid_argument(),
                           rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
    }
    auto promise =
      std::make_shared<std::promise<core::operations::management::user_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to fetch users");
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(resp.users.size()));
    for (const auto& entry : resp.users) {
      VALUE user = rb_hash_new();
      cb_extract_user(entry, user);
      rb_ary_push(res, user);
    }
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_user_get(VALUE self, VALUE domain, VALUE username, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(domain, T_SYMBOL);
  Check_Type(username, T_STRING);

  try {
    core::operations::management::user_get_request req{};
    cb_extract_timeout(req, timeout);
    if (domain == rb_id2sym(rb_intern("local"))) {
      req.domain = core::management::rbac::auth_domain::local;
    } else if (domain == rb_id2sym(rb_intern("external"))) {
      req.domain = core::management::rbac::auth_domain::external;
    } else {
      throw ruby_exception(exc_invalid_argument(),
                           rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
    }
    req.username = cb_string_new(username);
    auto promise =
      std::make_shared<std::promise<core::operations::management::user_get_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, fmt::format(R"(unable to fetch user "{}")", req.username));
    }

    VALUE res = rb_hash_new();
    cb_extract_user(resp.user, res);
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_user_drop(VALUE self, VALUE domain, VALUE username, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(domain, T_SYMBOL);
  Check_Type(username, T_STRING);

  try {
    core::operations::management::user_drop_request req{};
    cb_extract_timeout(req, timeout);
    if (domain == rb_id2sym(rb_intern("local"))) {
      req.domain = core::management::rbac::auth_domain::local;
    } else if (domain == rb_id2sym(rb_intern("external"))) {
      req.domain = core::management::rbac::auth_domain::external;
    } else {
      throw ruby_exception(exc_invalid_argument(),
                           rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
    }
    req.username = cb_string_new(username);
    auto promise =
      std::make_shared<std::promise<core::operations::management::user_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx, fmt::format(R"(unable to fetch user "{}")", req.username));
    }

    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_user_upsert(VALUE self, VALUE domain, VALUE user, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(domain, T_SYMBOL);
  Check_Type(user, T_HASH);

  try {
    core::operations::management::user_upsert_request req{};
    cb_extract_timeout(req, timeout);
    if (domain == rb_id2sym(rb_intern("local"))) {
      req.domain = core::management::rbac::auth_domain::local;
    } else if (domain == rb_id2sym(rb_intern("external"))) {
      req.domain = core::management::rbac::auth_domain::external;
    } else {
      throw ruby_exception(exc_invalid_argument(),
                           rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
    }
    VALUE name = rb_hash_aref(user, rb_id2sym(rb_intern("username")));
    if (NIL_P(name) || TYPE(name) != T_STRING) {
      throw ruby_exception(exc_invalid_argument(), "unable to upsert user: missing name");
    }
    req.user.username = cb_string_new(name);
    if (VALUE display_name = rb_hash_aref(user, rb_id2sym(rb_intern("display_name")));
        !NIL_P(display_name) && TYPE(display_name) == T_STRING) {
      req.user.display_name = cb_string_new(display_name);
    }
    if (VALUE password = rb_hash_aref(user, rb_id2sym(rb_intern("password")));
        !NIL_P(password) && TYPE(password) == T_STRING) {
      req.user.password = cb_string_new(password);
    }
    if (VALUE groups = rb_hash_aref(user, rb_id2sym(rb_intern("groups")));
        !NIL_P(groups) && TYPE(groups) == T_ARRAY) {
      auto groups_size = static_cast<std::size_t>(RARRAY_LEN(groups));
      for (std::size_t i = 0; i < groups_size; ++i) {
        if (VALUE entry = rb_ary_entry(groups, static_cast<long>(i)); TYPE(entry) == T_STRING) {
          req.user.groups.emplace(cb_string_new(entry));
        }
      }
    }
    if (VALUE roles = rb_hash_aref(user, rb_id2sym(rb_intern("roles")));
        !NIL_P(roles) && TYPE(roles) == T_ARRAY) {
      auto roles_size = static_cast<std::size_t>(RARRAY_LEN(roles));
      req.user.roles.reserve(roles_size);

      for (std::size_t i = 0; i < roles_size; ++i) {
        VALUE entry = rb_ary_entry(roles, static_cast<long>(i));
        if (TYPE(entry) == T_HASH) {
          core::management::rbac::role role{};
          VALUE role_name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
          role.name = cb_string_new(role_name);
          if (VALUE bucket = rb_hash_aref(entry, rb_id2sym(rb_intern("bucket")));
              !NIL_P(bucket) && TYPE(bucket) == T_STRING) {
            role.bucket = cb_string_new(bucket);
            VALUE scope = rb_hash_aref(entry, rb_id2sym(rb_intern("scope")));
            if (!NIL_P(scope) && TYPE(scope) == T_STRING) {
              role.scope = cb_string_new(scope);
              VALUE collection = rb_hash_aref(entry, rb_id2sym(rb_intern("collection")));
              if (!NIL_P(collection) && TYPE(collection) == T_STRING) {
                role.collection = cb_string_new(collection);
              }
            }
          }
          req.user.roles.emplace_back(role);
        }
      }
    }

    auto promise =
      std::make_shared<std::promise<core::operations::management::user_upsert_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to upsert user "{}" ({}))",
                                 req.user.username,
                                 fmt::join(resp.errors, ", ")));
    }

    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_change_password(VALUE self, VALUE new_password, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(new_password, T_STRING);

  try {
    core::operations::management::change_password_request req{};
    cb_extract_timeout(req, timeout);
    req.newPassword = cb_string_new(new_password);
    auto promise =
      std::make_shared<std::promise<core::operations::management::change_password_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to change password");
    }

    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

void
cb_extract_group(const core::management::rbac::group& entry, VALUE group)
{
  rb_hash_aset(group, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
  if (entry.description) {
    rb_hash_aset(group, rb_id2sym(rb_intern("description")), cb_str_new(entry.description.value()));
  }
  if (entry.ldap_group_reference) {
    rb_hash_aset(group,
                 rb_id2sym(rb_intern("ldap_group_reference")),
                 cb_str_new(entry.ldap_group_reference.value()));
  }
  VALUE roles = rb_ary_new_capa(static_cast<long>(entry.roles.size()));
  for (const auto& er : entry.roles) {
    VALUE role = rb_hash_new();
    rb_hash_aset(role, rb_id2sym(rb_intern("name")), cb_str_new(er.name));
    if (er.bucket) {
      rb_hash_aset(role, rb_id2sym(rb_intern("bucket")), cb_str_new(er.bucket.value()));
    }
    if (er.scope) {
      rb_hash_aset(role, rb_id2sym(rb_intern("scope")), cb_str_new(er.scope.value()));
    }
    if (er.collection) {
      rb_hash_aset(role, rb_id2sym(rb_intern("collection")), cb_str_new(er.collection.value()));
    }
    rb_ary_push(roles, role);
  }
  rb_hash_aset(group, rb_id2sym(rb_intern("roles")), roles);
}

VALUE
cb_Backend_group_get_all(VALUE self, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::group_get_all_request req{};
    cb_extract_timeout(req, timeout);
    auto promise =
      std::make_shared<std::promise<core::operations::management::group_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to fetch groups");
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(resp.groups.size()));
    for (const auto& entry : resp.groups) {
      VALUE group = rb_hash_new();
      cb_extract_group(entry, group);
      rb_ary_push(res, group);
    }
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_group_get(VALUE self, VALUE name, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(name, T_STRING);

  try {
    core::operations::management::group_get_request req{};
    cb_extract_timeout(req, timeout);
    req.name = cb_string_new(name);
    auto promise =
      std::make_shared<std::promise<core::operations::management::group_get_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, fmt::format(R"(unable to fetch group "{}")", req.name));
    }

    VALUE res = rb_hash_new();
    cb_extract_group(resp.group, res);
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_group_drop(VALUE self, VALUE name, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(name, T_STRING);

  try {
    core::operations::management::group_drop_request req{};
    cb_extract_timeout(req, timeout);
    req.name = cb_string_new(name);
    auto promise =
      std::make_shared<std::promise<core::operations::management::group_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx, fmt::format(R"(unable to drop group "{}")", req.name));
    }
    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_group_upsert(VALUE self, VALUE group, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(group, T_HASH);

  try {
    core::operations::management::group_upsert_request req{};
    cb_extract_timeout(req, timeout);

    if (VALUE name = rb_hash_aref(group, rb_id2sym(rb_intern("name")));
        !NIL_P(name) && TYPE(name) == T_STRING) {
      req.group.name = cb_string_new(name);
    } else {
      throw ruby_exception(exc_invalid_argument(), "unable to upsert group: missing name");
    }
    if (VALUE ldap_group_ref = rb_hash_aref(group, rb_id2sym(rb_intern("ldap_group_reference")));
        !NIL_P(ldap_group_ref) && TYPE(ldap_group_ref) == T_STRING) {
      req.group.ldap_group_reference = cb_string_new(ldap_group_ref);
    }
    if (VALUE description = rb_hash_aref(group, rb_id2sym(rb_intern("description")));
        !NIL_P(description) && TYPE(description) == T_STRING) {
      req.group.description = cb_string_new(description);
    }
    if (VALUE roles = rb_hash_aref(group, rb_id2sym(rb_intern("roles")));
        !NIL_P(roles) && TYPE(roles) == T_ARRAY) {
      auto roles_size = static_cast<std::size_t>(RARRAY_LEN(roles));
      req.group.roles.reserve(roles_size);
      for (std::size_t i = 0; i < roles_size; ++i) {
        if (VALUE entry = rb_ary_entry(roles, static_cast<long>(i)); TYPE(entry) == T_HASH) {
          core::management::rbac::role role{};
          VALUE role_name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
          role.name = cb_string_new(role_name);
          if (VALUE bucket = rb_hash_aref(entry, rb_id2sym(rb_intern("bucket")));
              !NIL_P(bucket) && TYPE(bucket) == T_STRING) {
            role.bucket = cb_string_new(bucket);
            VALUE scope = rb_hash_aref(entry, rb_id2sym(rb_intern("scope")));
            if (!NIL_P(scope) && TYPE(scope) == T_STRING) {
              role.scope = cb_string_new(scope);
              VALUE collection = rb_hash_aref(entry, rb_id2sym(rb_intern("collection")));
              if (!NIL_P(collection) && TYPE(collection) == T_STRING) {
                role.collection = cb_string_new(collection);
              }
            }
          }
          req.group.roles.emplace_back(role);
        }
      }
    }

    auto promise =
      std::make_shared<std::promise<core::operations::management::group_upsert_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to upsert group "{}" ({}))",
                                 req.group.name,
                                 fmt::join(resp.errors, ", ")));
    }
    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}
} // namespace

void
init_users(VALUE cBackend)
{
  rb_define_method(cBackend, "role_get_all", cb_Backend_role_get_all, 1);
  rb_define_method(cBackend, "user_get_all", cb_Backend_user_get_all, 2);
  rb_define_method(cBackend, "user_get", cb_Backend_user_get, 3);
  rb_define_method(cBackend, "user_drop", cb_Backend_user_drop, 3);
  rb_define_method(cBackend, "user_upsert", cb_Backend_user_upsert, 3);
  rb_define_method(cBackend, "group_get_all", cb_Backend_group_get_all, 1);
  rb_define_method(cBackend, "group_get", cb_Backend_group_get, 2);
  rb_define_method(cBackend, "group_drop", cb_Backend_group_drop, 2);
  rb_define_method(cBackend, "group_upsert", cb_Backend_group_upsert, 2);

  rb_define_method(cBackend, "change_password", cb_Backend_change_password, 2);
}
} // namespace couchbase::ruby
