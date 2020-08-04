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

#include <build_config.hxx>

#include <openssl/crypto.h>
#include <asio/version.hpp>

#include <spdlog/spdlog.h>
#include <spdlog/cfg/env.h>

#include <http_parser.h>

#include <snappy.h>

#include <version.hxx>
#include <cluster.hxx>
#include <operations.hxx>

#include <io/dns_client.hxx>
#include <utils/connection_string.hxx>

#include <ruby.h>
#if defined(HAVE_RUBY_VERSION_H)
#include <ruby/version.h>
#endif

#if !defined(RB_METHOD_DEFINITION_DECL)
#define VALUE_FUNC(f) reinterpret_cast<VALUE (*)(ANYARGS)>(f)
#define INT_FUNC(f) reinterpret_cast<int (*)(ANYARGS)>(f)
#else
#define VALUE_FUNC(f) (f)
#define INT_FUNC(f) (f)
#endif

static void
init_versions(VALUE mCouchbase)
{
    VALUE cb_Version{};
    if (rb_const_defined(mCouchbase, rb_intern("VERSION")) != 0) {
        cb_Version = rb_const_get(mCouchbase, rb_intern("VERSION"));
    } else {
        cb_Version = rb_hash_new();
        rb_const_set(mCouchbase, rb_intern("VERSION"), cb_Version);
    }
#define VERSION_SPLIT_(VER) (VER) / 100000, (VER) / 100 % 1000, (VER) % 100

    std::string ver;
    ver = fmt::format("{}.{}.{}", BACKEND_VERSION_MAJOR, BACKEND_VERSION_MINOR, BACKEND_VERSION_PATCH);
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("backend")), rb_str_freeze(rb_str_new(ver.c_str(), static_cast<long>(ver.size()))));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("build_timestamp")), rb_str_freeze(rb_str_new_cstr(BACKEND_BUILD_TIMESTAMP)));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("revision")), rb_str_freeze(rb_str_new_cstr(BACKEND_GIT_REVISION)));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("platform")), rb_str_freeze(rb_str_new_cstr(BACKEND_SYSTEM)));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("cpu")), rb_str_freeze(rb_str_new_cstr(BACKEND_SYSTEM_PROCESSOR)));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("cc")), rb_str_freeze(rb_str_new_cstr(BACKEND_C_COMPILER)));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("cxx")), rb_str_freeze(rb_str_new_cstr(BACKEND_CXX_COMPILER)));
#if defined(HAVE_RUBY_VERSION_H)
    ver = fmt::format("{}.{}.{}", RUBY_API_VERSION_MAJOR, RUBY_API_VERSION_MINOR, RUBY_API_VERSION_TEENY);
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("ruby")), rb_str_freeze(rb_str_new(ver.c_str(), static_cast<long>(ver.size()))));
#endif
    ver = fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR, SPDLOG_VER_MINOR, SPDLOG_VER_PATCH);
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("spdlog")), rb_str_freeze(rb_str_new(ver.c_str(), static_cast<long>(ver.size()))));
    ver = fmt::format("{}.{}.{}", VERSION_SPLIT_(ASIO_VERSION));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("asio")), rb_str_freeze(rb_str_new(ver.c_str(), static_cast<long>(ver.size()))));
    ver = fmt::format("{}.{}.{}", SNAPPY_MAJOR, SNAPPY_MINOR, SNAPPY_PATCHLEVEL);
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("snappy")), rb_str_freeze(rb_str_new(ver.c_str(), static_cast<long>(ver.size()))));
    ver = fmt::format("{}.{}.{}", HTTP_PARSER_VERSION_MAJOR, HTTP_PARSER_VERSION_MINOR, HTTP_PARSER_VERSION_PATCH);
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("http_parser")), rb_str_freeze(rb_str_new(ver.c_str(), static_cast<long>(ver.size()))));
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("openssl_headers")), rb_str_freeze(rb_str_new_cstr(OPENSSL_VERSION_TEXT)));
#if defined(OPENSSL_VERSION)
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("openssl_runtime")), rb_str_freeze(rb_str_new_cstr(OpenSSL_version(OPENSSL_VERSION))));
#elif defined(SSLEAY_VERSION)
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("openssl_runtime")), rb_str_freeze(rb_str_new_cstr(SSLeay_version(SSLEAY_VERSION))));
#endif

#undef VERSION_SPLIT_

    VALUE version_info = rb_inspect(cb_Version);
    spdlog::info("couchbase backend has been initialized: {}",
                 std::string_view(RSTRING_PTR(version_info), static_cast<std::size_t>(RSTRING_LEN(version_info))));
}

struct cb_backend_data {
    std::unique_ptr<asio::io_context> ctx;
    std::unique_ptr<couchbase::cluster> cluster;
    std::thread worker;
};

static void
cb__backend_close(cb_backend_data* backend)
{
    if (backend->cluster) {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        backend->cluster->close([barrier]() { barrier->set_value(); });
        f.wait();
        if (backend->worker.joinable()) {
            backend->worker.join();
        }
        backend->cluster.reset(nullptr);
        backend->ctx.reset(nullptr);
    }
}

static void
cb_Backend_mark(void* /* ptr */)
{
}

static void
cb_Backend_free(void* ptr)
{
    auto* backend = reinterpret_cast<cb_backend_data*>(ptr);
    cb__backend_close(backend);
    ruby_xfree(backend);
}

static size_t
cb_Backend_memsize(const void* ptr)
{
    const auto* backend = reinterpret_cast<const cb_backend_data*>(ptr);
    return sizeof(*backend) + sizeof(*backend->cluster);
}

static const rb_data_type_t cb_backend_type{
    "Couchbase/Backend",
    { cb_Backend_mark,
      cb_Backend_free,
      cb_Backend_memsize,
// only one reserved field when GC.compact implemented
#ifdef T_MOVED
      nullptr,
#endif
      {} },
#ifdef RUBY_TYPED_FREE_IMMEDIATELY
    nullptr,
    nullptr,
    RUBY_TYPED_FREE_IMMEDIATELY,
#endif
};

static VALUE
cb_Backend_allocate(VALUE klass)
{
    cb_backend_data* backend = nullptr;
    VALUE obj = TypedData_Make_Struct(klass, cb_backend_data, &cb_backend_type, backend);
    backend->ctx = std::make_unique<asio::io_context>();
    backend->cluster = std::make_unique<couchbase::cluster>(*backend->ctx);
    backend->worker = std::thread([backend]() { backend->ctx->run(); });
    return obj;
}

static VALUE eCouchbaseError;
static VALUE eAmbiguousTimeout;
static VALUE eAuthenticationFailure;
static VALUE eBucketExists;
static VALUE eBucketNotFlushable;
static VALUE eBucketNotFound;
static VALUE eCasMismatch;
static VALUE eCollectionExists;
static VALUE eCollectionNotFound;
static VALUE eCompilationFailure;
static VALUE eDatasetExists;
static VALUE eDatasetNotFound;
static VALUE eDataverseExists;
static VALUE eDataverseNotFound;
static VALUE eDecodingFailure;
static VALUE eDeltaInvalid;
static VALUE eDesignDocumentNotFound;
static VALUE eDocumentExists;
static VALUE eDocumentIrretrievable;
static VALUE eDocumentLocked;
static VALUE eDocumentNotFound;
static VALUE eDocumentNotJson;
static VALUE eDurabilityAmbiguous;
static VALUE eDurabilityImpossible;
static VALUE eDurabilityLevelNotAvailable;
static VALUE eDurableWriteInProgress;
static VALUE eDurableWriteReCommitInProgress;
static VALUE eEncodingFailure;
static VALUE eFeatureNotAvailable;
static VALUE eGroupNotFound;
static VALUE eIndexExists;
static VALUE eIndexFailure;
static VALUE eIndexNotFound;
static VALUE eInternalServerFailure;
static VALUE eInvalidArgument;
static VALUE eJobQueueFull;
static VALUE eLinkNotFound;
static VALUE eNumberTooBig;
static VALUE eParsingFailure;
static VALUE ePathExists;
static VALUE ePathInvalid;
static VALUE ePathMismatch;
static VALUE ePathNotFound;
static VALUE ePathTooBig;
static VALUE ePathTooDeep;
static VALUE ePlanningFailure;
static VALUE ePreparedStatementFailure;
static VALUE eRequestCanceled;
static VALUE eScopeExists;
static VALUE eScopeNotFound;
static VALUE eServiceNotAvailable;
static VALUE eTemporaryFailure;
static VALUE eUnambiguousTimeout;
static VALUE eUnsupportedOperation;
static VALUE eUserNotFound;
static VALUE eUserExists;
static VALUE eValueInvalid;
static VALUE eValueTooDeep;
static VALUE eValueTooLarge;
static VALUE eViewNotFound;
static VALUE eXattrCannotModifyVirtualAttribute;
static VALUE eXattrInvalidKeyCombo;
static VALUE eXattrUnknownMacro;
static VALUE eXattrUnknownVirtualAttribute;

static void
init_exceptions(VALUE mCouchbase)
{
    VALUE mError = rb_define_module_under(mCouchbase, "Error");
    eCouchbaseError = rb_define_class_under(mError, "CouchbaseError", rb_eStandardError);

    eAmbiguousTimeout = rb_define_class_under(mError, "AmbiguousTimeout", eCouchbaseError);
    eAuthenticationFailure = rb_define_class_under(mError, "AuthenticationFailure", eCouchbaseError);
    eBucketExists = rb_define_class_under(mError, "BucketExists", eCouchbaseError);
    eBucketNotFlushable = rb_define_class_under(mError, "BucketNotFlushable", eCouchbaseError);
    eBucketNotFound = rb_define_class_under(mError, "BucketNotFound", eCouchbaseError);
    eCasMismatch = rb_define_class_under(mError, "CasMismatch", eCouchbaseError);
    eCollectionExists = rb_define_class_under(mError, "CollectionExists", eCouchbaseError);
    eCollectionNotFound = rb_define_class_under(mError, "CollectionNotFound", eCouchbaseError);
    eCompilationFailure = rb_define_class_under(mError, "CompilationFailure", eCouchbaseError);
    eDatasetExists = rb_define_class_under(mError, "DatasetExists", eCouchbaseError);
    eDatasetNotFound = rb_define_class_under(mError, "DatasetNotFound", eCouchbaseError);
    eDataverseExists = rb_define_class_under(mError, "DataverseExists", eCouchbaseError);
    eDataverseNotFound = rb_define_class_under(mError, "DataverseNotFound", eCouchbaseError);
    eDecodingFailure = rb_define_class_under(mError, "DecodingFailure", eCouchbaseError);
    eDeltaInvalid = rb_define_class_under(mError, "DeltaInvalid", eCouchbaseError);
    eDesignDocumentNotFound = rb_define_class_under(mError, "DesignDocumentNotFound", eCouchbaseError);
    eDocumentExists = rb_define_class_under(mError, "DocumentExists", eCouchbaseError);
    eDocumentIrretrievable = rb_define_class_under(mError, "DocumentIrretrievable", eCouchbaseError);
    eDocumentLocked = rb_define_class_under(mError, "DocumentLocked", eCouchbaseError);
    eDocumentNotFound = rb_define_class_under(mError, "DocumentNotFound", eCouchbaseError);
    eDocumentNotJson = rb_define_class_under(mError, "DocumentNotJson", eCouchbaseError);
    eDurabilityAmbiguous = rb_define_class_under(mError, "DurabilityAmbiguous", eCouchbaseError);
    eDurabilityImpossible = rb_define_class_under(mError, "DurabilityImpossible", eCouchbaseError);
    eDurabilityLevelNotAvailable = rb_define_class_under(mError, "DurabilityLevelNotAvailable", eCouchbaseError);
    eDurableWriteInProgress = rb_define_class_under(mError, "DurableWriteInProgress", eCouchbaseError);
    eDurableWriteReCommitInProgress = rb_define_class_under(mError, "DurableWriteReCommitInProgress", eCouchbaseError);
    eEncodingFailure = rb_define_class_under(mError, "EncodingFailure", eCouchbaseError);
    eFeatureNotAvailable = rb_define_class_under(mError, "FeatureNotAvailable", eCouchbaseError);
    eGroupNotFound = rb_define_class_under(mError, "GroupNotFound", eCouchbaseError);
    eIndexExists = rb_define_class_under(mError, "IndexExists", eCouchbaseError);
    eIndexFailure = rb_define_class_under(mError, "IndexFailure", eCouchbaseError);
    eIndexNotFound = rb_define_class_under(mError, "IndexNotFound", eCouchbaseError);
    eInternalServerFailure = rb_define_class_under(mError, "InternalServerFailure", eCouchbaseError);
    eInvalidArgument = rb_define_class_under(mError, "InvalidArgument", rb_eArgError);
    eJobQueueFull = rb_define_class_under(mError, "JobQueueFull", eCouchbaseError);
    eLinkNotFound = rb_define_class_under(mError, "LinkNotFound", eCouchbaseError);
    eNumberTooBig = rb_define_class_under(mError, "NumberTooBig", eCouchbaseError);
    eParsingFailure = rb_define_class_under(mError, "ParsingFailure", eCouchbaseError);
    ePathExists = rb_define_class_under(mError, "PathExists", eCouchbaseError);
    ePathInvalid = rb_define_class_under(mError, "PathInvalid", eCouchbaseError);
    ePathMismatch = rb_define_class_under(mError, "PathMismatch", eCouchbaseError);
    ePathNotFound = rb_define_class_under(mError, "PathNotFound", eCouchbaseError);
    ePathTooBig = rb_define_class_under(mError, "PathTooBig", eCouchbaseError);
    ePathTooDeep = rb_define_class_under(mError, "PathTooDeep", eCouchbaseError);
    ePlanningFailure = rb_define_class_under(mError, "PlanningFailure", eCouchbaseError);
    ePreparedStatementFailure = rb_define_class_under(mError, "PreparedStatementFailure", eCouchbaseError);
    eRequestCanceled = rb_define_class_under(mError, "RequestCanceled", eCouchbaseError);
    eScopeExists = rb_define_class_under(mError, "ScopeExists", eCouchbaseError);
    eScopeNotFound = rb_define_class_under(mError, "ScopeNotFound", eCouchbaseError);
    eServiceNotAvailable = rb_define_class_under(mError, "ServiceNotAvailable", eCouchbaseError);
    eTemporaryFailure = rb_define_class_under(mError, "TemporaryFailure", eCouchbaseError);
    eUnambiguousTimeout = rb_define_class_under(mError, "UnambiguousTimeout", eCouchbaseError);
    eUnsupportedOperation = rb_define_class_under(mError, "UnsupportedOperation", eCouchbaseError);
    eUserNotFound = rb_define_class_under(mError, "UserNotFound", eCouchbaseError);
    eUserExists = rb_define_class_under(mError, "UserExists", eCouchbaseError);
    eValueInvalid = rb_define_class_under(mError, "ValueInvalid", eCouchbaseError);
    eValueTooDeep = rb_define_class_under(mError, "ValueTooDeep", eCouchbaseError);
    eValueTooLarge = rb_define_class_under(mError, "ValueTooLarge", eCouchbaseError);
    eViewNotFound = rb_define_class_under(mError, "ViewNotFound", eCouchbaseError);
    eXattrCannotModifyVirtualAttribute = rb_define_class_under(mError, "XattrCannotModifyVirtualAttribute", eCouchbaseError);
    eXattrInvalidKeyCombo = rb_define_class_under(mError, "XattrInvalidKeyCombo", eCouchbaseError);
    eXattrUnknownMacro = rb_define_class_under(mError, "XattrUnknownMacro", eCouchbaseError);
    eXattrUnknownVirtualAttribute = rb_define_class_under(mError, "XattrUnknownVirtualAttribute", eCouchbaseError);
}

static VALUE
cb__map_error_code(std::error_code ec, const std::string& message)
{
    if (ec.category() == couchbase::error::detail::get_common_category()) {
        switch (static_cast<couchbase::error::common_errc>(ec.value())) {
            case couchbase::error::common_errc::unambiguous_timeout:
                return rb_exc_new_cstr(eUnambiguousTimeout, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::ambiguous_timeout:
                return rb_exc_new_cstr(eAmbiguousTimeout, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::request_canceled:
                return rb_exc_new_cstr(eRequestCanceled, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::invalid_argument:
                return rb_exc_new_cstr(eInvalidArgument, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::service_not_available:
                return rb_exc_new_cstr(eServiceNotAvailable, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::internal_server_failure:
                return rb_exc_new_cstr(eInternalServerFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::authentication_failure:
                return rb_exc_new_cstr(eAuthenticationFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::temporary_failure:
                return rb_exc_new_cstr(eTemporaryFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::parsing_failure:
                return rb_exc_new_cstr(eParsingFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::cas_mismatch:
                return rb_exc_new_cstr(eCasMismatch, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::bucket_not_found:
                return rb_exc_new_cstr(eBucketNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::scope_not_found:
                return rb_exc_new_cstr(eScopeNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::collection_not_found:
                return rb_exc_new_cstr(eCollectionNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::unsupported_operation:
                return rb_exc_new_cstr(eUnsupportedOperation, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::feature_not_available:
                return rb_exc_new_cstr(eFeatureNotAvailable, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::encoding_failure:
                return rb_exc_new_cstr(eEncodingFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::decoding_failure:
                return rb_exc_new_cstr(eDecodingFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::index_not_found:
                return rb_exc_new_cstr(eIndexNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::index_exists:
                return rb_exc_new_cstr(eIndexExists, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_key_value_category()) {
        switch (static_cast<couchbase::error::key_value_errc>(ec.value())) {
            case couchbase::error::key_value_errc::document_not_found:
                return rb_exc_new_cstr(eDocumentNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::document_irretrievable:
                return rb_exc_new_cstr(eDocumentIrretrievable, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::document_locked:
                return rb_exc_new_cstr(eDocumentLocked, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::value_too_large:
                return rb_exc_new_cstr(eValueTooLarge, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::document_exists:
                return rb_exc_new_cstr(eDocumentExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::durability_level_not_available:
                return rb_exc_new_cstr(eDurabilityLevelNotAvailable, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::durability_impossible:
                return rb_exc_new_cstr(eDurabilityImpossible, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::durability_ambiguous:
                return rb_exc_new_cstr(eDurabilityAmbiguous, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::durable_write_in_progress:
                return rb_exc_new_cstr(eDurableWriteInProgress, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::durable_write_re_commit_in_progress:
                return rb_exc_new_cstr(eDurableWriteReCommitInProgress, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::path_not_found:
                return rb_exc_new_cstr(ePathNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::path_mismatch:
                return rb_exc_new_cstr(ePathMismatch, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::path_invalid:
                return rb_exc_new_cstr(ePathInvalid, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::path_too_big:
                return rb_exc_new_cstr(ePathTooBig, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::path_too_deep:
                return rb_exc_new_cstr(ePathTooDeep, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::value_too_deep:
                return rb_exc_new_cstr(eValueTooDeep, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::value_invalid:
                return rb_exc_new_cstr(eValueInvalid, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::document_not_json:
                return rb_exc_new_cstr(eDocumentNotJson, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::number_too_big:
                return rb_exc_new_cstr(eNumberTooBig, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::delta_invalid:
                return rb_exc_new_cstr(eDeltaInvalid, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::path_exists:
                return rb_exc_new_cstr(ePathExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::xattr_unknown_macro:
                return rb_exc_new_cstr(eXattrUnknownMacro, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::xattr_invalid_key_combo:
                return rb_exc_new_cstr(eXattrInvalidKeyCombo, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::xattr_unknown_virtual_attribute:
                return rb_exc_new_cstr(eXattrUnknownVirtualAttribute, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::xattr_cannot_modify_virtual_attribute:
                return rb_exc_new_cstr(eXattrCannotModifyVirtualAttribute, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_query_category()) {
        switch (static_cast<couchbase::error::query_errc>(ec.value())) {
            case couchbase::error::query_errc::planning_failure:
                return rb_exc_new_cstr(ePlanningFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::query_errc::index_failure:
                return rb_exc_new_cstr(eIndexFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::query_errc::prepared_statement_failure:
                return rb_exc_new_cstr(ePreparedStatementFailure, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_view_category()) {
        switch (static_cast<couchbase::error::view_errc>(ec.value())) {
            case couchbase::error::view_errc::view_not_found:
                return rb_exc_new_cstr(eViewNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::view_errc::design_document_not_found:
                return rb_exc_new_cstr(eDesignDocumentNotFound, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_analytics_category()) {
        switch (static_cast<couchbase::error::analytics_errc>(ec.value())) {
            case couchbase::error::analytics_errc::compilation_failure:
                return rb_exc_new_cstr(eCompilationFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::analytics_errc::job_queue_full:
                return rb_exc_new_cstr(eJobQueueFull, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::analytics_errc::dataset_not_found:
                return rb_exc_new_cstr(eDatasetNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::analytics_errc::dataverse_not_found:
                return rb_exc_new_cstr(eDataverseNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::analytics_errc::dataset_exists:
                return rb_exc_new_cstr(eDatasetExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::analytics_errc::dataverse_exists:
                return rb_exc_new_cstr(eDataverseExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::analytics_errc::link_not_found:
                return rb_exc_new_cstr(eLinkNotFound, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_management_category()) {
        switch (static_cast<couchbase::error::management_errc>(ec.value())) {
            case couchbase::error::management_errc::collection_exists:
                return rb_exc_new_cstr(eCollectionExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::management_errc::scope_exists:
                return rb_exc_new_cstr(eScopeExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::management_errc::user_not_found:
                return rb_exc_new_cstr(eUserNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::management_errc::group_not_found:
                return rb_exc_new_cstr(eGroupNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::management_errc::user_exists:
                return rb_exc_new_cstr(eUserExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::management_errc::bucket_exists:
                return rb_exc_new_cstr(eBucketExists, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::management_errc::bucket_not_flushable:
                return rb_exc_new_cstr(eBucketNotFlushable, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    }

    return rb_exc_new_cstr(eCouchbaseError, fmt::format("{}: {}", message, ec.message()).c_str());
}

static VALUE
cb_Backend_open(VALUE self, VALUE connection_string, VALUE username, VALUE password, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }
    Check_Type(connection_string, T_STRING);
    Check_Type(username, T_STRING);
    Check_Type(password, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    VALUE exc = Qnil;
    {
        std::string input(RSTRING_PTR(connection_string), static_cast<size_t>(RSTRING_LEN(connection_string)));
        auto connstr = couchbase::utils::parse_connection_string(input);
        std::string user(RSTRING_PTR(username), static_cast<size_t>(RSTRING_LEN(username)));
        std::string pass(RSTRING_PTR(password), static_cast<size_t>(RSTRING_LEN(password)));
        couchbase::origin origin(user, pass, std::move(connstr));
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        backend->cluster->open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        if (auto ec = f.get()) {
            exc = cb__map_error_code(ec, fmt::format("unable open cluster at {}", origin.next_address().first));
        }
    }
    if (!NIL_P(exc)) {
        rb_exc_raise(exc);
    }

    return Qnil;
}

static VALUE
cb_Backend_close(VALUE self)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);
    cb__backend_close(backend);
    return Qnil;
}

static VALUE
cb_Backend_open_bucket(VALUE self, VALUE bucket, VALUE wait_until_ready)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    bool wait = RTEST(wait_until_ready);

    VALUE exc = Qnil;
    {
        std::string name(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));

        if (wait) {
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            backend->cluster->open_bucket(name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
            if (auto ec = f.get()) {
                exc = cb__map_error_code(ec, fmt::format("unable open bucket \"{}\"", name));
            }
        } else {
            backend->cluster->open_bucket(name, [](std::error_code) {});
        }
    }
    if (!NIL_P(exc)) {
        rb_exc_raise(exc);
    }

    return Qnil;
}

template<typename Request>
void
cb__extract_timeout(Request& req, VALUE timeout)
{
    if (!NIL_P(timeout)) {
        switch (TYPE(timeout)) {
            case T_FIXNUM:
            case T_BIGNUM:
                req.timeout = std::chrono::milliseconds(NUM2ULL(timeout));
                break;
            default:
                rb_raise(rb_eArgError, "timeout must be an Integer");
        }
    }
}

static VALUE
cb_Backend_document_get(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::get_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::get_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::get_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable fetch {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_get_projected(VALUE self,
                                  VALUE bucket,
                                  VALUE collection,
                                  VALUE id,
                                  VALUE timeout,
                                  VALUE with_expiry,
                                  VALUE projections,
                                  VALUE preserve_array_indexes)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::get_projected_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        req.with_expiry = RTEST(with_expiry);
        req.preserve_array_indexes = RTEST(preserve_array_indexes);
        if (!NIL_P(projections)) {
            Check_Type(projections, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(projections));
            req.projections.reserve(entries_num);
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(projections, static_cast<long>(i));
                Check_Type(entry, T_STRING);
                req.projections.emplace_back(std::string(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry))));
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::get_projected_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::get_projected_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable fetch with projections {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        if (resp.expiry) {
            rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(resp.expiry.value()));
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_get_and_lock(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE lock_time)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(lock_time, T_FIXNUM);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::get_and_lock_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        req.lock_time = NUM2UINT(lock_time);

        auto barrier = std::make_shared<std::promise<couchbase::operations::get_and_lock_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::get_and_lock_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable lock and fetch {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_get_and_touch(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE expiry)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(expiry, T_FIXNUM);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::get_and_touch_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        req.expiry = NUM2UINT(expiry);

        auto barrier = std::make_shared<std::promise<couchbase::operations::get_and_touch_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::get_and_touch_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable fetch and touch {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

template<typename Response>
static VALUE
cb__extract_mutation_result(Response resp)
{
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    VALUE token = rb_hash_new();
    rb_hash_aset(token, rb_id2sym(rb_intern("partition_uuid")), ULL2NUM(resp.token.partition_uuid));
    rb_hash_aset(token, rb_id2sym(rb_intern("sequence_number")), ULONG2NUM(resp.token.sequence_number));
    rb_hash_aset(token, rb_id2sym(rb_intern("partition_id")), UINT2NUM(resp.token.partition_id));
    rb_hash_aset(token,
                 rb_id2sym(rb_intern("bucket_name")),
                 rb_str_new(resp.token.bucket_name.c_str(), static_cast<long>(resp.token.bucket_name.size())));
    rb_hash_aset(res, rb_id2sym(rb_intern("mutation_token")), token);
    return res;
}

static VALUE
cb_Backend_document_touch(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE expiry)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(expiry, T_FIXNUM);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::touch_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        req.expiry = NUM2UINT(expiry);

        auto barrier = std::make_shared<std::promise<couchbase::operations::touch_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::touch_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to touch {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_exists(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::exists_request req{ doc_id };
        cb__extract_timeout(req, timeout);

        auto barrier = std::make_shared<std::promise<couchbase::operations::exists_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::exists_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to exists {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("partition_id")), UINT2NUM(resp.partition_id));
        switch (resp.status) {
            case couchbase::operations::exists_response::observe_status::invalid:
                rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("invalid")));
                break;
            case couchbase::operations::exists_response::observe_status::found:
                rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("found")));
                break;
            case couchbase::operations::exists_response::observe_status::not_found:
                rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("not_found")));
                break;
            case couchbase::operations::exists_response::observe_status::persisted:
                rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("persisted")));
                break;
            case couchbase::operations::exists_response::observe_status::logically_deleted:
                rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("logically_deleted")));
                break;
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_unlock(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE cas)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::unlock_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        switch (TYPE(cas)) {
            case T_FIXNUM:
            case T_BIGNUM:
                req.cas = NUM2ULL(cas);
                break;
            default:
                rb_raise(rb_eArgError, "CAS must be an Integer");
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::unlock_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::unlock_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to unlock {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_upsert(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE content, VALUE flags, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    Check_Type(flags, T_FIXNUM);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::upsert_request req{ doc_id, value };
        cb__extract_timeout(req, timeout);
        req.flags = FIX2UINT(flags);

        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
            VALUE expiry = rb_hash_aref(options, rb_id2sym(rb_intern("expiry")));
            if (!NIL_P(expiry)) {
                Check_Type(expiry, T_FIXNUM);
                req.expiry = FIX2UINT(expiry);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::upsert_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to upsert {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        return cb__extract_mutation_result(resp);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_replace(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE content, VALUE flags, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    Check_Type(flags, T_FIXNUM);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::replace_request req{ doc_id, value };
        cb__extract_timeout(req, timeout);
        req.flags = FIX2UINT(flags);

        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
            VALUE expiry = rb_hash_aref(options, rb_id2sym(rb_intern("expiry")));
            if (!NIL_P(expiry)) {
                Check_Type(expiry, T_FIXNUM);
                req.expiry = FIX2UINT(expiry);
            }
            VALUE cas = rb_hash_aref(options, rb_id2sym(rb_intern("cas")));
            if (!NIL_P(cas)) {
                switch (TYPE(cas)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        req.cas = NUM2ULL(cas);
                        break;
                    default:
                        rb_raise(rb_eArgError, "CAS must be an Integer");
                }
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::replace_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::replace_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to replace {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        return cb__extract_mutation_result(resp);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_insert(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE content, VALUE flags, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    Check_Type(flags, T_FIXNUM);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::insert_request req{ doc_id, value };
        cb__extract_timeout(req, timeout);
        req.flags = FIX2UINT(flags);

        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
            VALUE expiry = rb_hash_aref(options, rb_id2sym(rb_intern("expiry")));
            if (!NIL_P(expiry)) {
                Check_Type(expiry, T_FIXNUM);
                req.expiry = FIX2UINT(expiry);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::insert_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::insert_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to insert {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        return cb__extract_mutation_result(resp);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_remove(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::remove_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::remove_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::remove_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to remove {} (opaque={})", doc_id, resp.opaque));
            break;
        }
        return cb__extract_mutation_result(resp);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_increment(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::increment_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
            VALUE delta = rb_hash_aref(options, rb_id2sym(rb_intern("delta")));
            if (!NIL_P(delta)) {
                switch (TYPE(delta)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        req.delta = NUM2ULL(delta);
                        break;
                    default:
                        rb_raise(rb_eArgError, "delta must be an Integer");
                }
            }
            VALUE initial_value = rb_hash_aref(options, rb_id2sym(rb_intern("initial_value")));
            if (!NIL_P(initial_value)) {
                switch (TYPE(initial_value)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        req.initial_value = NUM2ULL(initial_value);
                        break;
                    default:
                        rb_raise(rb_eArgError, "initial_value must be an Integer");
                }
            }
            VALUE expiry = rb_hash_aref(options, rb_id2sym(rb_intern("expiry")));
            if (!NIL_P(expiry)) {
                Check_Type(expiry, T_FIXNUM);
                req.expiry = FIX2UINT(expiry);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::increment_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::increment_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to increment {} by {} (opaque={})", doc_id, req.delta, resp.opaque));
            break;
        }
        VALUE res = cb__extract_mutation_result(resp);
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_decrement(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(options, T_HASH);

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::decrement_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
            VALUE delta = rb_hash_aref(options, rb_id2sym(rb_intern("delta")));
            if (!NIL_P(delta)) {
                switch (TYPE(delta)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        req.delta = NUM2ULL(delta);
                        break;
                    default:
                        rb_raise(rb_eArgError, "delta must be an Integer");
                }
            }
            VALUE initial_value = rb_hash_aref(options, rb_id2sym(rb_intern("initial_value")));
            if (!NIL_P(initial_value)) {
                switch (TYPE(initial_value)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        req.initial_value = NUM2ULL(initial_value);
                        break;
                    default:
                        rb_raise(rb_eArgError, "initial_value must be an Integer");
                }
            }
            VALUE expiry = rb_hash_aref(options, rb_id2sym(rb_intern("expiry")));
            if (!NIL_P(expiry)) {
                Check_Type(expiry, T_FIXNUM);
                req.expiry = FIX2UINT(expiry);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::decrement_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::decrement_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to decrement {} by {} (opaque={})", doc_id, req.delta, resp.opaque));
            break;
        }
        VALUE res = cb__extract_mutation_result(resp);
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb__map_subdoc_opcode(couchbase::protocol::subdoc_opcode opcode)
{
    switch (opcode) {
        case couchbase::protocol::subdoc_opcode::get:
            return rb_id2sym(rb_intern("get"));

        case couchbase::protocol::subdoc_opcode::exists:
            return rb_id2sym(rb_intern("exists"));

        case couchbase::protocol::subdoc_opcode::dict_add:
            return rb_id2sym(rb_intern("dict_add"));

        case couchbase::protocol::subdoc_opcode::dict_upsert:
            return rb_id2sym(rb_intern("dict_upsert"));

        case couchbase::protocol::subdoc_opcode::remove:
            return rb_id2sym(rb_intern("remove"));

        case couchbase::protocol::subdoc_opcode::replace:
            return rb_id2sym(rb_intern("replace"));

        case couchbase::protocol::subdoc_opcode::array_push_last:
            return rb_id2sym(rb_intern("array_push_last"));

        case couchbase::protocol::subdoc_opcode::array_push_first:
            return rb_id2sym(rb_intern("array_push_first"));

        case couchbase::protocol::subdoc_opcode::array_insert:
            return rb_id2sym(rb_intern("array_insert"));

        case couchbase::protocol::subdoc_opcode::array_add_unique:
            return rb_id2sym(rb_intern("array_add_unique"));

        case couchbase::protocol::subdoc_opcode::counter:
            return rb_id2sym(rb_intern("counter"));

        case couchbase::protocol::subdoc_opcode::get_count:
            return rb_id2sym(rb_intern("count"));

        case couchbase::protocol::subdoc_opcode::get_doc:
            return rb_id2sym(rb_intern("get_doc"));

        case couchbase::protocol::subdoc_opcode::set_doc:
            return rb_id2sym(rb_intern("set_doc"));
    }
    return rb_id2sym(rb_intern("unknown"));
}

static void
cb__map_subdoc_status(couchbase::protocol::status status, std::size_t index, const std::string& path, VALUE entry)
{
    switch (status) {
        case couchbase::protocol::status::success:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("success")));
            return;

        case couchbase::protocol::status::subdoc_path_not_found:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("path_not_found")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(ePathNotFound, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_path_mismatch:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("path_mismatch")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(ePathMismatch, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_path_invalid:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("path_invalid")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(ePathInvalid, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_path_too_big:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("path_too_big")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(ePathTooBig, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_value_cannot_insert:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("value_cannot_insert")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(eValueInvalid, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_doc_not_json:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("doc_not_json")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eDocumentNotJson, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_num_range_error:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("num_range")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(eNumberTooBig, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_delta_invalid:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("delta_invalid")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(eDeltaInvalid, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_path_exists:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("path_exists")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(ePathExists, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_value_too_deep:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("value_too_deep")));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("error")), rb_exc_new_cstr(eValueTooDeep, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_invalid_combo:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("invalid_combo")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eInvalidArgument, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_xattr_invalid_flag_combo:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("xattr_invalid_flag_combo")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eXattrInvalidKeyCombo, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_xattr_invalid_key_combo:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("xattr_invalid_key_combo")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eXattrInvalidKeyCombo, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_xattr_unknown_macro:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("xattr_unknown_macro")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eXattrUnknownMacro, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_xattr_unknown_vattr:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("xattr_unknown_vattr")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eXattrUnknownVirtualAttribute, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        case couchbase::protocol::status::subdoc_xattr_cannot_modify_vattr:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("xattr_cannot_modify_vattr")));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("error")),
                         rb_exc_new_cstr(eXattrCannotModifyVirtualAttribute, fmt::format("index={}, path={}", index, path).c_str()));
            return;

        default:
            rb_hash_aset(entry, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern("unknown")));
            rb_hash_aset(
              entry,
              rb_id2sym(rb_intern("error")),
              rb_exc_new_cstr(eCouchbaseError,
                              fmt::format("unknown subdocument error status={}, index={}, path={}", status, index, path).c_str()));
            return;
    }
}

static VALUE
cb_Backend_document_lookup_in(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE access_deleted, VALUE specs)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(specs, T_ARRAY);
    if (RARRAY_LEN(specs) <= 0) {
        rb_raise(rb_eArgError, "Array with specs cannot be empty");
    }

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::lookup_in_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        req.access_deleted = RTEST(access_deleted);
        auto entries_size = static_cast<size_t>(RARRAY_LEN(specs));
        req.specs.entries.reserve(entries_size);
        for (size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            Check_Type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, rb_id2sym(rb_intern("opcode")));
            Check_Type(operation, T_SYMBOL);
            ID operation_id = rb_sym2id(operation);
            couchbase::protocol::subdoc_opcode opcode;
            if (operation_id == rb_intern("get_doc")) {
                opcode = couchbase::protocol::subdoc_opcode::get_doc;
            } else if (operation_id == rb_intern("get")) {
                opcode = couchbase::protocol::subdoc_opcode::get;
            } else if (operation_id == rb_intern("exists")) {
                opcode = couchbase::protocol::subdoc_opcode::exists;
            } else if (operation_id == rb_intern("count")) {
                opcode = couchbase::protocol::subdoc_opcode::get_count;
            } else {
                rb_raise(rb_eArgError, "Unsupported operation for subdocument lookup");
            }
            bool xattr = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("xattr"))));
            VALUE path = rb_hash_aref(entry, rb_id2sym(rb_intern("path")));
            Check_Type(path, T_STRING);
            req.specs.add_spec(opcode, xattr, std::string(RSTRING_PTR(path), static_cast<size_t>(RSTRING_LEN(path))));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::lookup_in_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::lookup_in_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable fetch {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
        VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("fields")), fields);
        for (size_t i = 0; i < resp.fields.size(); ++i) {
            VALUE entry = rb_hash_new();
            rb_hash_aset(entry, rb_id2sym(rb_intern("index")), ULL2NUM(i));
            rb_hash_aset(entry, rb_id2sym(rb_intern("exists")), resp.fields[i].exists ? Qtrue : Qfalse);
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("path")), rb_str_new(resp.fields[i].path.data(), static_cast<long>(resp.fields[i].path.size())));
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("value")),
                         rb_str_new(resp.fields[i].value.data(), static_cast<long>(resp.fields[i].value.size())));
            cb__map_subdoc_status(resp.fields[i].status, i, resp.fields[i].path, entry);
            if (resp.fields[i].opcode == couchbase::protocol::subdoc_opcode::get && resp.fields[i].path.empty()) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("get_doc")));
            } else {
                rb_hash_aset(entry, rb_id2sym(rb_intern("type")), cb__map_subdoc_opcode(resp.fields[i].opcode));
            }
            rb_ary_store(fields, static_cast<long>(i), entry);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_mutate_in(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE timeout, VALUE specs, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(specs, T_ARRAY);
    if (RARRAY_LEN(specs) <= 0) {
        rb_raise(rb_eArgError, "Array with specs cannot be empty");
    }

    VALUE exc = Qnil;
    do {
        couchbase::document_id doc_id;
        doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
        doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
        doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

        couchbase::operations::mutate_in_request req{ doc_id };
        cb__extract_timeout(req, timeout);
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
            if (!NIL_P(durability_level)) {
                Check_Type(durability_level, T_SYMBOL);
                ID level = rb_sym2id(durability_level);
                if (level == rb_intern("none")) {
                    req.durability_level = couchbase::protocol::durability_level::none;
                } else if (level == rb_intern("majority")) {
                    req.durability_level = couchbase::protocol::durability_level::majority;
                } else if (level == rb_intern("majority_and_persist_to_active")) {
                    req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
                } else if (level == rb_intern("persist_to_majority")) {
                    req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
                } else {
                    rb_raise(rb_eArgError, "Unknown durability level");
                }
                VALUE durability_timeout = rb_hash_aref(options, rb_id2sym(rb_intern("durability_timeout")));
                if (!NIL_P(durability_timeout)) {
                    Check_Type(durability_timeout, T_FIXNUM);
                    req.durability_timeout = FIX2UINT(durability_timeout);
                }
            }
            VALUE access_deleted = rb_hash_aref(options, rb_id2sym(rb_intern("access_deleted")));
            if (!NIL_P(access_deleted)) {
                req.access_deleted = RTEST(access_deleted);
            }
            VALUE store_semantics = rb_hash_aref(options, rb_id2sym(rb_intern("store_semantics")));
            if (!NIL_P(store_semantics)) {
                Check_Type(store_semantics, T_SYMBOL);
                ID semantics = rb_sym2id(store_semantics);
                if (semantics == rb_intern("replace")) {
                    req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::replace;
                } else if (semantics == rb_intern("insert")) {
                    req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::insert;
                } else if (semantics == rb_intern("upsert")) {
                    req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::upsert;
                }
            }
            VALUE expiry = rb_hash_aref(options, rb_id2sym(rb_intern("expiry")));
            if (!NIL_P(expiry)) {
                Check_Type(expiry, T_FIXNUM);
                req.expiry = FIX2UINT(expiry);
            }
            VALUE cas = rb_hash_aref(options, rb_id2sym(rb_intern("cas")));
            if (!NIL_P(cas)) {
                switch (TYPE(cas)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        req.cas = NUM2ULL(cas);
                        break;
                    default:
                        rb_raise(rb_eArgError, "CAS must be an Integer");
                }
            }
        }
        auto entries_size = static_cast<size_t>(RARRAY_LEN(specs));
        req.specs.entries.reserve(entries_size);
        for (size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            Check_Type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, rb_id2sym(rb_intern("opcode")));
            Check_Type(operation, T_SYMBOL);
            ID operation_id = rb_sym2id(operation);
            couchbase::protocol::subdoc_opcode opcode;
            if (operation_id == rb_intern("dict_add")) {
                opcode = couchbase::protocol::subdoc_opcode::dict_add;
            } else if (operation_id == rb_intern("dict_upsert")) {
                opcode = couchbase::protocol::subdoc_opcode::dict_upsert;
            } else if (operation_id == rb_intern("remove")) {
                opcode = couchbase::protocol::subdoc_opcode::remove;
            } else if (operation_id == rb_intern("replace")) {
                opcode = couchbase::protocol::subdoc_opcode::replace;
            } else if (operation_id == rb_intern("array_push_last")) {
                opcode = couchbase::protocol::subdoc_opcode::array_push_last;
            } else if (operation_id == rb_intern("array_push_first")) {
                opcode = couchbase::protocol::subdoc_opcode::array_push_first;
            } else if (operation_id == rb_intern("array_insert")) {
                opcode = couchbase::protocol::subdoc_opcode::array_insert;
            } else if (operation_id == rb_intern("array_add_unique")) {
                opcode = couchbase::protocol::subdoc_opcode::array_add_unique;
            } else if (operation_id == rb_intern("counter")) {
                opcode = couchbase::protocol::subdoc_opcode::counter;
            } else if (operation_id == rb_intern("set_doc")) {
                opcode = couchbase::protocol::subdoc_opcode::set_doc;
            } else {
                rb_raise(rb_eArgError, "Unsupported operation for subdocument mutation: %+" PRIsVALUE, operation);
            }
            bool xattr = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("xattr"))));
            bool create_path = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("create_path"))));
            bool expand_macros = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("expand_macros"))));
            VALUE path = rb_hash_aref(entry, rb_id2sym(rb_intern("path")));
            Check_Type(path, T_STRING);
            VALUE param = rb_hash_aref(entry, rb_id2sym(rb_intern("param")));
            if (NIL_P(param)) {
                req.specs.add_spec(opcode, xattr, std::string(RSTRING_PTR(path), static_cast<size_t>(RSTRING_LEN(path))));
            } else if (opcode == couchbase::protocol::subdoc_opcode::counter) {
                Check_Type(param, T_FIXNUM);
                req.specs.add_spec(opcode,
                                   xattr,
                                   create_path,
                                   expand_macros,
                                   std::string(RSTRING_PTR(path), static_cast<size_t>(RSTRING_LEN(path))),
                                   FIX2LONG(param));
            } else {
                Check_Type(param, T_STRING);
                req.specs.add_spec(opcode,
                                   xattr,
                                   create_path,
                                   expand_macros,
                                   std::string(RSTRING_PTR(path), static_cast<size_t>(RSTRING_LEN(path))),
                                   std::string(RSTRING_PTR(param), static_cast<size_t>(RSTRING_LEN(param))));
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::mutate_in_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to mutate {} (opaque={})", doc_id, resp.opaque));
            break;
        }

        VALUE res = cb__extract_mutation_result(resp);
        if (resp.first_error_index) {
            rb_hash_aset(res, rb_id2sym(rb_intern("first_error_index")), ULONG2NUM(resp.first_error_index.value()));
        }
        VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("fields")), fields);
        for (size_t i = 0; i < resp.fields.size(); ++i) {
            VALUE entry = rb_hash_new();
            rb_hash_aset(entry, rb_id2sym(rb_intern("index")), ULL2NUM(i));
            rb_hash_aset(
              entry, rb_id2sym(rb_intern("path")), rb_str_new(resp.fields[i].path.data(), static_cast<long>(resp.fields[i].path.size())));
            if (resp.fields[i].status == couchbase::protocol::status::success ||
                resp.fields[i].status == couchbase::protocol::status::subdoc_success_deleted) {
                if (resp.fields[i].opcode == couchbase::protocol::subdoc_opcode::counter) {
                    if (resp.fields[i].value.size() > 0) {
                        rb_hash_aset(entry, rb_id2sym(rb_intern("value")), LONG2NUM(std::stoll(resp.fields[i].value)));
                    }
                } else {
                    rb_hash_aset(entry,
                                 rb_id2sym(rb_intern("value")),
                                 rb_str_new(resp.fields[i].value.data(), static_cast<long>(resp.fields[i].value.size())));
                }
            }
            cb__map_subdoc_status(resp.fields[i].status, i, resp.fields[i].path, entry);
            rb_hash_aset(entry, rb_id2sym(rb_intern("type")), cb__map_subdoc_opcode(resp.fields[i].opcode));
            rb_ary_store(fields, static_cast<long>(i), entry);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static int
cb__for_each_named_param(VALUE key, VALUE value, VALUE arg)
{
    auto* preq = reinterpret_cast<couchbase::operations::query_request*>(arg);
    Check_Type(key, T_STRING);
    Check_Type(value, T_STRING);
    preq->named_parameters.emplace(
      std::string_view(RSTRING_PTR(key), static_cast<std::size_t>(RSTRING_LEN(key))),
      tao::json::from_string(std::string_view(RSTRING_PTR(value), static_cast<std::size_t>(RSTRING_LEN(value)))));
    return ST_CONTINUE;
}

static VALUE
cb_Backend_document_query(VALUE self, VALUE statement, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(statement, T_STRING);
    Check_Type(options, T_HASH);

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_request req;
        req.statement.assign(RSTRING_PTR(statement), static_cast<size_t>(RSTRING_LEN(statement)));
        VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
        if (!NIL_P(client_context_id)) {
            Check_Type(client_context_id, T_STRING);
            req.client_context_id.assign(RSTRING_PTR(client_context_id), static_cast<size_t>(RSTRING_LEN(client_context_id)));
        }
        cb__extract_timeout(req, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
        VALUE adhoc = rb_hash_aref(options, rb_id2sym(rb_intern("adhoc")));
        if (!NIL_P(adhoc)) {
            req.adhoc = RTEST(adhoc);
        }
        VALUE metrics = rb_hash_aref(options, rb_id2sym(rb_intern("metrics")));
        if (!NIL_P(metrics)) {
            req.metrics = RTEST(metrics);
        }
        VALUE readonly = rb_hash_aref(options, rb_id2sym(rb_intern("readonly")));
        if (!NIL_P(readonly)) {
            req.readonly = RTEST(readonly);
        }
        VALUE scan_cap = rb_hash_aref(options, rb_id2sym(rb_intern("scan_cap")));
        if (!NIL_P(scan_cap)) {
            req.scan_cap = NUM2ULONG(scan_cap);
        }
        VALUE scan_wait = rb_hash_aref(options, rb_id2sym(rb_intern("scan_wait")));
        if (!NIL_P(scan_wait)) {
            req.scan_wait = NUM2ULONG(scan_wait);
        }
        VALUE max_parallelism = rb_hash_aref(options, rb_id2sym(rb_intern("max_parallelism")));
        if (!NIL_P(max_parallelism)) {
            req.max_parallelism = NUM2ULONG(max_parallelism);
        }
        VALUE pipeline_cap = rb_hash_aref(options, rb_id2sym(rb_intern("pipeline_cap")));
        if (!NIL_P(pipeline_cap)) {
            req.pipeline_cap = NUM2ULONG(pipeline_cap);
        }
        VALUE pipeline_batch = rb_hash_aref(options, rb_id2sym(rb_intern("pipeline_batch")));
        if (!NIL_P(pipeline_batch)) {
            req.pipeline_batch = NUM2ULONG(pipeline_batch);
        }
        VALUE profile = rb_hash_aref(options, rb_id2sym(rb_intern("profile")));
        if (!NIL_P(profile)) {
            Check_Type(profile, T_SYMBOL);
            ID mode = rb_sym2id(profile);
            if (mode == rb_intern("phases")) {
                req.profile = couchbase::operations::query_request::profile_mode::phases;
            } else if (mode == rb_intern("timings")) {
                req.profile = couchbase::operations::query_request::profile_mode::timings;
            } else if (mode == rb_intern("off")) {
                req.profile = couchbase::operations::query_request::profile_mode::off;
            }
        }
        VALUE positional_params = rb_hash_aref(options, rb_id2sym(rb_intern("positional_parameters")));
        if (!NIL_P(positional_params)) {
            Check_Type(positional_params, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(positional_params));
            req.positional_parameters.reserve(entries_num);
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
                Check_Type(entry, T_STRING);
                req.positional_parameters.emplace_back(
                  tao::json::from_string(std::string_view(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry)))));
            }
        }
        VALUE named_params = rb_hash_aref(options, rb_id2sym(rb_intern("named_parameters")));
        if (!NIL_P(named_params)) {
            Check_Type(named_params, T_HASH);
            rb_hash_foreach(named_params, INT_FUNC(cb__for_each_named_param), reinterpret_cast<VALUE>(&req));
        }
        VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
        if (!NIL_P(scan_consistency)) {
            Check_Type(scan_consistency, T_SYMBOL);
            ID type = rb_sym2id(scan_consistency);
            if (type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::operations::query_request::scan_consistency_type::not_bounded;
            } else if (type == rb_intern("request_plus")) {
                req.scan_consistency = couchbase::operations::query_request::scan_consistency_type::request_plus;
            }
        }
        VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state")));
        if (!NIL_P(mutation_state)) {
            Check_Type(mutation_state, T_ARRAY);
            auto state_size = static_cast<size_t>(RARRAY_LEN(mutation_state));
            req.mutation_state.reserve(state_size);
            for (size_t i = 0; i < state_size; ++i) {
                VALUE token = rb_ary_entry(mutation_state, static_cast<long>(i));
                Check_Type(token, T_HASH);
                VALUE bucket_name = rb_hash_aref(token, rb_id2sym(rb_intern("bucket_name")));
                Check_Type(bucket_name, T_STRING);
                VALUE partition_id = rb_hash_aref(token, rb_id2sym(rb_intern("partition_id")));
                Check_Type(partition_id, T_FIXNUM);
                VALUE partition_uuid = rb_hash_aref(token, rb_id2sym(rb_intern("partition_uuid")));
                switch (TYPE(partition_uuid)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        break;
                    default:
                        rb_raise(rb_eArgError, "partition_uuid must be an Integer");
                }
                VALUE sequence_number = rb_hash_aref(token, rb_id2sym(rb_intern("sequence_number")));
                switch (TYPE(sequence_number)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        break;
                    default:
                        rb_raise(rb_eArgError, "sequence_number must be an Integer");
                }
                req.mutation_state.emplace_back(
                  couchbase::mutation_token{ NUM2ULL(partition_uuid),
                                             NUM2ULL(sequence_number),
                                             gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                             std::string(RSTRING_PTR(bucket_name), static_cast<std::size_t>(RSTRING_LEN(bucket_name))) });
            }
        }

        VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters")));
        if (!NIL_P(raw_params)) {
            Check_Type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb__for_each_named_param), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::query_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req, [barrier](couchbase::operations::query_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.payload.meta_data.errors && !resp.payload.meta_data.errors->empty()) {
                const auto& first_error = resp.payload.meta_data.errors->front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to query: \"{}{}\" ({}: {})",
                                                     req.statement.substr(0, 50),
                                                     req.statement.size() > 50 ? "..." : "",
                                                     first_error.code,
                                                     first_error.message));
            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to query: \"{}{}\"", req.statement.substr(0, 50), req.statement.size() > 50 ? "..." : ""));
            }
            break;
        }
        VALUE res = rb_hash_new();
        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.payload.rows.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
        for (auto& row : resp.payload.rows) {
            rb_ary_push(rows, rb_str_new(row.data(), static_cast<long>(row.size())));
        }
        VALUE meta = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
        rb_hash_aset(meta,
                     rb_id2sym(rb_intern("status")),
                     rb_id2sym(rb_intern2(resp.payload.meta_data.status.data(), static_cast<long>(resp.payload.meta_data.status.size()))));
        rb_hash_aset(meta,
                     rb_id2sym(rb_intern("request_id")),
                     rb_str_new(resp.payload.meta_data.request_id.data(), static_cast<long>(resp.payload.meta_data.request_id.size())));
        rb_hash_aset(
          meta,
          rb_id2sym(rb_intern("client_context_id")),
          rb_str_new(resp.payload.meta_data.client_context_id.data(), static_cast<long>(resp.payload.meta_data.client_context_id.size())));
        if (resp.payload.meta_data.signature) {
            rb_hash_aset(meta,
                         rb_id2sym(rb_intern("signature")),
                         rb_str_new(resp.payload.meta_data.signature->data(), static_cast<long>(resp.payload.meta_data.signature->size())));
        }
        if (resp.payload.meta_data.profile) {
            rb_hash_aset(meta,
                         rb_id2sym(rb_intern("profile")),
                         rb_str_new(resp.payload.meta_data.profile->data(), static_cast<long>(resp.payload.meta_data.profile->size())));
        }
        metrics = rb_hash_new();
        rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("elapsed_time")),
                     rb_str_new(resp.payload.meta_data.metrics.elapsed_time.data(),
                                static_cast<long>(resp.payload.meta_data.metrics.elapsed_time.size())));
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("execution_time")),
                     rb_str_new(resp.payload.meta_data.metrics.execution_time.data(),
                                static_cast<long>(resp.payload.meta_data.metrics.execution_time.size())));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.payload.meta_data.metrics.result_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.payload.meta_data.metrics.result_count));
        if (resp.payload.meta_data.metrics.sort_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("sort_count")), ULL2NUM(*resp.payload.meta_data.metrics.sort_count));
        }
        if (resp.payload.meta_data.metrics.mutation_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("mutation_count")), ULL2NUM(*resp.payload.meta_data.metrics.mutation_count));
        }
        if (resp.payload.meta_data.metrics.error_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("error_count")), ULL2NUM(*resp.payload.meta_data.metrics.error_count));
        }
        if (resp.payload.meta_data.metrics.warning_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("warning_count")), ULL2NUM(*resp.payload.meta_data.metrics.warning_count));
        }

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static void
cb__generate_bucket_settings(VALUE bucket, couchbase::operations::bucket_settings& entry, bool is_create)
{
    {
        VALUE bucket_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("bucket_type")));
        Check_Type(bucket_type, T_SYMBOL);
        if (bucket_type == rb_id2sym(rb_intern("couchbase")) || bucket_type == rb_id2sym(rb_intern("membase"))) {
            entry.bucket_type = couchbase::operations::bucket_settings::bucket_type::couchbase;
        } else if (bucket_type == rb_id2sym(rb_intern("memcached"))) {
            entry.bucket_type = couchbase::operations::bucket_settings::bucket_type::memcached;
        } else if (bucket_type == rb_id2sym(rb_intern("ephemeral"))) {
            entry.bucket_type = couchbase::operations::bucket_settings::bucket_type::ephemeral;
        } else {
            rb_raise(rb_eArgError, "unknown bucket type");
        }
    }
    {
        VALUE name = rb_hash_aref(bucket, rb_id2sym(rb_intern("name")));
        Check_Type(name, T_STRING);
        entry.name.assign(RSTRING_PTR(name), static_cast<size_t>(RSTRING_LEN(name)));
    }
    {
        VALUE quota = rb_hash_aref(bucket, rb_id2sym(rb_intern("ram_quota_mb")));
        Check_Type(quota, T_FIXNUM);
        entry.ram_quota_mb = FIX2ULONG(quota);
    }
    {
        VALUE expiry = rb_hash_aref(bucket, rb_id2sym(rb_intern("max_expiry")));
        if (!NIL_P(expiry)) {
            Check_Type(expiry, T_FIXNUM);
            entry.max_expiry = FIX2UINT(expiry);
        }
    }
    {
        VALUE num_replicas = rb_hash_aref(bucket, rb_id2sym(rb_intern("num_replicas")));
        if (!NIL_P(num_replicas)) {
            Check_Type(num_replicas, T_FIXNUM);
            entry.num_replicas = FIX2UINT(num_replicas);
        }
    }
    {
        VALUE replica_indexes = rb_hash_aref(bucket, rb_id2sym(rb_intern("replica_indexes")));
        if (!NIL_P(replica_indexes)) {
            entry.replica_indexes = RTEST(replica_indexes);
        }
    }
    {
        VALUE flush_enabled = rb_hash_aref(bucket, rb_id2sym(rb_intern("flush_enabled")));
        if (!NIL_P(flush_enabled)) {
            entry.flush_enabled = RTEST(flush_enabled);
        }
    }
    {
        VALUE compression_mode = rb_hash_aref(bucket, rb_id2sym(rb_intern("compression_mode")));
        if (!NIL_P(compression_mode)) {
            Check_Type(compression_mode, T_SYMBOL);
            if (compression_mode == rb_id2sym(rb_intern("active"))) {
                entry.compression_mode = couchbase::operations::bucket_settings::compression_mode::active;
            } else if (compression_mode == rb_id2sym(rb_intern("passive"))) {
                entry.compression_mode = couchbase::operations::bucket_settings::compression_mode::passive;
            } else if (compression_mode == rb_id2sym(rb_intern("off"))) {
                entry.compression_mode = couchbase::operations::bucket_settings::compression_mode::off;
            } else {
                rb_raise(rb_eArgError, "unknown compression mode");
            }
        }
    }
    {
        VALUE ejection_policy = rb_hash_aref(bucket, rb_id2sym(rb_intern("ejection_policy")));
        if (!NIL_P(ejection_policy)) {
            Check_Type(ejection_policy, T_SYMBOL);
            if (ejection_policy == rb_id2sym(rb_intern("full"))) {
                entry.ejection_policy = couchbase::operations::bucket_settings::ejection_policy::full;
            } else if (ejection_policy == rb_id2sym(rb_intern("value_only"))) {
                entry.ejection_policy = couchbase::operations::bucket_settings::ejection_policy::value_only;
            } else {
                rb_raise(rb_eArgError, "unknown ejection policy");
            }
        }
    }
    if (is_create) {
        VALUE conflict_resolution_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("conflict_resolution_type")));
        if (!NIL_P(conflict_resolution_type)) {
            Check_Type(conflict_resolution_type, T_SYMBOL);
            if (conflict_resolution_type == rb_id2sym(rb_intern("timestamp"))) {
                entry.conflict_resolution_type = couchbase::operations::bucket_settings::conflict_resolution_type::timestamp;
            } else if (conflict_resolution_type == rb_id2sym(rb_intern("sequence_number"))) {
                entry.conflict_resolution_type = couchbase::operations::bucket_settings::conflict_resolution_type::sequence_number;
            } else {
                rb_raise(rb_eArgError, "unknown conflict resolution type");
            }
        }
    }
}

static VALUE
cb_Backend_bucket_create(VALUE self, VALUE bucket_settings, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_settings, T_HASH);

    VALUE exc = Qnil;
    do {
        couchbase::operations::bucket_create_request req{};
        cb__extract_timeout(req, timeout);
        cb__generate_bucket_settings(bucket_settings, req.bucket, true);
        auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::bucket_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec, fmt::format("unable to create bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
            break;
        }

        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_bucket_update(VALUE self, VALUE bucket_settings, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_settings, T_HASH);
    VALUE exc = Qnil;
    do {
        couchbase::operations::bucket_update_request req{};
        cb__extract_timeout(req, timeout);
        cb__generate_bucket_settings(bucket_settings, req.bucket, false);
        auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_update_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::bucket_update_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec, fmt::format("unable to update bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_bucket_drop(VALUE self, VALUE bucket_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::bucket_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::bucket_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_bucket_flush(VALUE self, VALUE bucket_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::bucket_flush_request req{};
        cb__extract_timeout(req, timeout);
        req.name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_flush_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::bucket_flush_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
            break;
        }

        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static void
cb__extract_bucket_settings(const couchbase::operations::bucket_settings& entry, VALUE bucket)
{
    switch (entry.bucket_type) {
        case couchbase::operations::bucket_settings::bucket_type::couchbase:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("couchbase")));
            break;
        case couchbase::operations::bucket_settings::bucket_type::memcached:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("memcached")));
            break;
        case couchbase::operations::bucket_settings::bucket_type::ephemeral:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("ephemeral")));
            break;
        case couchbase::operations::bucket_settings::bucket_type::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), Qnil);
            break;
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("name")), rb_str_new(entry.name.data(), static_cast<long>(entry.name.size())));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("uuid")), rb_str_new(entry.uuid.data(), static_cast<long>(entry.uuid.size())));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("ram_quota_mb")), ULL2NUM(entry.ram_quota_mb));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("max_expiry")), ULONG2NUM(entry.max_expiry));
    switch (entry.compression_mode) {
        case couchbase::operations::bucket_settings::compression_mode::off:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("off")));
            break;
        case couchbase::operations::bucket_settings::compression_mode::active:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("active")));
            break;
        case couchbase::operations::bucket_settings::compression_mode::passive:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("passive")));
            break;
        case couchbase::operations::bucket_settings::compression_mode::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), Qnil);
            break;
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("num_replicas")), ULONG2NUM(entry.num_replicas));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("replica_indexes")), entry.replica_indexes ? Qtrue : Qfalse);
    rb_hash_aset(bucket, rb_id2sym(rb_intern("flush_enabled")), entry.flush_enabled ? Qtrue : Qfalse);
    switch (entry.ejection_policy) {
        case couchbase::operations::bucket_settings::ejection_policy::full:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("ejection_policy")), rb_id2sym(rb_intern("full")));
            break;
        case couchbase::operations::bucket_settings::ejection_policy::value_only:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("ejection_policy")), rb_id2sym(rb_intern("value_only")));
            break;
        case couchbase::operations::bucket_settings::ejection_policy::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("ejection_policy")), Qnil);
            break;
    }
    switch (entry.conflict_resolution_type) {
        case couchbase::operations::bucket_settings::conflict_resolution_type::timestamp:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("timestamp")));
            break;
        case couchbase::operations::bucket_settings::conflict_resolution_type::sequence_number:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("sequence_number")));
            break;
        case couchbase::operations::bucket_settings::conflict_resolution_type::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), Qnil);
            break;
    }
    VALUE capabilities = rb_ary_new_capa(static_cast<long>(entry.capabilities.size()));
    for (const auto& capa : entry.capabilities) {
        rb_ary_push(capabilities, rb_str_new(capa.data(), static_cast<long>(capa.size())));
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("capabilities")), capabilities);
    VALUE nodes = rb_ary_new_capa(static_cast<long>(entry.nodes.size()));
    for (const auto& n : entry.nodes) {
        VALUE node = rb_hash_new();
        rb_hash_aset(node, rb_id2sym(rb_intern("status")), rb_str_new(n.status.data(), static_cast<long>(n.status.size())));
        rb_hash_aset(node, rb_id2sym(rb_intern("hostname")), rb_str_new(n.hostname.data(), static_cast<long>(n.hostname.size())));
        rb_hash_aset(node, rb_id2sym(rb_intern("version")), rb_str_new(n.version.data(), static_cast<long>(n.version.size())));
        rb_ary_push(nodes, node);
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("nodes")), nodes);
}

static VALUE
cb_Backend_bucket_get_all(VALUE self, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::bucket_get_all_request req{};
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::bucket_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, "unable to get list of the buckets of the cluster");
            break;
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.buckets.size()));
        for (const auto& entry : resp.buckets) {
            VALUE bucket = rb_hash_new();
            cb__extract_bucket_settings(entry, bucket);
            rb_ary_push(res, bucket);
        }

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_bucket_get(VALUE self, VALUE bucket_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::bucket_get_request req{};
        cb__extract_timeout(req, timeout);
        req.name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_get_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::bucket_get_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to locate bucket \"{}\" on the cluster", req.name));
            break;
        }

        VALUE res = rb_hash_new();
        cb__extract_bucket_settings(resp.bucket, res);

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_cluster_enable_developer_preview(VALUE self)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::cluster_developer_preview_enable_request req{};
        auto barrier = std::make_shared<std::promise<couchbase::operations::cluster_developer_preview_enable_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::cluster_developer_preview_enable_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to enable developer preview for this cluster"));
            break;
        }
        spdlog::critical(
          "Developer preview cannot be disabled once it is enabled. If you enter developer preview mode you will not be able to "
          "upgrade. DO NOT USE IN PRODUCTION.");
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_scope_get_all(VALUE self, VALUE bucket_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::scope_get_all_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::scope_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::scope_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to get list of the scopes of the bucket \"{}\"", req.bucket_name));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("uid")), ULL2NUM(resp.manifest.uid));
        VALUE scopes = rb_ary_new_capa(static_cast<long>(resp.manifest.scopes.size()));
        for (const auto& s : resp.manifest.scopes) {
            VALUE scope = rb_hash_new();
            rb_hash_aset(scope, rb_id2sym(rb_intern("uid")), ULL2NUM(s.uid));
            rb_hash_aset(scope, rb_id2sym(rb_intern("name")), rb_str_new(s.name.data(), static_cast<long>(s.name.size())));
            VALUE collections = rb_ary_new_capa(static_cast<long>(s.collections.size()));
            for (const auto& c : s.collections) {
                VALUE collection = rb_hash_new();
                rb_hash_aset(collection, rb_id2sym(rb_intern("uid")), ULL2NUM(c.uid));
                rb_hash_aset(collection, rb_id2sym(rb_intern("name")), rb_str_new(c.name.data(), static_cast<long>(c.name.size())));
                rb_ary_push(collections, collection);
            }
            rb_hash_aset(scope, rb_id2sym(rb_intern("collections")), collections);
            rb_ary_push(scopes, scope);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("scopes")), scopes);

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_scope_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::scope_create_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::scope_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::scope_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to create the scope on the bucket \"{}\"", req.bucket_name));
            break;
        }
        return ULL2NUM(resp.uid);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_scope_drop(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::scope_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::scope_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::scope_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec,
                                     fmt::format("unable to drop the scope \"{}\" on the bucket \"{}\"", req.scope_name, req.bucket_name));
            break;
        }
        return ULL2NUM(resp.uid);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_collection_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE max_expiry, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::collection_create_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
        req.collection_name.assign(RSTRING_PTR(collection_name), static_cast<size_t>(RSTRING_LEN(collection_name)));

        if (!NIL_P(max_expiry)) {
            Check_Type(max_expiry, T_FIXNUM);
            req.max_expiry = FIX2UINT(max_expiry);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::collection_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::collection_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec,
              fmt::format(
                R"(unable create the collection "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
            break;
        }
        return ULL2NUM(resp.uid);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_collection_drop(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::collection_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
        req.collection_name.assign(RSTRING_PTR(collection_name), static_cast<size_t>(RSTRING_LEN(collection_name)));

        auto barrier = std::make_shared<std::promise<couchbase::operations::collection_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::collection_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec,
              fmt::format(
                R"(unable to drop the collection  "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
            break;
        }
        return ULL2NUM(resp.uid);
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_get_all(VALUE self, VALUE bucket_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_index_get_all_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::query_index_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to get list of the indexes of the bucket \"{}\"", req.bucket_name));
            break;
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            rb_hash_aset(index, rb_id2sym(rb_intern("id")), rb_str_new(idx.id.data(), static_cast<long>(idx.id.size())));
            rb_hash_aset(index, rb_id2sym(rb_intern("state")), rb_str_new(idx.state.data(), static_cast<long>(idx.state.size())));
            rb_hash_aset(index, rb_id2sym(rb_intern("name")), rb_str_new(idx.name.data(), static_cast<long>(idx.name.size())));
            rb_hash_aset(
              index, rb_id2sym(rb_intern("datastore_id")), rb_str_new(idx.datastore_id.data(), static_cast<long>(idx.datastore_id.size())));
            rb_hash_aset(
              index, rb_id2sym(rb_intern("keyspace_id")), rb_str_new(idx.keyspace_id.data(), static_cast<long>(idx.keyspace_id.size())));
            rb_hash_aset(
              index, rb_id2sym(rb_intern("namespace_id")), rb_str_new(idx.namespace_id.data(), static_cast<long>(idx.namespace_id.size())));
            rb_hash_aset(index, rb_id2sym(rb_intern("type")), rb_str_new(idx.type.data(), static_cast<long>(idx.type.size())));
            rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
            VALUE index_key = rb_ary_new_capa(static_cast<long>(idx.index_key.size()));
            for (const auto& key : idx.index_key) {
                rb_ary_push(index_key, rb_str_new(key.data(), static_cast<long>(key.size())));
            }
            rb_hash_aset(index, rb_id2sym(rb_intern("index_key")), index_key);
            if (idx.condition) {
                rb_hash_aset(
                  index, rb_id2sym(rb_intern("condition")), rb_str_new(idx.condition->data(), static_cast<long>(idx.condition->size())));
            }
            rb_ary_push(indexes, index);
        }

        rb_hash_aset(res, rb_id2sym(rb_intern("indexes")), indexes);

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_create(VALUE self, VALUE bucket_name, VALUE index_name, VALUE fields, VALUE options, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_name, T_STRING);
    Check_Type(fields, T_ARRAY);

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_index_create_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        auto fields_num = static_cast<size_t>(RARRAY_LEN(fields));
        req.fields.reserve(fields_num);
        for (size_t i = 0; i < fields_num; ++i) {
            VALUE entry = rb_ary_entry(fields, static_cast<long>(i));
            Check_Type(entry, T_STRING);
            req.fields.emplace_back(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry)));
        }
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists")));
            if (ignore_if_exists == Qtrue) {
                req.ignore_if_exists = true;
            } else if (ignore_if_exists == Qfalse) {
                req.ignore_if_exists = false;
            } /* else use backend default */
            VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred")));
            if (deferred == Qtrue) {
                req.deferred = true;
            } else if (deferred == Qfalse) {
                req.deferred = false;
            } /* else use backend default */
            VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas")));
            if (!NIL_P(num_replicas)) {
                req.num_replicas = NUM2UINT(num_replicas);
            } /* else use backend default */
            VALUE condition = rb_hash_aref(options, rb_id2sym(rb_intern("condition")));
            if (!NIL_P(condition)) {
                req.condition.emplace(std::string(RSTRING_PTR(condition), static_cast<std::size_t>(RSTRING_LEN(condition))));
            } /* else use backend default */
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::query_index_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format(R"(unable to create index "{}" on the bucket "{}" ({}: {}))",
                                                     req.index_name,
                                                     req.bucket_name,
                                                     first_error.code,
                                                     first_error.message));
            } else {
                exc = cb__map_error_code(resp.ec,
                                         fmt::format(R"(unable to create index "{}" on the bucket "{}")", req.index_name, req.bucket_name));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), rb_str_new(err.message.data(), static_cast<long>(err.message.size())));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_drop(VALUE self, VALUE bucket_name, VALUE index_name, VALUE options, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_index_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE ignore_if_does_not_exist = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
            if (ignore_if_does_not_exist == Qtrue) {
                req.ignore_if_does_not_exist = true;
            } else if (ignore_if_does_not_exist == Qfalse) {
                req.ignore_if_does_not_exist = false;
            } /* else use backend default */
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::query_index_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format(R"(unable to drop index "{}" on the bucket "{}" ({}: {}))",
                                                     req.index_name,
                                                     req.bucket_name,
                                                     first_error.code,
                                                     first_error.message));
            } else {
                exc = cb__map_error_code(resp.ec,
                                         fmt::format(R"(unable to drop index "{}" on the bucket "{}")", req.index_name, req.bucket_name));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), rb_str_new(err.message.data(), static_cast<long>(err.message.size())));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_create_primary(VALUE self, VALUE bucket_name, VALUE options, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_index_create_request req{};
        cb__extract_timeout(req, timeout);
        req.is_primary = true;
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists")));
            if (ignore_if_exists == Qtrue) {
                req.ignore_if_exists = true;
            } else if (ignore_if_exists == Qfalse) {
                req.ignore_if_exists = false;
            } /* else use backend default */
            VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred")));
            if (deferred == Qtrue) {
                req.deferred = true;
            } else if (deferred == Qfalse) {
                req.deferred = false;
            } /* else use backend default */
            VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas")));
            if (!NIL_P(num_replicas)) {
                req.num_replicas = NUM2UINT(num_replicas);
            } /* else use backend default */
            VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name")));
            if (!NIL_P(index_name)) {
                req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
            } /* else use backend default */
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::query_index_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format(R"(unable to create primary index on the bucket "{}" ({}: {}))",
                                                     req.bucket_name,
                                                     first_error.code,
                                                     first_error.message));
            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format(R"(unable to create primary index on the bucket "{}")", req.index_name, req.bucket_name));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), rb_str_new(err.message.data(), static_cast<long>(err.message.size())));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_drop_primary(VALUE self, VALUE bucket_name, VALUE options, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_index_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.is_primary = true;
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        if (!NIL_P(options)) {
            Check_Type(options, T_HASH);
            VALUE ignore_if_does_not_exist = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
            if (ignore_if_does_not_exist == Qtrue) {
                req.ignore_if_does_not_exist = true;
            } else if (ignore_if_does_not_exist == Qfalse) {
                req.ignore_if_does_not_exist = false;
            } /* else use backend default */
            VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name")));
            if (!NIL_P(index_name)) {
                Check_Type(options, T_STRING);
                req.is_primary = false;
                req.bucket_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::query_index_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format(
                    R"(unable to drop primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));
            } else {
                exc = cb__map_error_code(resp.ec, fmt::format(R"(unable to drop primary index on the bucket "{}")", req.bucket_name));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), rb_str_new(err.message.data(), static_cast<long>(err.message.size())));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_build_deferred(VALUE self, VALUE bucket_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::query_index_build_deferred_request req{};
        cb__extract_timeout(req, timeout);
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_build_deferred_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::query_index_build_deferred_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format(
                    R"(unable to drop primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));

            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to trigger build for deferred indexes for the bucket \"{}\"", req.bucket_name));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_query_index_watch(VALUE self, VALUE bucket_name, VALUE index_names, VALUE timeout, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_names, T_ARRAY);
    Check_Type(timeout, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    return Qtrue;
}

static void
cb__extract_search_index(VALUE index, const couchbase::operations::search_index& idx)
{
    rb_hash_aset(index, rb_id2sym(rb_intern("uuid")), rb_str_new(idx.uuid.data(), static_cast<long>(idx.uuid.size())));
    rb_hash_aset(index, rb_id2sym(rb_intern("name")), rb_str_new(idx.name.data(), static_cast<long>(idx.name.size())));
    rb_hash_aset(index, rb_id2sym(rb_intern("type")), rb_str_new(idx.type.data(), static_cast<long>(idx.type.size())));
    if (!idx.params_json.empty()) {
        rb_hash_aset(index, rb_id2sym(rb_intern("params")), rb_str_new(idx.params_json.data(), static_cast<long>(idx.params_json.size())));
    }

    if (!idx.source_uuid.empty()) {
        rb_hash_aset(
          index, rb_id2sym(rb_intern("source_uuid")), rb_str_new(idx.source_uuid.data(), static_cast<long>(idx.source_uuid.size())));
    }
    if (!idx.source_name.empty()) {
        rb_hash_aset(
          index, rb_id2sym(rb_intern("source_name")), rb_str_new(idx.source_name.data(), static_cast<long>(idx.source_name.size())));
    }
    rb_hash_aset(index, rb_id2sym(rb_intern("source_type")), rb_str_new(idx.source_type.data(), static_cast<long>(idx.source_type.size())));
    if (!idx.source_params_json.empty()) {
        rb_hash_aset(index,
                     rb_id2sym(rb_intern("source_params")),
                     rb_str_new(idx.source_params_json.data(), static_cast<long>(idx.source_params_json.size())));
    }
    if (!idx.plan_params_json.empty()) {
        rb_hash_aset(index,
                     rb_id2sym(rb_intern("plan_params")),
                     rb_str_new(idx.plan_params_json.data(), static_cast<long>(idx.plan_params_json.size())));
    }
}

static VALUE
cb_Backend_search_index_get_all(VALUE self, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_get_all_request req{};
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, "unable to get list of the search indexes");
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        rb_hash_aset(
          res, rb_id2sym(rb_intern("impl_version")), rb_str_new(resp.impl_version.data(), static_cast<long>(resp.impl_version.size())));
        VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            cb__extract_search_index(index, idx);
            rb_ary_push(indexes, index);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("indexes")), indexes);
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_get(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_get_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_get_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_get_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to get search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to get search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        cb__extract_search_index(res, resp.index);
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_upsert(VALUE self, VALUE index_definition, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_definition, T_HASH);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_upsert_request req{};
        cb__extract_timeout(req, timeout);

        VALUE index_name = rb_hash_aref(index_definition, rb_id2sym(rb_intern("name")));
        Check_Type(index_name, T_STRING);
        req.index.name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));

        VALUE index_type = rb_hash_aref(index_definition, rb_id2sym(rb_intern("type")));
        Check_Type(index_type, T_STRING);
        req.index.type.assign(RSTRING_PTR(index_type), static_cast<size_t>(RSTRING_LEN(index_type)));

        VALUE index_uuid = rb_hash_aref(index_definition, rb_id2sym(rb_intern("uuid")));
        if (!NIL_P(index_uuid)) {
            Check_Type(index_uuid, T_STRING);
            req.index.uuid.assign(RSTRING_PTR(index_uuid), static_cast<size_t>(RSTRING_LEN(index_uuid)));
        }

        VALUE index_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("params")));
        if (!NIL_P(index_params)) {
            Check_Type(index_params, T_STRING);
            req.index.params_json.assign(std::string(RSTRING_PTR(index_params), static_cast<size_t>(RSTRING_LEN(index_params))));
        }

        VALUE source_name = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_name")));
        if (!NIL_P(source_name)) {
            Check_Type(source_name, T_STRING);
            req.index.source_name.assign(RSTRING_PTR(source_name), static_cast<size_t>(RSTRING_LEN(source_name)));
        }

        VALUE source_type = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_type")));
        Check_Type(source_type, T_STRING);
        req.index.source_type.assign(RSTRING_PTR(source_type), static_cast<size_t>(RSTRING_LEN(source_type)));

        VALUE source_uuid = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_uuid")));
        if (!NIL_P(source_uuid)) {
            Check_Type(source_uuid, T_STRING);
            req.index.source_uuid.assign(RSTRING_PTR(source_uuid), static_cast<size_t>(RSTRING_LEN(source_uuid)));
        }

        VALUE source_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_params")));
        if (!NIL_P(source_params)) {
            Check_Type(source_params, T_STRING);
            req.index.source_params_json.assign(std::string(RSTRING_PTR(source_params), static_cast<size_t>(RSTRING_LEN(source_params))));
        }

        VALUE plan_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("plan_params")));
        if (!NIL_P(plan_params)) {
            Check_Type(plan_params, T_STRING);
            req.index.plan_params_json.assign(std::string(RSTRING_PTR(plan_params), static_cast<size_t>(RSTRING_LEN(plan_params))));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_upsert_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_upsert_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to upsert the search index \"{}\"", req.index.name));
            } else {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to upsert the search index \"{}\": {}", req.index.name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_drop(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to drop the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to drop the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_get_documents_count(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_get_documents_count_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_get_documents_count_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_get_documents_count_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to get number of the indexed documents for the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format("unable to get number of the indexed documents for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("count")), ULL2NUM(resp.count));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_pause_ingest(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_control_ingest_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.pause = true;
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_control_ingest_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_control_ingest_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to pause ingest for the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to pause ingest for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_resume_ingest(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_control_ingest_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.pause = false;
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_control_ingest_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_control_ingest_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to resume ingest for the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to resume ingest for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_allow_querying(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_control_query_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.allow = true;
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_control_query_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_control_query_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to allow querying for the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to allow querying for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_disallow_querying(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_control_query_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.allow = false;
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_control_query_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_control_query_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to disallow querying for the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to disallow querying for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_freeze_plan(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_control_plan_freeze_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.freeze = true;
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_control_plan_freeze_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_control_plan_freeze_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to freeze for the search index \"{}\"", req.index_name));
            } else {
                exc =
                  cb__map_error_code(resp.ec, fmt::format("unable to freeze for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_unfreeze_plan(VALUE self, VALUE index_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_control_plan_freeze_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.freeze = false;
        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_control_plan_freeze_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_control_plan_freeze_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to unfreeze plan for the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to unfreeze for the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_search_index_analyze_document(VALUE self, VALUE index_name, VALUE encoded_document, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    Check_Type(encoded_document, T_STRING);
    VALUE exc = Qnil;
    do {
        couchbase::operations::search_index_analyze_document_request req{};
        cb__extract_timeout(req, timeout);

        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.encoded_document.assign(RSTRING_PTR(encoded_document), static_cast<size_t>(RSTRING_LEN(encoded_document)));

        auto barrier = std::make_shared<std::promise<couchbase::operations::search_index_analyze_document_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::search_index_analyze_document_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to analyze document using the search index \"{}\"", req.index_name));
            } else {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to analyze document using the search index \"{}\": {}", req.index_name, resp.error));
            }
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), rb_str_new(resp.status.data(), static_cast<long>(resp.status.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("analysis")), rb_str_new(resp.analysis.data(), static_cast<long>(resp.analysis.size())));
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_search(VALUE self, VALUE index_name, VALUE query, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    Check_Type(query, T_STRING);
    Check_Type(options, T_HASH);

    VALUE exc = Qnil;
    do {
        couchbase::operations::search_request req;
        VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
        if (!NIL_P(client_context_id)) {
            Check_Type(client_context_id, T_STRING);
            req.client_context_id.assign(RSTRING_PTR(client_context_id), static_cast<size_t>(RSTRING_LEN(client_context_id)));
        }
        cb__extract_timeout(req, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.query = tao::json::from_string(std::string(RSTRING_PTR(query), static_cast<size_t>(RSTRING_LEN(query))));

        VALUE explain = rb_hash_aref(options, rb_id2sym(rb_intern("explain")));
        if (!NIL_P(explain)) {
            req.explain = RTEST(explain);
        }

        VALUE skip = rb_hash_aref(options, rb_id2sym(rb_intern("skip")));
        if (!NIL_P(skip)) {
            Check_Type(skip, T_FIXNUM);
            req.skip = FIX2ULONG(skip);
        }

        VALUE limit = rb_hash_aref(options, rb_id2sym(rb_intern("limit")));
        if (!NIL_P(limit)) {
            Check_Type(limit, T_FIXNUM);
            req.limit = FIX2ULONG(limit);
        }

        VALUE highlight_style = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_style")));
        if (!NIL_P(highlight_style)) {
            Check_Type(highlight_style, T_SYMBOL);
            ID type = rb_sym2id(highlight_style);
            if (type == rb_intern("html")) {
                req.highlight_style = couchbase::operations::search_request::highlight_style_type::html;
            } else if (type == rb_intern("ansi")) {
                req.highlight_style = couchbase::operations::search_request::highlight_style_type::ansi;
            }
        }

        VALUE highlight_fields = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_fields")));
        if (!NIL_P(highlight_fields)) {
            Check_Type(highlight_fields, T_ARRAY);
            auto highlight_fields_size = static_cast<size_t>(RARRAY_LEN(highlight_fields));
            req.highlight_fields.reserve(highlight_fields_size);
            for (size_t i = 0; i < highlight_fields_size; ++i) {
                VALUE field = rb_ary_entry(highlight_fields, static_cast<long>(i));
                Check_Type(field, T_STRING);
                req.highlight_fields.emplace_back(std::string(RSTRING_PTR(field), static_cast<std::size_t>(RSTRING_LEN(field))));
            }
        }

        VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
        if (!NIL_P(scan_consistency)) {
            Check_Type(scan_consistency, T_SYMBOL);
            ID type = rb_sym2id(scan_consistency);
            if (type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::operations::search_request::scan_consistency_type::not_bounded;
            }
        }

        VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state")));
        if (!NIL_P(mutation_state)) {
            Check_Type(mutation_state, T_ARRAY);
            auto state_size = static_cast<size_t>(RARRAY_LEN(mutation_state));
            req.mutation_state.reserve(state_size);
            for (size_t i = 0; i < state_size; ++i) {
                VALUE token = rb_ary_entry(mutation_state, static_cast<long>(i));
                Check_Type(token, T_HASH);
                VALUE bucket_name = rb_hash_aref(token, rb_id2sym(rb_intern("bucket_name")));
                Check_Type(bucket_name, T_STRING);
                VALUE partition_id = rb_hash_aref(token, rb_id2sym(rb_intern("partition_id")));
                Check_Type(partition_id, T_FIXNUM);
                VALUE partition_uuid = rb_hash_aref(token, rb_id2sym(rb_intern("partition_uuid")));
                switch (TYPE(partition_uuid)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        break;
                    default:
                        rb_raise(rb_eArgError, "partition_uuid must be an Integer");
                }
                VALUE sequence_number = rb_hash_aref(token, rb_id2sym(rb_intern("sequence_number")));
                switch (TYPE(sequence_number)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        break;
                    default:
                        rb_raise(rb_eArgError, "sequence_number must be an Integer");
                }
                req.mutation_state.emplace_back(
                  couchbase::mutation_token{ NUM2ULL(partition_uuid),
                                             NUM2ULL(sequence_number),
                                             gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                             std::string(RSTRING_PTR(bucket_name), static_cast<std::size_t>(RSTRING_LEN(bucket_name))) });
            }
        }

        VALUE fields = rb_hash_aref(options, rb_id2sym(rb_intern("fields")));
        if (!NIL_P(fields)) {
            Check_Type(fields, T_ARRAY);
            auto fields_size = static_cast<size_t>(RARRAY_LEN(fields));
            req.fields.reserve(fields_size);
            for (size_t i = 0; i < fields_size; ++i) {
                VALUE field = rb_ary_entry(fields, static_cast<long>(i));
                Check_Type(field, T_STRING);
                req.fields.emplace_back(std::string(RSTRING_PTR(field), static_cast<std::size_t>(RSTRING_LEN(field))));
            }
        }

        VALUE sort = rb_hash_aref(options, rb_id2sym(rb_intern("sort")));
        if (!NIL_P(sort)) {
            Check_Type(sort, T_ARRAY);
            for (size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(sort)); ++i) {
                VALUE sort_spec = rb_ary_entry(sort, static_cast<long>(i));
                req.sort_specs.emplace_back(std::string(RSTRING_PTR(sort_spec), static_cast<std::size_t>(RSTRING_LEN(sort_spec))));
            }
        }

        VALUE facets = rb_hash_aref(options, rb_id2sym(rb_intern("facets")));
        if (!NIL_P(facets)) {
            Check_Type(facets, T_ARRAY);
            for (size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(facets)); ++i) {
                VALUE facet_pair = rb_ary_entry(facets, static_cast<long>(i));
                Check_Type(facet_pair, T_ARRAY);
                if (RARRAY_LEN(facet_pair) == 2) {
                    VALUE facet_name = rb_ary_entry(facet_pair, 0);
                    Check_Type(facet_name, T_STRING);
                    VALUE facet_definition = rb_ary_entry(facet_pair, 1);
                    Check_Type(facet_definition, T_STRING);
                    req.facets.emplace(std::string(RSTRING_PTR(facet_name), static_cast<std::size_t>(RSTRING_LEN(facet_name))),
                                       std::string(RSTRING_PTR(facet_definition), static_cast<std::size_t>(RSTRING_LEN(facet_definition))));
                }
            }
        }

        VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters")));
        if (!NIL_P(raw_params)) {
            Check_Type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb__for_each_named_param), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::search_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req, [barrier](couchbase::operations::search_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("unable to perform search query for index \"{}\"", req.index_name));
            break;
        }
        VALUE res = rb_hash_new();

        VALUE meta_data = rb_hash_new();
        rb_hash_aset(meta_data,
                     rb_id2sym(rb_intern("client_context_id")),
                     rb_str_new(resp.meta_data.client_context_id.data(), static_cast<long>(resp.meta_data.client_context_id.size())));

        VALUE metrics = rb_hash_new();
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("took")),
                     LONG2NUM(std::chrono::duration_cast<std::chrono::milliseconds>(resp.meta_data.metrics.took).count()));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("total_rows")), ULL2NUM(resp.meta_data.metrics.total_rows));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("max_score")), DBL2NUM(resp.meta_data.metrics.max_score));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("success_partition_count")), ULL2NUM(resp.meta_data.metrics.success_partition_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("error_partition_count")), ULL2NUM(resp.meta_data.metrics.error_partition_count));
        rb_hash_aset(meta_data, rb_id2sym(rb_intern("metrics")), metrics);

        if (!resp.meta_data.errors.empty()) {
            VALUE errors = rb_hash_new();
            for (auto err : resp.meta_data.errors) {
                rb_hash_aset(errors,
                             rb_str_new(err.first.data(), static_cast<long>(err.first.size())),
                             rb_str_new(err.second.data(), static_cast<long>(err.second.size())));
            }
            rb_hash_aset(meta_data, rb_id2sym(rb_intern("errors")), errors);
        }

        rb_hash_aset(res, rb_id2sym(rb_intern("meta_data")), meta_data);

        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
        for (const auto& entry : resp.rows) {
            VALUE row = rb_hash_new();
            rb_hash_aset(row, rb_id2sym(rb_intern("index")), rb_str_new(entry.index.data(), static_cast<long>(entry.index.size())));
            rb_hash_aset(row, rb_id2sym(rb_intern("id")), rb_str_new(entry.id.data(), static_cast<long>(entry.id.size())));
            rb_hash_aset(row, rb_id2sym(rb_intern("score")), DBL2NUM(entry.score));
            VALUE locations = rb_ary_new_capa(static_cast<long>(entry.locations.size()));
            for (const auto& loc : entry.locations) {
                VALUE location = rb_hash_new();
                rb_hash_aset(row, rb_id2sym(rb_intern("field")), rb_str_new(loc.field.data(), static_cast<long>(loc.field.size())));
                rb_hash_aset(row, rb_id2sym(rb_intern("term")), rb_str_new(loc.term.data(), static_cast<long>(loc.term.size())));
                rb_hash_aset(row, rb_id2sym(rb_intern("pos")), ULL2NUM(loc.position));
                rb_hash_aset(row, rb_id2sym(rb_intern("start_offset")), ULL2NUM(loc.start_offset));
                rb_hash_aset(row, rb_id2sym(rb_intern("end_offset")), ULL2NUM(loc.end_offset));
                if (loc.array_positions) {
                    VALUE ap = rb_ary_new_capa(static_cast<long>(loc.array_positions->size()));
                    for (const auto& pos : *loc.array_positions) {
                        rb_ary_push(ap, ULL2NUM(pos));
                    }
                    rb_hash_aset(row, rb_id2sym(rb_intern("array_positions")), ap);
                }
                rb_ary_push(locations, location);
            }
            rb_hash_aset(row, rb_id2sym(rb_intern("locations")), locations);
            if (!entry.fragments.empty()) {
                VALUE fragments = rb_hash_new();
                for (const auto& field_fragments : entry.fragments) {
                    VALUE fragments_list = rb_ary_new_capa(static_cast<long>(field_fragments.second.size()));
                    for (const auto& fragment : field_fragments.second) {
                        rb_ary_push(fragments_list, rb_str_new(fragment.data(), static_cast<long>(fragment.size())));
                    }
                    rb_hash_aset(
                      fragments, rb_str_new(field_fragments.first.data(), static_cast<long>(field_fragments.first.size())), fragments_list);
                }
                rb_hash_aset(row, rb_id2sym(rb_intern("fragments")), fragments);
            }
            if (!entry.fields.empty()) {
                rb_hash_aset(row, rb_id2sym(rb_intern("fields")), rb_str_new(entry.fields.data(), static_cast<long>(entry.fields.size())));
            }
            if (!entry.explanation.empty()) {
                rb_hash_aset(row,
                             rb_id2sym(rb_intern("explanation")),
                             rb_str_new(entry.explanation.data(), static_cast<long>(entry.explanation.size())));
            }
            rb_ary_push(rows, row);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);

        if (!resp.facets.empty()) {
            VALUE result_facets = rb_hash_new();
            for (const auto& entry : resp.facets) {
                VALUE facet = rb_hash_new();
                VALUE facet_name = rb_str_new(entry.name.data(), static_cast<long>(entry.name.size()));
                rb_hash_aset(facet, rb_id2sym(rb_intern("name")), facet_name);
                rb_hash_aset(facet, rb_id2sym(rb_intern("field")), rb_str_new(entry.field.data(), static_cast<long>(entry.field.size())));
                rb_hash_aset(facet, rb_id2sym(rb_intern("total")), ULL2NUM(entry.total));
                rb_hash_aset(facet, rb_id2sym(rb_intern("missing")), ULL2NUM(entry.missing));
                rb_hash_aset(facet, rb_id2sym(rb_intern("other")), ULL2NUM(entry.other));
                if (!entry.terms.empty()) {
                    VALUE terms = rb_ary_new_capa(static_cast<long>(entry.terms.size()));
                    for (const auto& item : entry.terms) {
                        VALUE term = rb_hash_new();
                        rb_hash_aset(term, rb_id2sym(rb_intern("term")), rb_str_new(item.term.data(), static_cast<long>(item.term.size())));
                        rb_hash_aset(term, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
                        rb_ary_push(terms, term);
                    }
                    rb_hash_aset(facet, rb_id2sym(rb_intern("terms")), terms);
                } else if (!entry.date_ranges.empty()) {
                    VALUE date_ranges = rb_ary_new_capa(static_cast<long>(entry.date_ranges.size()));
                    for (const auto& item : entry.date_ranges) {
                        VALUE date_range = rb_hash_new();
                        rb_hash_aset(
                          date_range, rb_id2sym(rb_intern("name")), rb_str_new(item.name.data(), static_cast<long>(item.name.size())));
                        rb_hash_aset(date_range, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
                        if (item.start) {
                            rb_hash_aset(date_range,
                                         rb_id2sym(rb_intern("start_time")),
                                         rb_str_new(item.start->data(), static_cast<long>(item.start->size())));
                        }
                        if (item.end) {
                            rb_hash_aset(date_range,
                                         rb_id2sym(rb_intern("end_time")),
                                         rb_str_new(item.end->data(), static_cast<long>(item.end->size())));
                        }
                        rb_ary_push(date_ranges, date_range);
                    }
                    rb_hash_aset(facet, rb_id2sym(rb_intern("date_ranges")), date_ranges);
                } else if (!entry.numeric_ranges.empty()) {
                    VALUE numeric_ranges = rb_ary_new_capa(static_cast<long>(entry.numeric_ranges.size()));
                    for (const auto& item : entry.numeric_ranges) {
                        VALUE numeric_range = rb_hash_new();
                        rb_hash_aset(
                          numeric_range, rb_id2sym(rb_intern("name")), rb_str_new(item.name.data(), static_cast<long>(item.name.size())));
                        rb_hash_aset(numeric_range, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
                        if (std::holds_alternative<double>(item.min)) {
                            rb_hash_aset(numeric_range, rb_id2sym(rb_intern("min")), DBL2NUM(std::get<double>(item.min)));
                        } else if (std::holds_alternative<std::uint64_t>(item.min)) {
                            rb_hash_aset(numeric_range, rb_id2sym(rb_intern("min")), ULL2NUM(std::get<std::uint64_t>(item.min)));
                        }
                        if (std::holds_alternative<double>(item.max)) {
                            rb_hash_aset(numeric_range, rb_id2sym(rb_intern("max")), DBL2NUM(std::get<double>(item.max)));
                        } else if (std::holds_alternative<std::uint64_t>(item.max)) {
                            rb_hash_aset(numeric_range, rb_id2sym(rb_intern("max")), ULL2NUM(std::get<std::uint64_t>(item.max)));
                        }
                        rb_ary_push(numeric_ranges, numeric_range);
                    }
                    rb_hash_aset(facet, rb_id2sym(rb_intern("numeric_ranges")), numeric_ranges);
                }
                rb_hash_aset(result_facets, facet_name, facet);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("facets")), result_facets);
        }

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_dns_srv(VALUE self, VALUE hostname, VALUE service)
{
    (void)self;
    Check_Type(hostname, T_STRING);
    Check_Type(service, T_SYMBOL);

    bool tls = false;

    ID type = rb_sym2id(service);
    if (type == rb_intern("couchbase")) {
        tls = false;
    } else if (type == rb_intern("couchbases")) {
        tls = true;
    } else {
        rb_raise(rb_eArgError, "Unsupported service type: %+" PRIsVALUE, service);
    }
    VALUE exc = Qnil;
    do {
        asio::io_context ctx;

        couchbase::io::dns::dns_client client(ctx);
        std::string host_name(RSTRING_PTR(hostname), static_cast<size_t>(RSTRING_LEN(hostname)));
        std::string service_name("_couchbase");
        if (tls) {
            service_name = "_couchbases";
        }
        auto barrier = std::make_shared<std::promise<couchbase::io::dns::dns_client::dns_srv_response>>();
        auto f = barrier->get_future();
        client.query_srv(
          host_name, service_name, [barrier](couchbase::io::dns::dns_client::dns_srv_response resp) mutable { barrier->set_value(resp); });
        ctx.run();
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, fmt::format("DNS SRV query failure for name \"{}\" (service: {})", host_name, service_name));
            break;
        }

        VALUE res = rb_ary_new();
        for (const auto& target : resp.targets) {
            VALUE addr = rb_hash_new();
            rb_hash_aset(
              addr, rb_id2sym(rb_intern("hostname")), rb_str_new(target.hostname.data(), static_cast<long>(target.hostname.size())));
            rb_hash_aset(addr, rb_id2sym(rb_intern("port")), UINT2NUM(target.port));
            rb_ary_push(res, addr);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_get_pending_mutations(VALUE self, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_get_pending_mutations_request req{};
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_get_pending_mutations_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_get_pending_mutations_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, "unable to get pending mutations for the analytics service");
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format("unable to get pending mutations for the analytics service ({}: {})", first_error.code, first_error.message));
            }
            break;
        }
        VALUE res = rb_hash_new();
        for (const auto& entry : resp.stats) {
            rb_hash_aset(res, rb_str_new(entry.first.data(), static_cast<long>(entry.first.size())), ULL2NUM(entry.second));
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataset_get_all(VALUE self, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_dataset_get_all_request req{};
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_dataset_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_dataset_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, "unable to fetch all datasets");
            } else {
                const auto& first_error = resp.errors.front();
                exc =
                  cb__map_error_code(resp.ec, fmt::format("unable to fetch all datasets ({}: {})", first_error.code, first_error.message));
            }
            break;
        }
        VALUE res = rb_ary_new_capa(static_cast<long>(resp.datasets.size()));
        for (const auto& ds : resp.datasets) {
            VALUE dataset = rb_hash_new();
            rb_hash_aset(dataset, rb_id2sym(rb_intern("name")), rb_str_new(ds.name.data(), static_cast<long>(ds.name.size())));
            rb_hash_aset(dataset,
                         rb_id2sym(rb_intern("dataverse_name")),
                         rb_str_new(ds.dataverse_name.data(), static_cast<long>(ds.dataverse_name.size())));
            rb_hash_aset(
              dataset, rb_id2sym(rb_intern("link_name")), rb_str_new(ds.link_name.data(), static_cast<long>(ds.link_name.size())));
            rb_hash_aset(
              dataset, rb_id2sym(rb_intern("bucket_name")), rb_str_new(ds.bucket_name.data(), static_cast<long>(ds.bucket_name.size())));
            rb_ary_push(res, dataset);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataset_drop(VALUE self, VALUE dataset_name, VALUE dataverse_name, VALUE ignore_if_does_not_exist, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(dataset_name, T_STRING);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_dataset_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.dataset_name.assign(RSTRING_PTR(dataset_name), static_cast<size_t>(RSTRING_LEN(dataset_name)));
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        }
        if (!NIL_P(ignore_if_does_not_exist)) {
            req.ignore_if_does_not_exist = RTEST(ignore_if_does_not_exist);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_dataset_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_dataset_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to drop dataset `{}`.`{}`", req.dataverse_name, req.dataset_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to drop dataset `{}`.`{}` ({}: {})",
                                                     req.dataverse_name,
                                                     req.dataset_name,
                                                     first_error.code,
                                                     first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataset_create(VALUE self,
                                    VALUE dataset_name,
                                    VALUE bucket_name,
                                    VALUE condition,
                                    VALUE dataverse_name,
                                    VALUE ignore_if_exists,
                                    VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(dataset_name, T_STRING);
    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(condition)) {
        Check_Type(condition, T_STRING);
    }
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_dataset_create_request req{};
        cb__extract_timeout(req, timeout);
        req.dataset_name.assign(RSTRING_PTR(dataset_name), static_cast<size_t>(RSTRING_LEN(dataset_name)));
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        if (!NIL_P(condition)) {
            req.condition.emplace(std::string(RSTRING_PTR(condition), static_cast<size_t>(RSTRING_LEN(condition))));
        }
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        }
        if (!NIL_P(ignore_if_exists)) {
            req.ignore_if_exists = RTEST(ignore_if_exists);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_dataset_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_dataset_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to create dataset `{}`.`{}`", req.dataverse_name, req.dataset_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to create dataset `{}`.`{}` ({}: {})",
                                                     req.dataverse_name,
                                                     req.dataset_name,
                                                     first_error.code,
                                                     first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataverse_drop(VALUE self, VALUE dataverse_name, VALUE ignore_if_does_not_exist, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(dataverse_name, T_STRING);

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_dataverse_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        if (!NIL_P(ignore_if_does_not_exist)) {
            req.ignore_if_does_not_exist = RTEST(ignore_if_does_not_exist);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_dataverse_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_dataverse_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to drop dataverse `{}`", req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format("unable to drop dataverse `{}` ({}: {})", req.dataverse_name, first_error.code, first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataverse_create(VALUE self, VALUE dataverse_name, VALUE ignore_if_exists, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(dataverse_name, T_STRING);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_dataverse_create_request req{};
        cb__extract_timeout(req, timeout);
        req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        if (!NIL_P(ignore_if_exists)) {
            req.ignore_if_exists = RTEST(ignore_if_exists);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_dataverse_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_dataverse_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to create dataverse `{}`", req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format("unable to create dataverse `{}` ({}: {})", req.dataverse_name, first_error.code, first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_index_get_all(VALUE self, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_index_get_all_request req{};
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_index_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_index_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, "unable to fetch all indexes");
            } else {
                const auto& first_error = resp.errors.front();
                exc =
                  cb__map_error_code(resp.ec, fmt::format("unable to fetch all indexes ({}: {})", first_error.code, first_error.message));
            }
            break;
        }
        VALUE res = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            rb_hash_aset(index, rb_id2sym(rb_intern("name")), rb_str_new(idx.name.data(), static_cast<long>(idx.name.size())));
            rb_hash_aset(
              index, rb_id2sym(rb_intern("dataset_name")), rb_str_new(idx.dataset_name.data(), static_cast<long>(idx.dataset_name.size())));
            rb_hash_aset(index,
                         rb_id2sym(rb_intern("dataverse_name")),
                         rb_str_new(idx.dataverse_name.data(), static_cast<long>(idx.dataverse_name.size())));
            rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
            rb_ary_push(res, index);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_index_create(VALUE self,
                                  VALUE index_name,
                                  VALUE dataset_name,
                                  VALUE fields,
                                  VALUE dataverse_name,
                                  VALUE ignore_if_exists,
                                  VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    Check_Type(dataset_name, T_STRING);
    Check_Type(fields, T_ARRAY);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_index_create_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.dataset_name.assign(RSTRING_PTR(dataset_name), static_cast<size_t>(RSTRING_LEN(dataset_name)));
        auto fields_num = static_cast<size_t>(RARRAY_LEN(fields));
        for (size_t i = 0; i < fields_num; ++i) {
            VALUE entry = rb_ary_entry(fields, static_cast<long>(i));
            Check_Type(entry, T_ARRAY);
            if (RARRAY_LEN(entry) == 2) {
                VALUE field = rb_ary_entry(entry, 0);
                VALUE type = rb_ary_entry(entry, 1);
                req.fields.emplace(std::string(RSTRING_PTR(field), static_cast<std::size_t>(RSTRING_LEN(field))),
                                   std::string(RSTRING_PTR(type), static_cast<std::size_t>(RSTRING_LEN(type))));
            }
        }
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        }
        if (!NIL_P(ignore_if_exists)) {
            req.ignore_if_exists = RTEST(ignore_if_exists);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_index_create_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_index_create_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to create index `{}` on `{}`.`{}`", req.index_name, req.dataverse_name, req.dataset_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to create index `{}` on `{}`.`{}` ({}: {})",
                                                     req.index_name,
                                                     req.dataverse_name,
                                                     req.dataset_name,
                                                     first_error.code,
                                                     first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_index_drop(VALUE self,
                                VALUE index_name,
                                VALUE dataset_name,
                                VALUE dataverse_name,
                                VALUE ignore_if_does_not_exist,
                                VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(index_name, T_STRING);
    Check_Type(dataset_name, T_STRING);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_index_drop_request req{};
        cb__extract_timeout(req, timeout);
        req.index_name.assign(RSTRING_PTR(index_name), static_cast<size_t>(RSTRING_LEN(index_name)));
        req.dataset_name.assign(RSTRING_PTR(dataset_name), static_cast<size_t>(RSTRING_LEN(dataset_name)));
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        }
        if (!NIL_P(ignore_if_does_not_exist)) {
            req.ignore_if_does_not_exist = RTEST(ignore_if_does_not_exist);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_index_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_index_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(
                  resp.ec, fmt::format("unable to drop index `{}`.`{}`.`{}`", req.dataverse_name, req.dataset_name, req.index_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to drop index `{}`.`{}`.`{}` ({}: {})",
                                                     req.dataverse_name,
                                                     req.dataset_name,
                                                     req.index_name,
                                                     first_error.code,
                                                     first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_connect(VALUE self, VALUE link_name, VALUE force, VALUE dataverse_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(link_name, T_STRING);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_link_connect_request req{};
        cb__extract_timeout(req, timeout);
        req.link_name.assign(RSTRING_PTR(link_name), static_cast<size_t>(RSTRING_LEN(link_name)));
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        }
        if (!NIL_P(force)) {
            req.force = RTEST(force);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_link_connect_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_link_connect_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to connect link `{}` on `{}`", req.link_name, req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to connect link `{}` on `{}` ({}: {})",
                                                     req.link_name,
                                                     req.dataverse_name,
                                                     first_error.code,
                                                     first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_disconnect(VALUE self, VALUE link_name, VALUE dataverse_name, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(link_name, T_STRING);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_link_disconnect_request req{};
        cb__extract_timeout(req, timeout);
        req.link_name.assign(RSTRING_PTR(link_name), static_cast<size_t>(RSTRING_LEN(link_name)));
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name.assign(RSTRING_PTR(dataverse_name), static_cast<size_t>(RSTRING_LEN(dataverse_name)));
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_link_disconnect_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::analytics_link_disconnect_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.errors.empty()) {
                exc = cb__map_error_code(resp.ec, fmt::format("unable to disconnect link `{}` on `{}`", req.link_name, req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to disconnect link `{}` on `{}` ({}: {})",
                                                     req.link_name,
                                                     req.dataverse_name,
                                                     first_error.code,
                                                     first_error.message));
            }
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static int
cb__for_each_named_param__analytics(VALUE key, VALUE value, VALUE arg)
{
    auto* preq = reinterpret_cast<couchbase::operations::analytics_request*>(arg);
    Check_Type(key, T_STRING);
    Check_Type(value, T_STRING);
    preq->named_parameters.emplace(
      std::string_view(RSTRING_PTR(key), static_cast<std::size_t>(RSTRING_LEN(key))),
      tao::json::from_string(std::string_view(RSTRING_PTR(value), static_cast<std::size_t>(RSTRING_LEN(value)))));
    return ST_CONTINUE;
}

static VALUE
cb_Backend_document_analytics(VALUE self, VALUE statement, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(statement, T_STRING);
    Check_Type(options, T_HASH);

    VALUE exc = Qnil;
    do {
        couchbase::operations::analytics_request req;
        req.statement.assign(RSTRING_PTR(statement), static_cast<size_t>(RSTRING_LEN(statement)));
        VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
        if (!NIL_P(client_context_id)) {
            Check_Type(client_context_id, T_STRING);
            req.client_context_id.assign(RSTRING_PTR(client_context_id), static_cast<size_t>(RSTRING_LEN(client_context_id)));
        }
        cb__extract_timeout(req, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
        VALUE readonly = rb_hash_aref(options, rb_id2sym(rb_intern("readonly")));
        if (!NIL_P(readonly)) {
            req.readonly = RTEST(readonly);
        }
        VALUE priority = rb_hash_aref(options, rb_id2sym(rb_intern("priority")));
        if (!NIL_P(priority)) {
            req.priority = RTEST(priority);
        }
        VALUE positional_params = rb_hash_aref(options, rb_id2sym(rb_intern("positional_parameters")));
        if (!NIL_P(positional_params)) {
            Check_Type(positional_params, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(positional_params));
            req.positional_parameters.reserve(entries_num);
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
                Check_Type(entry, T_STRING);
                req.positional_parameters.emplace_back(
                  tao::json::from_string(std::string_view(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry)))));
            }
        }
        VALUE named_params = rb_hash_aref(options, rb_id2sym(rb_intern("named_parameters")));
        if (!NIL_P(named_params)) {
            Check_Type(named_params, T_HASH);
            rb_hash_foreach(named_params, INT_FUNC(cb__for_each_named_param__analytics), reinterpret_cast<VALUE>(&req));
        }
        VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
        if (!NIL_P(scan_consistency)) {
            Check_Type(scan_consistency, T_SYMBOL);
            ID type = rb_sym2id(scan_consistency);
            if (type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::operations::analytics_request::scan_consistency_type::not_bounded;
            } else if (type == rb_intern("request_plus")) {
                req.scan_consistency = couchbase::operations::analytics_request::scan_consistency_type::request_plus;
            }
        }

        VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters")));
        if (!NIL_P(raw_params)) {
            Check_Type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb__for_each_named_param__analytics), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::analytics_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.payload.meta_data.errors && !resp.payload.meta_data.errors->empty()) {
                const auto& first_error = resp.payload.meta_data.errors->front();
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to execute analytics query: \"{}{}\" ({}: {})",
                                                     req.statement.substr(0, 50),
                                                     req.statement.size() > 50 ? "..." : "",
                                                     first_error.code,
                                                     first_error.message));
            } else {
                exc = cb__map_error_code(resp.ec,
                                         fmt::format("unable to execute analytics query: \"{}{}\"",
                                                     req.statement.substr(0, 50),
                                                     req.statement.size() > 50 ? "..." : ""));
            }
            break;
        }
        VALUE res = rb_hash_new();
        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.payload.rows.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
        for (auto& row : resp.payload.rows) {
            rb_ary_push(rows, rb_str_new(row.data(), static_cast<long>(row.size())));
        }
        VALUE meta = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
        rb_hash_aset(meta,
                     rb_id2sym(rb_intern("status")),
                     rb_id2sym(rb_intern2(resp.payload.meta_data.status.data(), static_cast<long>(resp.payload.meta_data.status.size()))));
        rb_hash_aset(meta,
                     rb_id2sym(rb_intern("request_id")),
                     rb_str_new(resp.payload.meta_data.request_id.data(), static_cast<long>(resp.payload.meta_data.request_id.size())));
        rb_hash_aset(
          meta,
          rb_id2sym(rb_intern("client_context_id")),
          rb_str_new(resp.payload.meta_data.client_context_id.data(), static_cast<long>(resp.payload.meta_data.client_context_id.size())));
        if (resp.payload.meta_data.signature) {
            rb_hash_aset(meta,
                         rb_id2sym(rb_intern("signature")),
                         rb_str_new(resp.payload.meta_data.signature->data(), static_cast<long>(resp.payload.meta_data.signature->size())));
        }
        if (resp.payload.meta_data.profile) {
            rb_hash_aset(meta,
                         rb_id2sym(rb_intern("profile")),
                         rb_str_new(resp.payload.meta_data.profile->data(), static_cast<long>(resp.payload.meta_data.profile->size())));
        }
        VALUE metrics = rb_hash_new();
        rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("elapsed_time")),
                     rb_str_new(resp.payload.meta_data.metrics.elapsed_time.data(),
                                static_cast<long>(resp.payload.meta_data.metrics.elapsed_time.size())));
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("execution_time")),
                     rb_str_new(resp.payload.meta_data.metrics.execution_time.data(),
                                static_cast<long>(resp.payload.meta_data.metrics.execution_time.size())));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.payload.meta_data.metrics.result_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.payload.meta_data.metrics.result_count));
        if (resp.payload.meta_data.metrics.sort_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("sort_count")), ULL2NUM(*resp.payload.meta_data.metrics.sort_count));
        }
        if (resp.payload.meta_data.metrics.mutation_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("mutation_count")), ULL2NUM(*resp.payload.meta_data.metrics.mutation_count));
        }
        if (resp.payload.meta_data.metrics.error_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("error_count")), ULL2NUM(*resp.payload.meta_data.metrics.error_count));
        }
        if (resp.payload.meta_data.metrics.warning_count) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("warning_count")), ULL2NUM(*resp.payload.meta_data.metrics.warning_count));
        }

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_parse_connection_string(VALUE self, VALUE connection_string)
{
    (void)self;
    Check_Type(connection_string, T_STRING);

    std::string input(RSTRING_PTR(connection_string), static_cast<size_t>(RSTRING_LEN(connection_string)));
    auto connstr = couchbase::utils::parse_connection_string(input);

    VALUE res = rb_hash_new();
    if (!connstr.scheme.empty()) {
        rb_hash_aset(res, rb_id2sym(rb_intern("scheme")), rb_str_new(connstr.scheme.data(), static_cast<long>(connstr.scheme.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("tls")), connstr.tls ? Qtrue : Qfalse);
    }

    VALUE nodes = rb_ary_new_capa(static_cast<long>(connstr.bootstrap_nodes.size()));
    for (const auto& entry : connstr.bootstrap_nodes) {
        VALUE node = rb_hash_new();
        rb_hash_aset(node, rb_id2sym(rb_intern("address")), rb_str_new(entry.address.data(), static_cast<long>(entry.address.size())));
        if (entry.port > 0) {
            rb_hash_aset(node, rb_id2sym(rb_intern("port")), UINT2NUM(entry.port));
        }
        switch (entry.mode) {
            case couchbase::utils::connection_string::bootstrap_mode::gcccp:
                rb_hash_aset(node, rb_id2sym(rb_intern("mode")), rb_id2sym(rb_intern("gcccp")));
                break;
            case couchbase::utils::connection_string::bootstrap_mode::http:
                rb_hash_aset(node, rb_id2sym(rb_intern("mode")), rb_id2sym(rb_intern("http")));
                break;
            case couchbase::utils::connection_string::bootstrap_mode::unspecified:
                break;
        }
        switch (entry.type) {
            case couchbase::utils::connection_string::address_type::ipv4:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("ipv4")));
                break;
            case couchbase::utils::connection_string::address_type::ipv6:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("ipv6")));
                break;
            case couchbase::utils::connection_string::address_type::dns:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("dns")));
                break;
        }
        rb_ary_push(nodes, node);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("nodes")), nodes);

    VALUE params = rb_hash_new();
    for (const auto& param : connstr.params) {
        rb_hash_aset(params,
                     rb_str_new(param.first.data(), static_cast<long>(param.first.size())),
                     rb_str_new(param.second.data(), static_cast<long>(param.second.size())));
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("params")), params);

    if (connstr.default_bucket_name) {
        rb_hash_aset(res,
                     rb_id2sym(rb_intern("default_bucket_name")),
                     rb_str_new(connstr.default_bucket_name->data(), static_cast<long>(connstr.default_bucket_name->size())));
    }
    if (connstr.default_port > 0) {
        rb_hash_aset(res, rb_id2sym(rb_intern("default_port")), UINT2NUM(connstr.default_port));
    }
    switch (connstr.default_mode) {
        case couchbase::utils::connection_string::bootstrap_mode::gcccp:
            rb_hash_aset(res, rb_id2sym(rb_intern("default_mode")), rb_id2sym(rb_intern("gcccp")));
            break;
        case couchbase::utils::connection_string::bootstrap_mode::http:
            rb_hash_aset(res, rb_id2sym(rb_intern("default_mode")), rb_id2sym(rb_intern("http")));
            break;
        case couchbase::utils::connection_string::bootstrap_mode::unspecified:
            break;
    }
    if (connstr.error) {
        rb_hash_aset(res, rb_id2sym(rb_intern("error")), rb_str_new(connstr.error->data(), static_cast<long>(connstr.error->size())));
    }
    return res;
}

static VALUE
cb_Backend_view_index_get_all(VALUE self, VALUE bucket_name, VALUE name_space, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns;
    ID type = rb_sym2id(name_space);
    if (type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::view_index_get_all_request req{};
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.name_space = ns;
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::view_index_get_all_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::view_index_get_all_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(resp.ec, "unable to get list of the design documents");
            break;
        }
        VALUE res = rb_ary_new_capa(static_cast<long>(resp.design_documents.size()));
        for (const auto& entry : resp.design_documents) {
            VALUE dd = rb_hash_new();
            rb_hash_aset(dd, rb_id2sym(rb_intern("name")), rb_str_new(entry.name.data(), static_cast<long>(entry.name.size())));
            rb_hash_aset(dd, rb_id2sym(rb_intern("rev")), rb_str_new(entry.rev.data(), static_cast<long>(entry.rev.size())));
            switch (entry.ns) {
                case couchbase::operations::design_document::name_space::development:
                    rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
                    break;
                case couchbase::operations::design_document::name_space::production:
                    rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("production")));
                    break;
            }
            VALUE views = rb_hash_new();
            for (const auto& view_entry : entry.views) {
                VALUE view_name = rb_str_new(view_entry.first.data(), static_cast<long>(view_entry.first.size()));
                VALUE view = rb_hash_new();
                rb_hash_aset(view, rb_id2sym(rb_intern("name")), view_name);
                if (view_entry.second.map) {
                    rb_hash_aset(view,
                                 rb_id2sym(rb_intern("map")),
                                 rb_str_new(view_entry.second.map->data(), static_cast<long>(view_entry.second.map->size())));
                }
                if (view_entry.second.reduce) {
                    rb_hash_aset(view,
                                 rb_id2sym(rb_intern("reduce")),
                                 rb_str_new(view_entry.second.reduce->data(), static_cast<long>(view_entry.second.reduce->size())));
                }
                rb_hash_aset(views, view_name, view);
            }
            rb_hash_aset(dd, rb_id2sym(rb_intern("views")), views);
            rb_ary_push(res, dd);
        }
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_view_index_get(VALUE self, VALUE bucket_name, VALUE document_name, VALUE name_space, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(document_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns;
    ID type = rb_sym2id(name_space);
    if (type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::view_index_get_request req{};
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.document_name.assign(RSTRING_PTR(document_name), static_cast<size_t>(RSTRING_LEN(document_name)));
        req.name_space = ns;
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::view_index_get_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::view_index_get_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec,
              fmt::format(R"(unable to get design document "{}" ({}) on bucket "{}")", req.document_name, req.name_space, req.bucket_name));
            break;
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(
          res, rb_id2sym(rb_intern("name")), rb_str_new(resp.document.name.data(), static_cast<long>(resp.document.name.size())));
        rb_hash_aset(res, rb_id2sym(rb_intern("rev")), rb_str_new(resp.document.rev.data(), static_cast<long>(resp.document.rev.size())));
        switch (resp.document.ns) {
            case couchbase::operations::design_document::name_space::development:
                rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
                break;
            case couchbase::operations::design_document::name_space::production:
                rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("production")));
                break;
        }
        VALUE views = rb_hash_new();
        for (const auto& view_entry : resp.document.views) {
            VALUE view_name = rb_str_new(view_entry.first.data(), static_cast<long>(view_entry.first.size()));
            VALUE view = rb_hash_new();
            rb_hash_aset(view, rb_id2sym(rb_intern("name")), view_name);
            if (view_entry.second.map) {
                rb_hash_aset(view,
                             rb_id2sym(rb_intern("map")),
                             rb_str_new(view_entry.second.map->data(), static_cast<long>(view_entry.second.map->size())));
            }
            if (view_entry.second.reduce) {
                rb_hash_aset(view,
                             rb_id2sym(rb_intern("reduce")),
                             rb_str_new(view_entry.second.reduce->data(), static_cast<long>(view_entry.second.reduce->size())));
            }
            rb_hash_aset(views, view_name, view);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("views")), views);
        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_view_index_drop(VALUE self, VALUE bucket_name, VALUE document_name, VALUE name_space, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(document_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns;
    ID type = rb_sym2id(name_space);
    if (type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::view_index_drop_request req{};
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.document_name.assign(RSTRING_PTR(document_name), static_cast<size_t>(RSTRING_LEN(document_name)));
        req.name_space = ns;
        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::view_index_drop_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::view_index_drop_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec,
              fmt::format(
                R"(unable to drop design document "{}" ({}) on bucket "{}")", req.document_name, req.name_space, req.bucket_name));
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_view_index_upsert(VALUE self, VALUE bucket_name, VALUE document, VALUE name_space, VALUE timeout)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(document, T_HASH);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns;
    ID type = rb_sym2id(name_space);
    if (type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::view_index_upsert_request req{};
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.document.ns = ns;
        VALUE document_name = rb_hash_aref(document, rb_id2sym(rb_intern("name")));
        if (!NIL_P(document_name)) {
            Check_Type(document_name, T_STRING);
            req.document.name.assign(RSTRING_PTR(document_name), static_cast<size_t>(RSTRING_LEN(document_name)));
        }
        VALUE views = rb_hash_aref(document, rb_id2sym(rb_intern("views")));
        if (!NIL_P(views)) {
            Check_Type(views, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(views));
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(views, static_cast<long>(i));
                Check_Type(entry, T_HASH);
                couchbase::operations::design_document::view view;
                VALUE name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
                Check_Type(name, T_STRING);
                view.name.assign(RSTRING_PTR(name), static_cast<std::size_t>(RSTRING_LEN(name)));
                VALUE map = rb_hash_aref(entry, rb_id2sym(rb_intern("map")));
                if (!NIL_P(map)) {
                    view.map.emplace(std::string(RSTRING_PTR(map), static_cast<std::size_t>(RSTRING_LEN(map))));
                }
                VALUE reduce = rb_hash_aref(entry, rb_id2sym(rb_intern("reduce")));
                if (!NIL_P(reduce)) {
                    view.reduce.emplace(std::string(RSTRING_PTR(reduce), static_cast<std::size_t>(RSTRING_LEN(reduce))));
                }
                req.document.views[view.name] = view;
            }
        }

        cb__extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::view_index_upsert_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(
          req, [barrier](couchbase::operations::view_index_upsert_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            exc = cb__map_error_code(
              resp.ec,
              fmt::format(
                R"(unable to store design document "{}" ({}) on bucket "{}")", req.document.name, req.document.ns, req.bucket_name));
            break;
        }
        return Qtrue;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static VALUE
cb_Backend_document_view(VALUE self, VALUE bucket_name, VALUE design_document_name, VALUE view_name, VALUE name_space, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(design_document_name, T_STRING);
    Check_Type(view_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);
    couchbase::operations::design_document::name_space ns;
    ID type = rb_sym2id(name_space);
    if (type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    VALUE exc = Qnil;
    do {
        couchbase::operations::document_view_request req{};
        req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
        req.document_name.assign(RSTRING_PTR(design_document_name), static_cast<size_t>(RSTRING_LEN(design_document_name)));
        req.view_name.assign(RSTRING_PTR(view_name), static_cast<size_t>(RSTRING_LEN(view_name)));
        req.name_space = ns;
        if (!NIL_P(options)) {
            cb__extract_timeout(req, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
            VALUE debug = rb_hash_aref(options, rb_id2sym(rb_intern("debug")));
            if (!NIL_P(debug)) {
                req.debug = RTEST(debug);
            }
            VALUE limit = rb_hash_aref(options, rb_id2sym(rb_intern("limit")));
            if (!NIL_P(limit)) {
                Check_Type(limit, T_FIXNUM);
                req.limit = FIX2ULONG(limit);
            }
            VALUE skip = rb_hash_aref(options, rb_id2sym(rb_intern("skip")));
            if (!NIL_P(skip)) {
                Check_Type(skip, T_FIXNUM);
                req.skip = FIX2ULONG(skip);
            }
            VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
            if (!NIL_P(scan_consistency)) {
                Check_Type(scan_consistency, T_SYMBOL);
                ID consistency = rb_sym2id(scan_consistency);
                if (consistency == rb_intern("request_plus")) {
                    req.consistency = couchbase::operations::document_view_request::scan_consistency::request_plus;
                } else if (consistency == rb_intern("update_after")) {
                    req.consistency = couchbase::operations::document_view_request::scan_consistency::update_after;
                } else if (consistency == rb_intern("not_bounded")) {
                    req.consistency = couchbase::operations::document_view_request::scan_consistency::not_bounded;
                }
            }
            VALUE key = rb_hash_aref(options, rb_id2sym(rb_intern("key")));
            if (!NIL_P(key)) {
                Check_Type(key, T_STRING);
                req.key.emplace(RSTRING_PTR(key), static_cast<size_t>(RSTRING_LEN(key)));
            }
            VALUE start_key = rb_hash_aref(options, rb_id2sym(rb_intern("start_key")));
            if (!NIL_P(start_key)) {
                Check_Type(start_key, T_STRING);
                req.start_key.emplace(RSTRING_PTR(start_key), static_cast<size_t>(RSTRING_LEN(start_key)));
            }
            VALUE end_key = rb_hash_aref(options, rb_id2sym(rb_intern("end_key")));
            if (!NIL_P(end_key)) {
                Check_Type(end_key, T_STRING);
                req.end_key.emplace(RSTRING_PTR(end_key), static_cast<size_t>(RSTRING_LEN(end_key)));
            }
            VALUE start_key_doc_id = rb_hash_aref(options, rb_id2sym(rb_intern("start_key_doc_id")));
            if (!NIL_P(start_key_doc_id)) {
                Check_Type(start_key_doc_id, T_STRING);
                req.start_key_doc_id.emplace(RSTRING_PTR(start_key_doc_id), static_cast<size_t>(RSTRING_LEN(start_key_doc_id)));
            }
            VALUE end_key_doc_id = rb_hash_aref(options, rb_id2sym(rb_intern("end_key_doc_id")));
            if (!NIL_P(end_key_doc_id)) {
                Check_Type(end_key_doc_id, T_STRING);
                req.end_key_doc_id.emplace(RSTRING_PTR(end_key_doc_id), static_cast<size_t>(RSTRING_LEN(end_key_doc_id)));
            }
            VALUE inclusive_end = rb_hash_aref(options, rb_id2sym(rb_intern("inclusive_end")));
            if (!NIL_P(inclusive_end)) {
                req.inclusive_end = RTEST(inclusive_end);
            }
            VALUE reduce = rb_hash_aref(options, rb_id2sym(rb_intern("reduce")));
            if (!NIL_P(reduce)) {
                req.reduce = RTEST(reduce);
            }
            VALUE group = rb_hash_aref(options, rb_id2sym(rb_intern("group")));
            if (!NIL_P(group)) {
                req.group = RTEST(group);
            }
            VALUE group_level = rb_hash_aref(options, rb_id2sym(rb_intern("group_level")));
            if (!NIL_P(group_level)) {
                Check_Type(group_level, T_FIXNUM);
                req.group_level = FIX2ULONG(group_level);
            }
            VALUE sort_order = rb_hash_aref(options, rb_id2sym(rb_intern("order")));
            if (!NIL_P(sort_order)) {
                Check_Type(sort_order, T_SYMBOL);
                ID order = rb_sym2id(sort_order);
                if (order == rb_intern("ascending")) {
                    req.order = couchbase::operations::document_view_request::sort_order::ascending;
                } else if (order == rb_intern("descending")) {
                    req.order = couchbase::operations::document_view_request::sort_order::descending;
                }
            }
            VALUE keys = rb_hash_aref(options, rb_id2sym(rb_intern("keys")));
            if (!NIL_P(keys)) {
                Check_Type(keys, T_ARRAY);
                auto entries_num = static_cast<size_t>(RARRAY_LEN(keys));
                req.keys.reserve(entries_num);
                for (size_t i = 0; i < entries_num; ++i) {
                    VALUE entry = rb_ary_entry(keys, static_cast<long>(i));
                    Check_Type(entry, T_STRING);
                    req.keys.emplace_back(std::string(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry))));
                }
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::document_view_response>>();
        auto f = barrier->get_future();
        backend->cluster->execute_http(req,
                                       [barrier](couchbase::operations::document_view_response resp) mutable { barrier->set_value(resp); });
        auto resp = f.get();
        if (resp.ec) {
            if (resp.error) {
                exc = cb__map_error_code(
                  resp.ec,
                  fmt::format(R"(unable to execute query for view "{}" of design document "{}" ({}) on bucket "{}": {} ({}))",
                              req.view_name,
                              req.document_name,
                              req.name_space,
                              req.bucket_name,
                              resp.error->code,
                              resp.error->message));
            } else {
                exc = cb__map_error_code(resp.ec,
                                         fmt::format(R"(unable to execute query for view "{}" of design document "{}" ({}) on bucket "{}")",
                                                     req.view_name,
                                                     req.document_name,
                                                     req.name_space,
                                                     req.bucket_name));
            }
            break;
        }
        VALUE res = rb_hash_new();

        VALUE meta = rb_hash_new();
        if (resp.meta_data.total_rows) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("total_rows")), ULL2NUM(*resp.meta_data.total_rows));
        }
        if (resp.meta_data.debug_info) {
            rb_hash_aset(meta,
                         rb_id2sym(rb_intern("debug_info")),
                         rb_str_new(resp.meta_data.debug_info->data(), static_cast<long>(resp.meta_data.debug_info->size())));
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);

        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
        for (const auto& entry : resp.rows) {
            VALUE row = rb_hash_new();
            if (entry.id) {
                rb_hash_aset(row, rb_id2sym(rb_intern("id")), rb_str_new(entry.id->data(), static_cast<long>(entry.id->size())));
            }
            rb_hash_aset(row, rb_id2sym(rb_intern("key")), rb_str_new(entry.key.data(), static_cast<long>(entry.key.size())));
            rb_hash_aset(row, rb_id2sym(rb_intern("value")), rb_str_new(entry.value.data(), static_cast<long>(entry.value.size())));
            rb_ary_push(rows, row);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);

        return res;
    } while (false);
    rb_exc_raise(exc);
    return Qnil;
}

static void
init_backend(VALUE mCouchbase)
{
    VALUE cBackend = rb_define_class_under(mCouchbase, "Backend", rb_cBasicObject);
    rb_define_alloc_func(cBackend, cb_Backend_allocate);
    rb_define_method(cBackend, "open", VALUE_FUNC(cb_Backend_open), 4);
    rb_define_method(cBackend, "close", VALUE_FUNC(cb_Backend_close), 0);
    rb_define_method(cBackend, "open_bucket", VALUE_FUNC(cb_Backend_open_bucket), 2);

    rb_define_method(cBackend, "document_get", VALUE_FUNC(cb_Backend_document_get), 4);
    rb_define_method(cBackend, "document_get_projected", VALUE_FUNC(cb_Backend_document_get_projected), 7);
    rb_define_method(cBackend, "document_get_and_lock", VALUE_FUNC(cb_Backend_document_get_and_lock), 5);
    rb_define_method(cBackend, "document_get_and_touch", VALUE_FUNC(cb_Backend_document_get_and_touch), 5);
    rb_define_method(cBackend, "document_insert", VALUE_FUNC(cb_Backend_document_insert), 7);
    rb_define_method(cBackend, "document_replace", VALUE_FUNC(cb_Backend_document_replace), 7);
    rb_define_method(cBackend, "document_upsert", VALUE_FUNC(cb_Backend_document_upsert), 7);
    rb_define_method(cBackend, "document_remove", VALUE_FUNC(cb_Backend_document_remove), 5);
    rb_define_method(cBackend, "document_lookup_in", VALUE_FUNC(cb_Backend_document_lookup_in), 6);
    rb_define_method(cBackend, "document_mutate_in", VALUE_FUNC(cb_Backend_document_mutate_in), 6);
    rb_define_method(cBackend, "document_query", VALUE_FUNC(cb_Backend_document_query), 2);
    rb_define_method(cBackend, "document_touch", VALUE_FUNC(cb_Backend_document_touch), 5);
    rb_define_method(cBackend, "document_exists", VALUE_FUNC(cb_Backend_document_exists), 4);
    rb_define_method(cBackend, "document_unlock", VALUE_FUNC(cb_Backend_document_unlock), 5);
    rb_define_method(cBackend, "document_increment", VALUE_FUNC(cb_Backend_document_increment), 5);
    rb_define_method(cBackend, "document_decrement", VALUE_FUNC(cb_Backend_document_decrement), 5);
    rb_define_method(cBackend, "document_search", VALUE_FUNC(cb_Backend_document_search), 3);
    rb_define_method(cBackend, "document_analytics", VALUE_FUNC(cb_Backend_document_analytics), 2);
    rb_define_method(cBackend, "document_view", VALUE_FUNC(cb_Backend_document_view), 5);

    rb_define_method(cBackend, "bucket_create", VALUE_FUNC(cb_Backend_bucket_create), 2);
    rb_define_method(cBackend, "bucket_update", VALUE_FUNC(cb_Backend_bucket_update), 2);
    rb_define_method(cBackend, "bucket_drop", VALUE_FUNC(cb_Backend_bucket_drop), 2);
    rb_define_method(cBackend, "bucket_flush", VALUE_FUNC(cb_Backend_bucket_flush), 2);
    rb_define_method(cBackend, "bucket_get_all", VALUE_FUNC(cb_Backend_bucket_get_all), 1);
    rb_define_method(cBackend, "bucket_get", VALUE_FUNC(cb_Backend_bucket_get), 2);

    rb_define_method(cBackend, "cluster_enable_developer_preview!", VALUE_FUNC(cb_Backend_cluster_enable_developer_preview), 0);

    rb_define_method(cBackend, "scope_get_all", VALUE_FUNC(cb_Backend_scope_get_all), 2);
    rb_define_method(cBackend, "scope_create", VALUE_FUNC(cb_Backend_scope_create), 3);
    rb_define_method(cBackend, "scope_drop", VALUE_FUNC(cb_Backend_scope_drop), 3);
    rb_define_method(cBackend, "collection_create", VALUE_FUNC(cb_Backend_collection_create), 5);
    rb_define_method(cBackend, "collection_drop", VALUE_FUNC(cb_Backend_collection_drop), 4);

    rb_define_method(cBackend, "query_index_get_all", VALUE_FUNC(cb_Backend_query_index_get_all), 2);
    rb_define_method(cBackend, "query_index_create", VALUE_FUNC(cb_Backend_query_index_create), 5);
    rb_define_method(cBackend, "query_index_create_primary", VALUE_FUNC(cb_Backend_query_index_create_primary), 3);
    rb_define_method(cBackend, "query_index_drop", VALUE_FUNC(cb_Backend_query_index_drop), 4);
    rb_define_method(cBackend, "query_index_drop_primary", VALUE_FUNC(cb_Backend_query_index_drop_primary), 3);
    rb_define_method(cBackend, "query_index_build_deferred", VALUE_FUNC(cb_Backend_query_index_build_deferred), 2);
    rb_define_method(cBackend, "query_index_watch", VALUE_FUNC(cb_Backend_query_index_watch), 4);

    rb_define_method(cBackend, "search_index_get_all", VALUE_FUNC(cb_Backend_search_index_get_all), 1);
    rb_define_method(cBackend, "search_index_get", VALUE_FUNC(cb_Backend_search_index_get), 2);
    rb_define_method(cBackend, "search_index_upsert", VALUE_FUNC(cb_Backend_search_index_upsert), 2);
    rb_define_method(cBackend, "search_index_drop", VALUE_FUNC(cb_Backend_search_index_drop), 2);
    rb_define_method(cBackend, "search_index_get_documents_count", VALUE_FUNC(cb_Backend_search_index_get_documents_count), 2);
    rb_define_method(cBackend, "search_index_pause_ingest", VALUE_FUNC(cb_Backend_search_index_pause_ingest), 2);
    rb_define_method(cBackend, "search_index_resume_ingest", VALUE_FUNC(cb_Backend_search_index_resume_ingest), 2);
    rb_define_method(cBackend, "search_index_allow_querying", VALUE_FUNC(cb_Backend_search_index_allow_querying), 2);
    rb_define_method(cBackend, "search_index_disallow_querying", VALUE_FUNC(cb_Backend_search_index_disallow_querying), 2);
    rb_define_method(cBackend, "search_index_freeze_plan", VALUE_FUNC(cb_Backend_search_index_freeze_plan), 2);
    rb_define_method(cBackend, "search_index_unfreeze_plan", VALUE_FUNC(cb_Backend_search_index_unfreeze_plan), 2);
    rb_define_method(cBackend, "search_index_analyze_document", VALUE_FUNC(cb_Backend_search_index_analyze_document), 3);

    rb_define_method(cBackend, "analytics_get_pending_mutations", VALUE_FUNC(cb_Backend_analytics_get_pending_mutations), 1);
    rb_define_method(cBackend, "analytics_dataverse_drop", VALUE_FUNC(cb_Backend_analytics_dataverse_drop), 3);
    rb_define_method(cBackend, "analytics_dataverse_create", VALUE_FUNC(cb_Backend_analytics_dataverse_create), 3);
    rb_define_method(cBackend, "analytics_dataset_create", VALUE_FUNC(cb_Backend_analytics_dataset_create), 6);
    rb_define_method(cBackend, "analytics_dataset_drop", VALUE_FUNC(cb_Backend_analytics_dataset_drop), 4);
    rb_define_method(cBackend, "analytics_dataset_get_all", VALUE_FUNC(cb_Backend_analytics_dataset_get_all), 1);
    rb_define_method(cBackend, "analytics_index_get_all", VALUE_FUNC(cb_Backend_analytics_index_get_all), 1);
    rb_define_method(cBackend, "analytics_index_create", VALUE_FUNC(cb_Backend_analytics_index_create), 6);
    rb_define_method(cBackend, "analytics_index_drop", VALUE_FUNC(cb_Backend_analytics_index_drop), 5);
    rb_define_method(cBackend, "analytics_link_connect", VALUE_FUNC(cb_Backend_analytics_link_connect), 4);
    rb_define_method(cBackend, "analytics_link_disconnect", VALUE_FUNC(cb_Backend_analytics_link_disconnect), 3);

    rb_define_method(cBackend, "view_index_get_all", VALUE_FUNC(cb_Backend_view_index_get_all), 3);
    rb_define_method(cBackend, "view_index_get", VALUE_FUNC(cb_Backend_view_index_get), 4);
    rb_define_method(cBackend, "view_index_drop", VALUE_FUNC(cb_Backend_view_index_drop), 4);
    rb_define_method(cBackend, "view_index_upsert", VALUE_FUNC(cb_Backend_view_index_upsert), 4);

    rb_define_singleton_method(cBackend, "dns_srv", VALUE_FUNC(cb_Backend_dns_srv), 2);
    rb_define_singleton_method(cBackend, "parse_connection_string", VALUE_FUNC(cb_Backend_parse_connection_string), 1);
}

void
init_logger()
{
    spdlog::set_pattern("[%Y-%m-%d %T.%e] [%P,%t] [%^%l%$] %oms %v");

    auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_LOG_LEVEL");
    if (env_val.empty()) {
        spdlog::set_level(spdlog::level::critical);
    } else {
        auto levels = spdlog::cfg::helpers::extract_levels(env_val);
        spdlog::details::registry::instance().update_levels(std::move(levels));
    }
}

extern "C" {
void
Init_libcouchbase(void)
{
    init_logger();

    VALUE mCouchbase = rb_define_module("Couchbase");
    init_versions(mCouchbase);
    init_backend(mCouchbase);
    init_exceptions(mCouchbase);
}
}
