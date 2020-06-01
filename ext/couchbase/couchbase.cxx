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

#include <openssl/crypto.h>
#include <asio/version.hpp>

#include <spdlog/spdlog.h>
#include <spdlog/cfg/env.h>

#include <http_parser.h>

#include <snappy.h>

#include <version.hxx>
#include <cluster.hxx>
#include <operations.hxx>

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
        backend->cluster.reset(nullptr);
        if (backend->worker.joinable()) {
            backend->worker.join();
        }
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

static VALUE eBackendError;
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
    eBackendError = rb_define_class_under(mError, "BackendError", rb_eStandardError);
    eAmbiguousTimeout = rb_define_class_under(mError, "AmbiguousTimeout", rb_eStandardError);
    eAuthenticationFailure = rb_define_class_under(mError, "AuthenticationFailure", rb_eStandardError);
    eBucketExists = rb_define_class_under(mError, "BucketExists", rb_eStandardError);
    eBucketNotFlushable = rb_define_class_under(mError, "BucketNotFlushable", rb_eStandardError);
    eBucketNotFound = rb_define_class_under(mError, "BucketNotFound", rb_eStandardError);
    eCasMismatch = rb_define_class_under(mError, "CasMismatch", rb_eStandardError);
    eCollectionExists = rb_define_class_under(mError, "CollectionExists", rb_eStandardError);
    eCollectionNotFound = rb_define_class_under(mError, "CollectionNotFound", rb_eStandardError);
    eCompilationFailure = rb_define_class_under(mError, "CompilationFailure", rb_eStandardError);
    eDatasetExists = rb_define_class_under(mError, "DatasetExists", rb_eStandardError);
    eDatasetNotFound = rb_define_class_under(mError, "DatasetNotFound", rb_eStandardError);
    eDataverseExists = rb_define_class_under(mError, "DataverseExists", rb_eStandardError);
    eDataverseNotFound = rb_define_class_under(mError, "DataverseNotFound", rb_eStandardError);
    eDecodingFailure = rb_define_class_under(mError, "DecodingFailure", rb_eStandardError);
    eDeltaInvalid = rb_define_class_under(mError, "DeltaInvalid", rb_eStandardError);
    eDesignDocumentNotFound = rb_define_class_under(mError, "DesignDocumentNotFound", rb_eStandardError);
    eDocumentExists = rb_define_class_under(mError, "DocumentExists", rb_eStandardError);
    eDocumentIrretrievable = rb_define_class_under(mError, "DocumentIrretrievable", rb_eStandardError);
    eDocumentLocked = rb_define_class_under(mError, "DocumentLocked", rb_eStandardError);
    eDocumentNotFound = rb_define_class_under(mError, "DocumentNotFound", rb_eStandardError);
    eDocumentNotJson = rb_define_class_under(mError, "DocumentNotJson", rb_eStandardError);
    eDurabilityAmbiguous = rb_define_class_under(mError, "DurabilityAmbiguous", rb_eStandardError);
    eDurabilityImpossible = rb_define_class_under(mError, "DurabilityImpossible", rb_eStandardError);
    eDurabilityLevelNotAvailable = rb_define_class_under(mError, "DurabilityLevelNotAvailable", rb_eStandardError);
    eDurableWriteInProgress = rb_define_class_under(mError, "DurableWriteInProgress", rb_eStandardError);
    eDurableWriteReCommitInProgress = rb_define_class_under(mError, "DurableWriteReCommitInProgress", rb_eStandardError);
    eEncodingFailure = rb_define_class_under(mError, "EncodingFailure", rb_eStandardError);
    eFeatureNotAvailable = rb_define_class_under(mError, "FeatureNotAvailable", rb_eStandardError);
    eGroupNotFound = rb_define_class_under(mError, "GroupNotFound", rb_eStandardError);
    eIndexExists = rb_define_class_under(mError, "IndexExists", rb_eStandardError);
    eIndexFailure = rb_define_class_under(mError, "IndexFailure", rb_eStandardError);
    eIndexNotFound = rb_define_class_under(mError, "IndexNotFound", rb_eStandardError);
    eInternalServerFailure = rb_define_class_under(mError, "InternalServerFailure", rb_eStandardError);
    eInvalidArgument = rb_define_class_under(mError, "InvalidArgument", rb_eStandardError);
    eJobQueueFull = rb_define_class_under(mError, "JobQueueFull", rb_eStandardError);
    eLinkNotFound = rb_define_class_under(mError, "LinkNotFound", rb_eStandardError);
    eNumberTooBig = rb_define_class_under(mError, "NumberTooBig", rb_eStandardError);
    eParsingFailure = rb_define_class_under(mError, "ParsingFailure", rb_eStandardError);
    ePathExists = rb_define_class_under(mError, "PathExists", rb_eStandardError);
    ePathInvalid = rb_define_class_under(mError, "PathInvalid", rb_eStandardError);
    ePathMismatch = rb_define_class_under(mError, "PathMismatch", rb_eStandardError);
    ePathNotFound = rb_define_class_under(mError, "PathNotFound", rb_eStandardError);
    ePathTooBig = rb_define_class_under(mError, "PathTooBig", rb_eStandardError);
    ePathTooDeep = rb_define_class_under(mError, "PathTooDeep", rb_eStandardError);
    ePlanningFailure = rb_define_class_under(mError, "PlanningFailure", rb_eStandardError);
    ePreparedStatementFailure = rb_define_class_under(mError, "PreparedStatementFailure", rb_eStandardError);
    eRequestCanceled = rb_define_class_under(mError, "RequestCanceled", rb_eStandardError);
    eScopeExists = rb_define_class_under(mError, "ScopeExists", rb_eStandardError);
    eScopeNotFound = rb_define_class_under(mError, "ScopeNotFound", rb_eStandardError);
    eServiceNotAvailable = rb_define_class_under(mError, "ServiceNotAvailable", rb_eStandardError);
    eTemporaryFailure = rb_define_class_under(mError, "TemporaryFailure", rb_eStandardError);
    eUnambiguousTimeout = rb_define_class_under(mError, "UnambiguousTimeout", rb_eStandardError);
    eUnsupportedOperation = rb_define_class_under(mError, "UnsupportedOperation", rb_eStandardError);
    eUserNotFound = rb_define_class_under(mError, "UserNotFound", rb_eStandardError);
    eUserExists = rb_define_class_under(mError, "UserExists", rb_eStandardError);
    eValueInvalid = rb_define_class_under(mError, "ValueInvalid", rb_eStandardError);
    eValueTooDeep = rb_define_class_under(mError, "ValueTooDeep", rb_eStandardError);
    eValueTooLarge = rb_define_class_under(mError, "ValueTooLarge", rb_eStandardError);
    eViewNotFound = rb_define_class_under(mError, "ViewNotFound", rb_eStandardError);
    eXattrCannotModifyVirtualAttribute = rb_define_class_under(mError, "XattrCannotModifyVirtualAttribute", rb_eStandardError);
    eXattrInvalidKeyCombo = rb_define_class_under(mError, "XattrInvalidKeyCombo", rb_eStandardError);
    eXattrUnknownMacro = rb_define_class_under(mError, "XattrUnknownMacro", rb_eStandardError);
    eXattrUnknownVirtualAttribute = rb_define_class_under(mError, "XattrUnknownVirtualAttribute", rb_eStandardError);
}

static NORETURN(void cb_raise_error_code(std::error_code ec, const std::string& message))
{
    if (ec.category() == couchbase::error::detail::get_common_category()) {
        switch (static_cast<couchbase::error::common_errc>(ec.value())) {
            case couchbase::error::common_errc::unambiguous_timeout:
                rb_raise(eUnambiguousTimeout, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::ambiguous_timeout:
                rb_raise(eAmbiguousTimeout, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::request_canceled:
                rb_raise(eRequestCanceled, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::invalid_argument:
                rb_raise(eInvalidArgument, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::service_not_available:
                rb_raise(eServiceNotAvailable, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::internal_server_failure:
                rb_raise(eInternalServerFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::authentication_failure:
                rb_raise(eAuthenticationFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::temporary_failure:
                rb_raise(eTemporaryFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::parsing_failure:
                rb_raise(eParsingFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::cas_mismatch:
                rb_raise(eCasMismatch, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::bucket_not_found:
                rb_raise(eBucketNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::scope_not_found:
                rb_raise(eScopeNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::collection_not_found:
                rb_raise(eCollectionNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::unsupported_operation:
                rb_raise(eUnsupportedOperation, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::feature_not_available:
                rb_raise(eFeatureNotAvailable, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::encoding_failure:
                rb_raise(eEncodingFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::decoding_failure:
                rb_raise(eDecodingFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::index_not_found:
                rb_raise(eIndexNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::common_errc::index_exists:
                rb_raise(eIndexExists, "%s: %s", message.c_str(), ec.message().c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_key_value_category()) {
        switch (static_cast<couchbase::error::key_value_errc>(ec.value())) {
            case couchbase::error::key_value_errc::document_not_found:
                rb_raise(eDocumentNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::document_irretrievable:
                rb_raise(eDocumentIrretrievable, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::document_locked:
                rb_raise(eDocumentLocked, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::value_too_large:
                rb_raise(eValueTooLarge, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::document_exists:
                rb_raise(eDocumentExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::durability_level_not_available:
                rb_raise(eDurabilityLevelNotAvailable, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::durability_impossible:
                rb_raise(eDurabilityImpossible, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::durability_ambiguous:
                rb_raise(eDurabilityAmbiguous, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::durable_write_in_progress:
                rb_raise(eDurableWriteInProgress, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::durable_write_re_commit_in_progress:
                rb_raise(eDurableWriteReCommitInProgress, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::path_not_found:
                rb_raise(ePathNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::path_mismatch:
                rb_raise(ePathMismatch, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::path_invalid:
                rb_raise(ePathInvalid, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::path_too_big:
                rb_raise(ePathTooBig, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::path_too_deep:
                rb_raise(ePathTooDeep, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::value_too_deep:
                rb_raise(eValueTooDeep, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::value_invalid:
                rb_raise(eValueInvalid, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::document_not_json:
                rb_raise(eDocumentNotJson, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::number_too_big:
                rb_raise(eNumberTooBig, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::delta_invalid:
                rb_raise(eDeltaInvalid, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::path_exists:
                rb_raise(ePathExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::xattr_unknown_macro:
                rb_raise(eXattrUnknownMacro, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::xattr_invalid_key_combo:
                rb_raise(eXattrInvalidKeyCombo, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::xattr_unknown_virtual_attribute:
                rb_raise(eXattrUnknownVirtualAttribute, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::key_value_errc::xattr_cannot_modify_virtual_attribute:
                rb_raise(eXattrCannotModifyVirtualAttribute, "%s: %s", message.c_str(), ec.message().c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_query_category()) {
        switch (static_cast<couchbase::error::query_errc>(ec.value())) {
            case couchbase::error::query_errc::planning_failure:
                rb_raise(ePlanningFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::query_errc::index_failure:
                rb_raise(eIndexFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::query_errc::prepared_statement_failure:
                rb_raise(ePreparedStatementFailure, "%s: %s", message.c_str(), ec.message().c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_view_category()) {
        switch (static_cast<couchbase::error::view_errc>(ec.value())) {
            case couchbase::error::view_errc::view_not_found:
                rb_raise(eViewNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::view_errc::design_document_not_found:
                rb_raise(eDesignDocumentNotFound, "%s: %s", message.c_str(), ec.message().c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_analytics_category()) {
        switch (static_cast<couchbase::error::analytics_errc>(ec.value())) {
            case couchbase::error::analytics_errc::compilation_failure:
                rb_raise(eCompilationFailure, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::analytics_errc::job_queue_full:
                rb_raise(eJobQueueFull, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::analytics_errc::dataset_not_found:
                rb_raise(eDatasetNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::analytics_errc::dataverse_not_found:
                rb_raise(eDataverseNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::analytics_errc::dataset_exists:
                rb_raise(eDatasetExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::analytics_errc::dataverse_exists:
                rb_raise(eDataverseExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::analytics_errc::link_not_found:
                rb_raise(eLinkNotFound, "%s: %s", message.c_str(), ec.message().c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_management_category()) {
        switch (static_cast<couchbase::error::management_errc>(ec.value())) {
            case couchbase::error::management_errc::collection_exists:
                rb_raise(eCollectionExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::management_errc::scope_exists:
                rb_raise(eScopeExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::management_errc::user_not_found:
                rb_raise(eUserNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::management_errc::group_not_found:
                rb_raise(eGroupNotFound, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::management_errc::user_exists:
                rb_raise(eUserExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::management_errc::bucket_exists:
                rb_raise(eBucketExists, "%s: %s", message.c_str(), ec.message().c_str());

            case couchbase::error::management_errc::bucket_not_flushable:
                rb_raise(eBucketNotFlushable, "%s: %s", message.c_str(), ec.message().c_str());
        }
    }

    rb_raise(eBackendError, "%s: %s", message.c_str(), ec.message().c_str());
}

static VALUE
cb_Backend_open(VALUE self, VALUE hostname, VALUE username, VALUE password)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(hostname, T_STRING);
    Check_Type(username, T_STRING);
    Check_Type(password, T_STRING);

    couchbase::origin options;
    options.hostname.assign(RSTRING_PTR(hostname), static_cast<size_t>(RSTRING_LEN(hostname)));
    options.username.assign(RSTRING_PTR(username), static_cast<size_t>(RSTRING_LEN(username)));
    options.password.assign(RSTRING_PTR(password), static_cast<size_t>(RSTRING_LEN(password)));
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    backend->cluster->open(options, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    if (auto ec = f.get()) {
        cb_raise_error_code(ec, fmt::format("unable open cluster at {}", options.hostname));
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
cb_Backend_open_bucket(VALUE self, VALUE bucket)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    std::string name(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));

    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    backend->cluster->open_bucket(name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    if (auto ec = f.get()) {
        cb_raise_error_code(ec, fmt::format("unable open bucket \"{}\"", name));
    }

    return Qtrue;
}

static VALUE
cb_Backend_document_get(VALUE self, VALUE bucket, VALUE collection, VALUE id)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::get_request req{ doc_id };
    auto barrier = std::make_shared<std::promise<couchbase::operations::get_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::get_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable fetch {}", doc_id));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
    return res;
}

static VALUE
cb_Backend_document_get_and_lock(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE lock_time)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::get_and_lock_request req{ doc_id };
    req.lock_time = NUM2UINT(lock_time);

    auto barrier = std::make_shared<std::promise<couchbase::operations::get_and_lock_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::get_and_lock_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable lock and fetch {}", doc_id));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
    return res;
}

static VALUE
cb_Backend_document_get_and_touch(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE expiration)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(expiration, T_FIXNUM);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::get_and_touch_request req{ doc_id };
    req.expiration = NUM2UINT(expiration);

    auto barrier = std::make_shared<std::promise<couchbase::operations::get_and_touch_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::get_and_touch_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable fetch and touch {}", doc_id));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), rb_str_new(resp.value.data(), static_cast<long>(resp.value.size())));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
    return res;
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
cb_Backend_document_touch(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE expiration)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(expiration, T_FIXNUM);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::touch_request req{ doc_id };
    req.expiration = NUM2UINT(expiration);

    auto barrier = std::make_shared<std::promise<couchbase::operations::touch_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::touch_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to touch {}", doc_id));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    return res;
}

static VALUE
cb_Backend_document_exists(VALUE self, VALUE bucket, VALUE collection, VALUE id)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::exists_request req{ doc_id };

    auto barrier = std::make_shared<std::promise<couchbase::operations::exists_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::exists_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to exists {}", doc_id));
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
}

static VALUE
cb_Backend_document_unlock(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE cas)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::unlock_request req{ doc_id };
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
        cb_raise_error_code(resp.ec, fmt::format("unable to unlock {}", doc_id));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    return res;
}

static VALUE
cb_Backend_document_upsert(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));
    std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

    couchbase::operations::upsert_request req{ doc_id, value };
    req.flags = FIX2UINT(flags);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        VALUE expiration = rb_hash_aref(options, rb_id2sym(rb_intern("expiration")));
        if (!NIL_P(expiration)) {
            Check_Type(expiration, T_FIXNUM);
            req.expiration = FIX2UINT(expiration);
        }
    }

    auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::upsert_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to upsert {}", doc_id));
    }

    return cb__extract_mutation_result(resp);
}

static VALUE
cb_Backend_document_replace(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));
    std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

    couchbase::operations::replace_request req{ doc_id, value };
    req.flags = FIX2UINT(flags);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        VALUE expiration = rb_hash_aref(options, rb_id2sym(rb_intern("expiration")));
        if (!NIL_P(expiration)) {
            Check_Type(expiration, T_FIXNUM);
            req.expiration = FIX2UINT(expiration);
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
        cb_raise_error_code(resp.ec, fmt::format("unable to replace {}", doc_id));
    }

    return cb__extract_mutation_result(resp);
}

static VALUE
cb_Backend_document_insert(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));
    std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

    couchbase::operations::insert_request req{ doc_id, value };
    req.flags = FIX2UINT(flags);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        VALUE expiration = rb_hash_aref(options, rb_id2sym(rb_intern("expiration")));
        if (!NIL_P(expiration)) {
            Check_Type(expiration, T_FIXNUM);
            req.expiration = FIX2UINT(expiration);
        }
    }

    auto barrier = std::make_shared<std::promise<couchbase::operations::insert_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::insert_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to insert {}", doc_id));
    }

    return cb__extract_mutation_result(resp);
}

static VALUE
cb_Backend_document_remove(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::remove_request req{ doc_id };
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        cb_raise_error_code(resp.ec, fmt::format("unable to remove {}", doc_id));
    }
    return cb__extract_mutation_result(resp);
}

static VALUE
cb_Backend_document_increment(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::increment_request req{ doc_id };
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        VALUE expiration = rb_hash_aref(options, rb_id2sym(rb_intern("expiration")));
        if (!NIL_P(expiration)) {
            Check_Type(expiration, T_FIXNUM);
            req.expiration = FIX2UINT(expiration);
        }
    }

    auto barrier = std::make_shared<std::promise<couchbase::operations::increment_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::increment_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to increment {} by {}", doc_id, req.delta));
    }
    VALUE res = cb__extract_mutation_result(resp);
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
    return res;
}

static VALUE
cb_Backend_document_decrement(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE options)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::decrement_request req{ doc_id };
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        VALUE expiration = rb_hash_aref(options, rb_id2sym(rb_intern("expiration")));
        if (!NIL_P(expiration)) {
            Check_Type(expiration, T_FIXNUM);
            req.expiration = FIX2UINT(expiration);
        }
    }

    auto barrier = std::make_shared<std::promise<couchbase::operations::decrement_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute(req, [barrier](couchbase::operations::decrement_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to decrement {} by {}", doc_id, req.delta));
    }
    VALUE res = cb__extract_mutation_result(resp);
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
    return res;
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
    }
    return rb_id2sym(rb_intern("unknown"));
}

static VALUE
cb__map_subdoc_status(couchbase::protocol::status status)
{
    switch (status) {
        case couchbase::protocol::status::success:
            return rb_id2sym(rb_intern("success"));

        case couchbase::protocol::status::subdoc_path_mismatch:
            return rb_id2sym(rb_intern("path_mismatch"));

        case couchbase::protocol::status::subdoc_path_invalid:
            return rb_id2sym(rb_intern("path_invalid"));

        case couchbase::protocol::status::subdoc_path_too_big:
            return rb_id2sym(rb_intern("path_too_big"));

        case couchbase::protocol::status::subdoc_value_cannot_insert:
            return rb_id2sym(rb_intern("value_cannot_insert"));

        case couchbase::protocol::status::subdoc_doc_not_json:
            return rb_id2sym(rb_intern("doc_not_json"));

        case couchbase::protocol::status::subdoc_num_range_error:
            return rb_id2sym(rb_intern("num_range"));

        case couchbase::protocol::status::subdoc_delta_invalid:
            return rb_id2sym(rb_intern("delta_invalid"));

        case couchbase::protocol::status::subdoc_path_exists:
            return rb_id2sym(rb_intern("path_exists"));

        case couchbase::protocol::status::subdoc_value_too_deep:
            return rb_id2sym(rb_intern("value_too_deep"));

        case couchbase::protocol::status::subdoc_invalid_combo:
            return rb_id2sym(rb_intern("invalid_combo"));

        case couchbase::protocol::status::subdoc_xattr_invalid_flag_combo:
            return rb_id2sym(rb_intern("xattr_invalid_flag_combo"));

        case couchbase::protocol::status::subdoc_xattr_invalid_key_combo:
            return rb_id2sym(rb_intern("xattr_invalid_key_combo"));

        case couchbase::protocol::status::subdoc_xattr_unknown_macro:
            return rb_id2sym(rb_intern("xattr_unknown_macro"));

        case couchbase::protocol::status::subdoc_xattr_unknown_vattr:
            return rb_id2sym(rb_intern("xattr_unknown_vattr"));

        case couchbase::protocol::status::subdoc_xattr_cannot_modify_vattr:
            return rb_id2sym(rb_intern("xattr_cannot_modify_vattr"));

        default:
            return rb_id2sym(rb_intern("unknown"));
    }
}

static VALUE
cb_Backend_document_lookup_in(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE access_deleted, VALUE specs)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::lookup_in_request req{ doc_id };
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
        if (operation_id == rb_intern("get") || operation_id == rb_intern("get_doc")) {
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
        cb_raise_error_code(resp.ec, fmt::format("unable fetch {}", doc_id));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), ULL2NUM(resp.cas));
    VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
    rb_hash_aset(res, rb_id2sym(rb_intern("fields")), fields);
    for (size_t i = 0; i < resp.fields.size(); ++i) {
        VALUE entry = rb_hash_new();
        rb_hash_aset(entry, rb_id2sym(rb_intern("exists")), resp.fields[i].exists ? Qtrue : Qfalse);
        rb_hash_aset(
          entry, rb_id2sym(rb_intern("path")), rb_str_new(resp.fields[i].path.data(), static_cast<long>(resp.fields[i].path.size())));
        rb_hash_aset(
          entry, rb_id2sym(rb_intern("value")), rb_str_new(resp.fields[i].value.data(), static_cast<long>(resp.fields[i].value.size())));
        rb_hash_aset(entry, rb_id2sym(rb_intern("status")), cb__map_subdoc_status(resp.fields[i].status));
        if (resp.fields[i].opcode == couchbase::protocol::subdoc_opcode::get && resp.fields[i].path.empty()) {
            rb_hash_aset(entry, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("get_doc")));
        } else {
            rb_hash_aset(entry, rb_id2sym(rb_intern("type")), cb__map_subdoc_opcode(resp.fields[i].opcode));
        }
        rb_ary_store(fields, static_cast<long>(i), entry);
    }
    return res;
}

static VALUE
cb_Backend_document_mutate_in(VALUE self, VALUE bucket, VALUE collection, VALUE id, VALUE access_deleted, VALUE specs, VALUE options)
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

    couchbase::document_id doc_id;
    doc_id.bucket.assign(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));
    doc_id.collection.assign(RSTRING_PTR(collection), static_cast<size_t>(RSTRING_LEN(collection)));
    doc_id.key.assign(RSTRING_PTR(id), static_cast<size_t>(RSTRING_LEN(id)));

    couchbase::operations::mutate_in_request req{ doc_id };
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE durability_level = rb_hash_aref(options, rb_id2sym(rb_intern("durability_level")));
        if (!NIL_P(durability_level)) {
            Check_Type(durability_level, T_SYMBOL);
            ID level = rb_sym2id(durability_level);
            if (level == rb_intern("none")) {
                req.durability_level = couchbase::protocol::durability_level::none;
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
        } else {
            rb_raise(rb_eArgError, "Unsupported operation for subdocument mutation: %+" PRIsVALUE, operation);
        }
        bool xattr = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("xattr"))));
        bool create_parents = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("create_parents"))));
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
                               create_parents,
                               expand_macros,
                               std::string(RSTRING_PTR(path), static_cast<size_t>(RSTRING_LEN(path))),
                               FIX2LONG(param));
        } else {
            Check_Type(param, T_STRING);
            req.specs.add_spec(opcode,
                               xattr,
                               create_parents,
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
        cb_raise_error_code(resp.ec, fmt::format("unable to mutate {}", doc_id));
    }

    VALUE res = cb__extract_mutation_result(resp);
    if (resp.first_error_index) {
        rb_hash_aset(res, rb_id2sym(rb_intern("first_error_index")), ULONG2NUM(resp.first_error_index.value()));
    }
    VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
    rb_hash_aset(res, rb_id2sym(rb_intern("fields")), fields);
    for (size_t i = 0; i < resp.fields.size(); ++i) {
        VALUE entry = rb_hash_new();
        rb_hash_aset(
          entry, rb_id2sym(rb_intern("path")), rb_str_new(resp.fields[i].path.data(), static_cast<long>(resp.fields[i].path.size())));
        if (resp.fields[i].opcode == couchbase::protocol::subdoc_opcode::counter) {
            rb_hash_aset(entry, rb_id2sym(rb_intern("value")), LONG2NUM(std::stoll(resp.fields[i].value)));
        } else {
            rb_hash_aset(entry,
                         rb_id2sym(rb_intern("value")),
                         rb_str_new(resp.fields[i].value.data(), static_cast<long>(resp.fields[i].value.size())));
        }
        rb_hash_aset(entry, rb_id2sym(rb_intern("status")), cb__map_subdoc_status(resp.fields[i].status));
        rb_hash_aset(entry, rb_id2sym(rb_intern("type")), cb__map_subdoc_opcode(resp.fields[i].opcode));
        rb_ary_store(fields, static_cast<long>(i), entry);
    }
    return res;
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

    couchbase::operations::query_request req;
    req.statement.assign(RSTRING_PTR(statement), static_cast<size_t>(RSTRING_LEN(statement)));
    VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
    if (!NIL_P(client_context_id)) {
        Check_Type(client_context_id, T_STRING);
        req.client_context_id.assign(RSTRING_PTR(client_context_id), static_cast<size_t>(RSTRING_LEN(client_context_id)));
    }
    VALUE timeout = rb_hash_aref(options, rb_id2sym(rb_intern("timeout")));
    if (!NIL_P(timeout)) {
        switch (TYPE(timeout)) {
            case T_FIXNUM:
            case T_BIGNUM:
                break;
            default:
                rb_raise(rb_eArgError, "timeout must be an Integer");
        }
        req.timeout = NUM2ULL(timeout);
    }
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
            cb_raise_error_code(resp.ec,
                                fmt::format("unable to query: \"{}{}\" ({}: {})",
                                            req.statement.substr(0, 50),
                                            req.statement.size() > 50 ? "..." : "",
                                            first_error.code,
                                            first_error.message));
        } else {
            cb_raise_error_code(
              resp.ec, fmt::format("unable to query: \"{}{}\"", req.statement.substr(0, 50), req.statement.size() > 50 ? "..." : ""));
        }
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
cb_Backend_bucket_create(VALUE self, VALUE bucket_settings)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_settings, T_HASH);
    couchbase::operations::bucket_create_request req{};
    cb__generate_bucket_settings(bucket_settings, req.bucket, true);
    auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_create_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::bucket_create_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec,
                            fmt::format("unable to create bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
    }

    return Qtrue;
}

static VALUE
cb_Backend_bucket_update(VALUE self, VALUE bucket_settings)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_settings, T_HASH);
    couchbase::operations::bucket_update_request req{};
    cb__generate_bucket_settings(bucket_settings, req.bucket, false);
    auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_update_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::bucket_update_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec,
                            fmt::format("unable to update bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
    }

    return Qtrue;
}

static VALUE
cb_Backend_bucket_drop(VALUE self, VALUE bucket_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    couchbase::operations::bucket_drop_request req{};
    req.name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_drop_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req, [barrier](couchbase::operations::bucket_drop_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
    }

    return Qtrue;
}

static VALUE
cb_Backend_bucket_flush(VALUE self, VALUE bucket_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    couchbase::operations::bucket_flush_request req{};
    req.name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_flush_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req, [barrier](couchbase::operations::bucket_flush_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
    }

    return Qtrue;
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
cb_Backend_bucket_get_all(VALUE self)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    couchbase::operations::bucket_get_all_request req{};
    auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_get_all_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::bucket_get_all_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, "unable to get list of the buckets of the cluster");
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(resp.buckets.size()));
    for (const auto& entry : resp.buckets) {
        VALUE bucket = rb_hash_new();
        cb__extract_bucket_settings(entry, bucket);
        rb_ary_push(res, bucket);
    }

    return res;
}

static VALUE
cb_Backend_bucket_get(VALUE self, VALUE bucket_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    couchbase::operations::bucket_get_request req{};
    req.name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::bucket_get_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req, [barrier](couchbase::operations::bucket_get_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to locate bucket \"{}\" on the cluster", req.name));
    }

    VALUE res = rb_hash_new();
    cb__extract_bucket_settings(resp.bucket, res);

    return res;
}

static VALUE
cb_Backend_cluster_enable_developer_preview(VALUE self)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    couchbase::operations::cluster_developer_preview_enable_request req{};
    auto barrier = std::make_shared<std::promise<couchbase::operations::cluster_developer_preview_enable_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(
      req, [barrier](couchbase::operations::cluster_developer_preview_enable_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to enable developer preview for this cluster"));
    }
    spdlog::critical("Developer preview cannot be disabled once it is enabled. If you enter developer preview mode you will not be able to "
                     "upgrade. DO NOT USE IN PRODUCTION.");
    return Qtrue;
}

static VALUE
cb_Backend_scope_get_all(VALUE self, VALUE bucket_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    couchbase::operations::scope_get_all_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::scope_get_all_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::scope_get_all_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to get list of the scopes of the bucket \"{}\"", req.bucket_name));
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
}

static VALUE
cb_Backend_scope_create(VALUE self, VALUE bucket_name, VALUE scope_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);

    couchbase::operations::scope_create_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::scope_create_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req, [barrier](couchbase::operations::scope_create_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to create the scope on the bucket \"{}\"", req.bucket_name));
    }
    return ULL2NUM(resp.uid);
}

static VALUE
cb_Backend_scope_drop(VALUE self, VALUE bucket_name, VALUE scope_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);

    couchbase::operations::scope_drop_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::scope_drop_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req, [barrier](couchbase::operations::scope_drop_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to drop the scope \"{}\" on the bucket \"{}\"", req.scope_name, req.bucket_name));
    }
    return ULL2NUM(resp.uid);
}

static VALUE
cb_Backend_collection_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE max_expiry)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);

    couchbase::operations::collection_create_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
    req.collection_name.assign(RSTRING_PTR(collection_name), static_cast<size_t>(RSTRING_LEN(collection_name)));

    if (!NIL_P(max_expiry)) {
        Check_Type(max_expiry, T_FIXNUM);
        req.max_expiry = FIX2UINT(max_expiry);
    }
    auto barrier = std::make_shared<std::promise<couchbase::operations::collection_create_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::collection_create_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to create the collection on the bucket \"{}\"", req.bucket_name));
    }
    return ULL2NUM(resp.uid);
}

static VALUE
cb_Backend_collection_drop(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);

    couchbase::operations::collection_drop_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    req.scope_name.assign(RSTRING_PTR(scope_name), static_cast<size_t>(RSTRING_LEN(scope_name)));
    req.collection_name.assign(RSTRING_PTR(collection_name), static_cast<size_t>(RSTRING_LEN(collection_name)));

    auto barrier = std::make_shared<std::promise<couchbase::operations::collection_drop_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::collection_drop_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(
          resp.ec,
          fmt::format(
            R"(unable to drop the collection  "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
    }
    return ULL2NUM(resp.uid);
}

static VALUE
cb_Backend_query_index_get_all(VALUE self, VALUE bucket_name)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    couchbase::operations::query_index_get_all_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_get_all_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(
      req, [barrier](couchbase::operations::query_index_get_all_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        cb_raise_error_code(resp.ec, fmt::format("unable to get list of the indexes of the bucket \"{}\"", req.bucket_name));
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
}

static VALUE
cb_Backend_query_index_create(VALUE self, VALUE bucket_name, VALUE index_name, VALUE fields, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_name, T_STRING);
    Check_Type(fields, T_ARRAY);

    couchbase::operations::query_index_create_request req{};
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
            cb_raise_error_code(resp.ec,
                                fmt::format(R"(unable to create index "{}" on the bucket "{}" ({}: {}))",
                                            req.index_name,
                                            req.bucket_name,
                                            first_error.code,
                                            first_error.message));
        } else {
            cb_raise_error_code(resp.ec, fmt::format(R"(unable to create index "{}" on the bucket "{}")", req.index_name, req.bucket_name));
        }
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
}

static VALUE
cb_Backend_query_index_drop(VALUE self, VALUE bucket_name, VALUE index_name, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_name, T_STRING);

    couchbase::operations::query_index_drop_request req{};
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
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::query_index_drop_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        if (!resp.errors.empty()) {
            const auto& first_error = resp.errors.front();
            cb_raise_error_code(resp.ec,
                                fmt::format(R"(unable to drop index "{}" on the bucket "{}" ({}: {}))",
                                            req.index_name,
                                            req.bucket_name,
                                            first_error.code,
                                            first_error.message));
        } else {
            cb_raise_error_code(resp.ec, fmt::format(R"(unable to drop index "{}" on the bucket "{}")", req.index_name, req.bucket_name));
        }
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
}

static VALUE
cb_Backend_query_index_create_primary(VALUE self, VALUE bucket_name, VALUE options)
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

    couchbase::operations::query_index_create_request req{};
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
            cb_raise_error_code(
              resp.ec,
              fmt::format(
                R"(unable to create primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));
        } else {
            cb_raise_error_code(resp.ec,
                                fmt::format(R"(unable to create primary index on the bucket "{}")", req.index_name, req.bucket_name));
        }
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
}

static VALUE
cb_Backend_query_index_drop_primary(VALUE self, VALUE bucket_name, VALUE options)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }

    Check_Type(bucket_name, T_STRING);

    couchbase::operations::query_index_drop_request req{};
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
    backend->cluster->execute_http(req,
                                   [barrier](couchbase::operations::query_index_drop_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        if (!resp.errors.empty()) {
            const auto& first_error = resp.errors.front();
            cb_raise_error_code(
              resp.ec,
              fmt::format(
                R"(unable to drop primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));
        } else {
            cb_raise_error_code(resp.ec, fmt::format(R"(unable to drop primary index on the bucket "{}")", req.bucket_name));
        }
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
}

static VALUE
cb_Backend_query_index_build_deferred(VALUE self, VALUE bucket_name, VALUE options)
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

    couchbase::operations::query_index_build_deferred_request req{};
    req.bucket_name.assign(RSTRING_PTR(bucket_name), static_cast<size_t>(RSTRING_LEN(bucket_name)));
    auto barrier = std::make_shared<std::promise<couchbase::operations::query_index_build_deferred_response>>();
    auto f = barrier->get_future();
    backend->cluster->execute_http(
      req, [barrier](couchbase::operations::query_index_build_deferred_response resp) mutable { barrier->set_value(resp); });
    auto resp = f.get();
    if (resp.ec) {
        if (!resp.errors.empty()) {
            const auto& first_error = resp.errors.front();
            cb_raise_error_code(
              resp.ec,
              fmt::format(
                R"(unable to drop primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));

        } else {
            cb_raise_error_code(resp.ec,
                                fmt::format("unable to trigger build for deferred indexes for the bucket \"{}\"", req.bucket_name));
        }
    }
    return Qtrue;
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
init_backend(VALUE mCouchbase)
{
    VALUE cBackend = rb_define_class_under(mCouchbase, "Backend", rb_cBasicObject);
    rb_define_alloc_func(cBackend, cb_Backend_allocate);
    rb_define_method(cBackend, "open", VALUE_FUNC(cb_Backend_open), 3);
    rb_define_method(cBackend, "close", VALUE_FUNC(cb_Backend_close), 0);
    rb_define_method(cBackend, "open_bucket", VALUE_FUNC(cb_Backend_open_bucket), 1);

    rb_define_method(cBackend, "document_get", VALUE_FUNC(cb_Backend_document_get), 3);
    rb_define_method(cBackend, "document_get_and_lock", VALUE_FUNC(cb_Backend_document_get_and_lock), 4);
    rb_define_method(cBackend, "document_get_and_touch", VALUE_FUNC(cb_Backend_document_get_and_touch), 4);
    rb_define_method(cBackend, "document_insert", VALUE_FUNC(cb_Backend_document_insert), 6);
    rb_define_method(cBackend, "document_replace", VALUE_FUNC(cb_Backend_document_replace), 6);
    rb_define_method(cBackend, "document_upsert", VALUE_FUNC(cb_Backend_document_upsert), 6);
    rb_define_method(cBackend, "document_remove", VALUE_FUNC(cb_Backend_document_remove), 4);
    rb_define_method(cBackend, "document_lookup_in", VALUE_FUNC(cb_Backend_document_lookup_in), 5);
    rb_define_method(cBackend, "document_mutate_in", VALUE_FUNC(cb_Backend_document_mutate_in), 6);
    rb_define_method(cBackend, "document_query", VALUE_FUNC(cb_Backend_document_query), 2);
    rb_define_method(cBackend, "document_touch", VALUE_FUNC(cb_Backend_document_touch), 4);
    rb_define_method(cBackend, "document_exists", VALUE_FUNC(cb_Backend_document_exists), 3);
    rb_define_method(cBackend, "document_unlock", VALUE_FUNC(cb_Backend_document_unlock), 4);
    rb_define_method(cBackend, "document_increment", VALUE_FUNC(cb_Backend_document_increment), 4);
    rb_define_method(cBackend, "document_decrement", VALUE_FUNC(cb_Backend_document_decrement), 4);

    rb_define_method(cBackend, "bucket_create", VALUE_FUNC(cb_Backend_bucket_create), 1);
    rb_define_method(cBackend, "bucket_update", VALUE_FUNC(cb_Backend_bucket_update), 1);
    rb_define_method(cBackend, "bucket_drop", VALUE_FUNC(cb_Backend_bucket_drop), 1);
    rb_define_method(cBackend, "bucket_flush", VALUE_FUNC(cb_Backend_bucket_flush), 1);
    rb_define_method(cBackend, "bucket_get_all", VALUE_FUNC(cb_Backend_bucket_get_all), 0);
    rb_define_method(cBackend, "bucket_get", VALUE_FUNC(cb_Backend_bucket_get), 1);

    rb_define_method(cBackend, "cluster_enable_developer_preview!", VALUE_FUNC(cb_Backend_cluster_enable_developer_preview), 0);

    rb_define_method(cBackend, "scope_get_all", VALUE_FUNC(cb_Backend_scope_get_all), 1);
    rb_define_method(cBackend, "scope_create", VALUE_FUNC(cb_Backend_scope_create), 2);
    rb_define_method(cBackend, "scope_drop", VALUE_FUNC(cb_Backend_scope_drop), 2);
    rb_define_method(cBackend, "collection_create", VALUE_FUNC(cb_Backend_collection_create), 4);
    rb_define_method(cBackend, "collection_drop", VALUE_FUNC(cb_Backend_collection_drop), 3);

    rb_define_method(cBackend, "query_index_get_all", VALUE_FUNC(cb_Backend_query_index_get_all), 1);
    rb_define_method(cBackend, "query_index_create", VALUE_FUNC(cb_Backend_query_index_create), 4);
    rb_define_method(cBackend, "query_index_create_primary", VALUE_FUNC(cb_Backend_query_index_create_primary), 2);
    rb_define_method(cBackend, "query_index_drop", VALUE_FUNC(cb_Backend_query_index_drop), 3);
    rb_define_method(cBackend, "query_index_drop_primary", VALUE_FUNC(cb_Backend_query_index_drop_primary), 2);
    rb_define_method(cBackend, "query_index_build_deferred", VALUE_FUNC(cb_Backend_query_index_build_deferred), 2);
    rb_define_method(cBackend, "query_index_watch", VALUE_FUNC(cb_Backend_query_index_watch), 4);
}

extern "C" {
void
Init_libcouchbase(void)
{
    auto env_val = spdlog::details::os::getenv("SPDLOG_LEVEL");
    if (env_val.empty()) {
        spdlog::set_level(spdlog::level::critical);
    } else {
        spdlog::cfg::load_env_levels();
    }
    spdlog::set_pattern("[%Y-%m-%d %T.%e] [%P,%t] [%^%l%$] %v");

    VALUE mCouchbase = rb_define_module("Couchbase");
    init_versions(mCouchbase);
    init_backend(mCouchbase);
    init_exceptions(mCouchbase);
}
}
