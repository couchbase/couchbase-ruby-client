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

#include <couchbase/meta/version.hxx>
#include "ext_build_info.hxx"
#include "ext_build_version.hxx"

#include <asio.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/cfg/env.h>

#include <snappy.h>

#include <couchbase/platform/terminate_handler.h>

#include <couchbase/cluster.hxx>
#include <couchbase/operations.hxx>
#include <couchbase/operations/management/analytics.hxx>
#include <couchbase/operations/management/bucket.hxx>
#include <couchbase/operations/management/collections.hxx>
#include <couchbase/operations/management/query.hxx>
#include <couchbase/operations/management/search.hxx>
#include <couchbase/operations/management/user.hxx>
#include <couchbase/operations/management/view.hxx>
#include <couchbase/operations/management/cluster_developer_preview_enable.hxx>

#include <couchbase/io/dns_client.hxx>
#include <couchbase/utils/connection_string.hxx>
#include <couchbase/utils/unsigned_leb128.hxx>

#include <ruby.h>
#if defined(HAVE_RUBY_VERSION_H)
#include <ruby/version.h>
#endif
#include <ruby/thread.h>

#if defined(RB_METHOD_DEFINITION_DECL) || RUBY_API_VERSION_MAJOR == 3
#define VALUE_FUNC(f) f
#define INT_FUNC(f) f
#else
#define VALUE_FUNC(f) reinterpret_cast<VALUE (*)(ANYARGS)>(f)
#define INT_FUNC(f) reinterpret_cast<int (*)(ANYARGS)>(f)
#endif

static VALUE
cb_displaying_class_of(VALUE x)
{
    switch (x) {
        case Qfalse:
            return rb_str_new_cstr("false");
        case Qnil:
            return rb_str_new_cstr("nil");
        case Qtrue:
            return rb_str_new_cstr("true");
        default:
            return rb_obj_class(x);
    }
}

static const char*
cb_builtin_type_name(int type)
{
    switch (type) {
        case RUBY_T_OBJECT:
            return "Object";
        case RUBY_T_CLASS:
            return "Class";
        case RUBY_T_MODULE:
            return "Module";
        case RUBY_T_FLOAT:
            return "Float";
        case RUBY_T_STRING:
            return "String";
        case RUBY_T_REGEXP:
            return "Regexp";
        case RUBY_T_ARRAY:
            return "Array";
        case RUBY_T_HASH:
            return "Hash";
        case RUBY_T_STRUCT:
            return "Struct";
        case RUBY_T_BIGNUM:
            return "Integer";
        case RUBY_T_FILE:
            return "File";
        case RUBY_T_DATA:
            return "Data";
        case RUBY_T_MATCH:
            return "MatchData";
        case RUBY_T_COMPLEX:
            return "Complex";
        case RUBY_T_RATIONAL:
            return "Rational";
        case RUBY_T_NIL:
            return "nil";
        case RUBY_T_TRUE:
            return "true";
        case RUBY_T_FALSE:
            return "false";
        case RUBY_T_SYMBOL:
            return "Symbol";
        case RUBY_T_FIXNUM:
            return "Integer";
        default:
            break;
    }
    return "unknown or system-reserved type";
}

class ruby_exception : public std::exception
{
  public:
    explicit ruby_exception(VALUE exc)
      : exc_{ exc }
    {
    }

    ruby_exception(VALUE exc_type, VALUE exc_message)
      : exc_{ rb_exc_new_str(exc_type, exc_message) }
    {
    }

    ruby_exception(VALUE exc_type, const std::string& exc_message)
      : exc_{ rb_exc_new_cstr(exc_type, exc_message.c_str()) }
    {
    }

    [[nodiscard]] VALUE exception_object() const
    {
        return exc_;
    }

  private:
    VALUE exc_;
};
/*
 * Destructor-friendly rb_check_type from error.c
 *
 * Returns exception instead of raising it.
 */
static inline void
cb_check_type(VALUE object, int type)
{
    if (RB_UNLIKELY(object == Qundef)) {
        rb_bug("undef leaked to the Ruby space");
    }

    if (auto object_type = TYPE(object); object_type != type || (object_type == T_DATA && RTYPEDDATA_P(object))) {
        throw ruby_exception(
          rb_eTypeError,
          rb_sprintf("wrong argument type %" PRIsVALUE " (expected %s)", cb_displaying_class_of(object), cb_builtin_type_name(type)));
    }
}

static inline std::string
cb_string_new(VALUE str)
{
    return { RSTRING_PTR(str), static_cast<std::size_t>(RSTRING_LEN(str)) };
}

static inline VALUE
cb_str_new(const std::string& str)
{
    return rb_external_str_new(str.data(), static_cast<long>(str.size()));
}

static inline VALUE
cb_str_new(const std::optional<std::string>& str)
{
    if (str) {
        return rb_external_str_new(str->data(), static_cast<long>(str->size()));
    }
    return Qnil;
}

static inline VALUE
cb_cas_to_num(const couchbase::protocol::cas& cas)
{
    return ULL2NUM(cas.value);
}

static inline couchbase::protocol::cas
cb_num_to_cas(VALUE num)
{
    return { NUM2ULL(num) };
}

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

#if defined(HAVE_RUBY_VERSION_H)
    rb_hash_aset(
      cb_Version,
      rb_id2sym(rb_intern("ruby_abi")),
      rb_str_freeze(cb_str_new(fmt::format("{}.{}.{}", RUBY_API_VERSION_MAJOR, RUBY_API_VERSION_MINOR, RUBY_API_VERSION_TEENY))));
#endif
    rb_hash_aset(cb_Version, rb_id2sym(rb_intern("revision")), rb_str_freeze(rb_str_new_cstr(EXT_GIT_REVISION)));

    VALUE version_info = rb_inspect(cb_Version);
    spdlog::debug("couchbase backend has been initialized: {}",
                  std::string_view(RSTRING_PTR(version_info), static_cast<std::size_t>(RSTRING_LEN(version_info))));

    VALUE cb_BuildInfo = rb_hash_new();
    rb_const_set(mCouchbase, rb_intern("BUILD_INFO"), cb_BuildInfo);
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("ruby_library")), rb_str_freeze(rb_str_new_cstr(RUBY_LIBRARY)));
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("ruby_include_dir")), rb_str_freeze(rb_str_new_cstr(RUBY_INCLUDE_DIR)));
    VALUE cb_CoreInfo = rb_hash_new();
    for (const auto& [name, value] : couchbase::meta::sdk_build_info()) {
        if (name == "version_major" || name == "version_minor" || name == "version_patch" || name == "version_build") {
            rb_hash_aset(cb_CoreInfo, rb_id2sym(rb_intern(name.c_str())), INT2FIX(std::stoi(value)));
        } else if (name == "snapshot" || name == "static_stdlib" || name == "static_openssl") {
            rb_hash_aset(cb_CoreInfo, rb_id2sym(rb_intern(name.c_str())), value == "true" ? Qtrue : Qfalse);
        } else {
            rb_hash_aset(cb_CoreInfo, rb_id2sym(rb_intern(name.c_str())), rb_str_freeze(rb_str_new_cstr(value.c_str())));
        }
    }
    rb_hash_aset(cb_BuildInfo, rb_id2sym(rb_intern("cxx_client")), cb_CoreInfo);
    VALUE build_info = rb_inspect(cb_BuildInfo);
    spdlog::debug("couchbase backend build info: {}",
                  std::string_view(RSTRING_PTR(build_info), static_cast<std::size_t>(RSTRING_LEN(build_info))));
}

struct cb_backend_data {
    std::unique_ptr<asio::io_context> ctx;
    std::unique_ptr<couchbase::cluster> cluster;
    std::thread worker;
};

static void
cb_backend_close(cb_backend_data* backend)
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
    /* no embeded ruby objects -- no mark */
}

static void
cb_Backend_free(void* ptr)
{
    auto* backend = static_cast<cb_backend_data*>(ptr);
    cb_backend_close(backend);
    ruby_xfree(backend);
}

static size_t
cb_Backend_memsize(const void* ptr)
{
    const auto* backend = static_cast<const cb_backend_data*>(ptr);
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

static inline const std::unique_ptr<couchbase::cluster>&
cb_backend_to_cluster(VALUE self)
{
    const cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(rb_eArgError, "Cluster has been closed already");
    }
    return backend->cluster;
}

static VALUE eAmbiguousTimeout;
static VALUE eAuthenticationFailure;
static VALUE eBucketExists;
static VALUE eBucketNotFlushable;
static VALUE eBucketNotFound;
static VALUE eCasMismatch;
static VALUE eCollectionExists;
static VALUE eCollectionNotFound;
static VALUE eCompilationFailure;
static VALUE eConsistencyMismatch;
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
static VALUE eIndexNotReady;
static VALUE eInternalServerFailure;
static VALUE eInvalidArgument;
static VALUE eJobQueueFull;
static VALUE eLinkNotFound;
static VALUE eLinkExists;
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
static VALUE eRateLimited;
static VALUE eQuotaLimited;
static VALUE eXattrNoAccess;
static VALUE eCannotReviveLivingDocument;
static VALUE eDmlFailure;

static VALUE eBackendError;
static VALUE eNetworkError;
static VALUE eResolveFailure;
static VALUE eNoEndpointsLeft;
static VALUE eHandshakeFailure;
static VALUE eProtocolError;
static VALUE eConfigurationNotAvailable;

static void
init_exceptions(VALUE mCouchbase)
{
    VALUE mError = rb_define_module_under(mCouchbase, "Error");
    VALUE eCouchbaseError = rb_define_class_under(mError, "CouchbaseError", rb_eStandardError);

    VALUE eTimeout = rb_define_class_under(mError, "Timeout", eCouchbaseError);

    eAmbiguousTimeout = rb_define_class_under(mError, "AmbiguousTimeout", eTimeout);
    eAuthenticationFailure = rb_define_class_under(mError, "AuthenticationFailure", eCouchbaseError);
    eBucketExists = rb_define_class_under(mError, "BucketExists", eCouchbaseError);
    eBucketNotFlushable = rb_define_class_under(mError, "BucketNotFlushable", eCouchbaseError);
    eBucketNotFound = rb_define_class_under(mError, "BucketNotFound", eCouchbaseError);
    eCasMismatch = rb_define_class_under(mError, "CasMismatch", eCouchbaseError);
    eCollectionExists = rb_define_class_under(mError, "CollectionExists", eCouchbaseError);
    eCollectionNotFound = rb_define_class_under(mError, "CollectionNotFound", eCouchbaseError);
    eCompilationFailure = rb_define_class_under(mError, "CompilationFailure", eCouchbaseError);
    eConsistencyMismatch = rb_define_class_under(mError, "ConsistencyMismatch", eCouchbaseError);
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
    eIndexNotReady = rb_define_class_under(mError, "IndexNotReady", eCouchbaseError);
    eInternalServerFailure = rb_define_class_under(mError, "InternalServerFailure", eCouchbaseError);
    eInvalidArgument = rb_define_class_under(mError, "InvalidArgument", rb_eArgError);
    eJobQueueFull = rb_define_class_under(mError, "JobQueueFull", eCouchbaseError);
    eLinkNotFound = rb_define_class_under(mError, "LinkNotFound", eCouchbaseError);
    eLinkExists = rb_define_class_under(mError, "LinkExists", eCouchbaseError);
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
    eUnambiguousTimeout = rb_define_class_under(mError, "UnambiguousTimeout", eTimeout);
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
    eRateLimited = rb_define_class_under(mError, "RateLimited", eCouchbaseError);
    eQuotaLimited = rb_define_class_under(mError, "QuotaLimited", eCouchbaseError);
    eXattrNoAccess = rb_define_class_under(mError, "XattrNoAccess", eCouchbaseError);
    eCannotReviveLivingDocument = rb_define_class_under(mError, "CannotReviveLivingDocument", eCouchbaseError);
    eDmlFailure = rb_define_class_under(mError, "DmlFailure", eCouchbaseError);

    eBackendError = rb_define_class_under(mError, "BackendError", eCouchbaseError);
    eNetworkError = rb_define_class_under(mError, "NetworkError", eBackendError);
    eResolveFailure = rb_define_class_under(mError, "ResolveFailure", eNetworkError);
    eNoEndpointsLeft = rb_define_class_under(mError, "NoEndpointsLeft", eNetworkError);
    eHandshakeFailure = rb_define_class_under(mError, "HandshakeFailure", eNetworkError);
    eProtocolError = rb_define_class_under(mError, "ProtocolError", eNetworkError);
    eConfigurationNotAvailable = rb_define_class_under(mError, "ConfigurationNotAvailable", eNetworkError);
}

[[nodiscard]] static VALUE
cb_map_error_code(std::error_code ec, const std::string& message)
{
    if (ec.category() == couchbase::error::detail::get_common_category()) {
        switch (couchbase::error::common_errc(ec.value())) {
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

            case couchbase::error::common_errc::rate_limited:
                return rb_exc_new_cstr(eRateLimited, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::common_errc::quota_limited:
                return rb_exc_new_cstr(eQuotaLimited, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_key_value_category()) {
        switch (couchbase::error::key_value_errc(ec.value())) {
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

            case couchbase::error::key_value_errc::xattr_no_access:
                return rb_exc_new_cstr(eXattrNoAccess, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::key_value_errc::cannot_revive_living_document:
                return rb_exc_new_cstr(eCannotReviveLivingDocument, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_query_category()) {
        switch (couchbase::error::query_errc(ec.value())) {
            case couchbase::error::query_errc::planning_failure:
                return rb_exc_new_cstr(ePlanningFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::query_errc::index_failure:
                return rb_exc_new_cstr(eIndexFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::query_errc::prepared_statement_failure:
                return rb_exc_new_cstr(ePreparedStatementFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::query_errc::dml_failure:
                return rb_exc_new_cstr(eDmlFailure, fmt::format("{}: {}", message, ec.message()).c_str());
                break;
        }
    } else if (ec.category() == couchbase::error::detail::get_search_category()) {
        switch (couchbase::error::search_errc(ec.value())) {
            case couchbase::error::search_errc::index_not_ready:
                return rb_exc_new_cstr(eIndexNotReady, fmt::format("{}: {}", message, ec.message()).c_str());
            case couchbase::error::search_errc::consistency_mismatch:
                return rb_exc_new_cstr(eConsistencyMismatch, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_view_category()) {
        switch (couchbase::error::view_errc(ec.value())) {
            case couchbase::error::view_errc::view_not_found:
                return rb_exc_new_cstr(eViewNotFound, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::view_errc::design_document_not_found:
                return rb_exc_new_cstr(eDesignDocumentNotFound, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_analytics_category()) {
        switch (couchbase::error::analytics_errc(ec.value())) {
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

            case couchbase::error::analytics_errc::link_exists:
                return rb_exc_new_cstr(eLinkExists, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    } else if (ec.category() == couchbase::error::detail::get_management_category()) {
        switch (couchbase::error::management_errc(ec.value())) {
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
    } else if (ec.category() == couchbase::error::detail::network_error_category()) {
        switch (couchbase::error::network_errc(ec.value())) {
            case couchbase::error::network_errc::resolve_failure:
                return rb_exc_new_cstr(eResolveFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::network_errc::no_endpoints_left:
                return rb_exc_new_cstr(eNoEndpointsLeft, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::network_errc::handshake_failure:
                return rb_exc_new_cstr(eHandshakeFailure, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::network_errc::protocol_error:
                return rb_exc_new_cstr(eProtocolError, fmt::format("{}: {}", message, ec.message()).c_str());

            case couchbase::error::network_errc::configuration_not_available:
                return rb_exc_new_cstr(eConfigurationNotAvailable, fmt::format("{}: {}", message, ec.message()).c_str());
        }
    }

    return rb_exc_new_cstr(eBackendError, fmt::format("{}: {}", message, ec.message()).c_str());
}

[[noreturn]] static void
cb_throw_error_code(std::error_code ec, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ec, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::error_context::key_value& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("id")), cb_str_new(ctx.id.key()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("scope")), cb_str_new(ctx.id.scope()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("collection")), cb_str_new(ctx.id.collection()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("bucket")), cb_str_new(ctx.id.bucket()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("opaque")), ULONG2NUM(ctx.opaque));
    if (ctx.status_code) {
        std::string status(fmt::format("{}", ctx.status_code.value()));
        rb_hash_aset(error_context, rb_id2sym(rb_intern("status")), cb_str_new(status));
    }
    if (ctx.error_map_info) {
        VALUE error_map_info = rb_hash_new();
        rb_hash_aset(error_map_info, rb_id2sym(rb_intern("name")), cb_str_new(ctx.error_map_info->name));
        rb_hash_aset(error_map_info, rb_id2sym(rb_intern("desc")), cb_str_new(ctx.error_map_info->description));
        rb_hash_aset(error_context, rb_id2sym(rb_intern("error_map_info")), error_map_info);
    }
    if (ctx.enhanced_error_info) {
        VALUE enhanced_error_info = rb_hash_new();
        rb_hash_aset(enhanced_error_info, rb_id2sym(rb_intern("reference")), cb_str_new(ctx.enhanced_error_info->reference));
        rb_hash_aset(enhanced_error_info, rb_id2sym(rb_intern("context")), cb_str_new(ctx.enhanced_error_info->context));
        rb_hash_aset(error_context, rb_id2sym(rb_intern("extended_error_info")), enhanced_error_info);
    }
    if (ctx.retry_attempts > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts));
        if (!ctx.retry_reasons.empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons.size()));
            for (const auto& reason : ctx.retry_reasons) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to.value()));
    }
    if (ctx.last_dispatched_from) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from.value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::error_context::key_value& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::error_context::query& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("statement")), cb_str_new(ctx.statement));
    if (ctx.parameters) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("parameters")), cb_str_new(ctx.parameters.value()));
    }
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
    if (ctx.retry_attempts > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts));
        if (!ctx.retry_reasons.empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons.size()));
            for (const auto& reason : ctx.retry_reasons) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to.value()));
    }
    if (ctx.last_dispatched_from) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from.value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::error_context::query& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::error_context::analytics& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("statement")), cb_str_new(ctx.statement));
    if (ctx.parameters) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("parameters")), cb_str_new(ctx.parameters.value()));
    }
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
    if (ctx.retry_attempts > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts));
        if (!ctx.retry_reasons.empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons.size()));
            for (const auto& reason : ctx.retry_reasons) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to.value()));
    }
    if (ctx.last_dispatched_from) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from.value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::error_context::analytics& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::error_context::view& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("design_document_name")), cb_str_new(ctx.design_document_name));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("view_name")), cb_str_new(ctx.view_name));
    if (!ctx.query_string.empty()) {
        VALUE parameters = rb_ary_new_capa(static_cast<long>(ctx.query_string.size()));
        for (const auto& param : ctx.query_string) {
            rb_ary_push(parameters, cb_str_new(param));
        }
        rb_hash_aset(error_context, rb_id2sym(rb_intern("parameters")), parameters);
    }
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
    if (ctx.retry_attempts > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts));
        if (!ctx.retry_reasons.empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons.size()));
            for (const auto& reason : ctx.retry_reasons) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to.value()));
    }
    if (ctx.last_dispatched_from) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from.value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::error_context::view& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::error_context::http& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("method")), cb_str_new(ctx.method));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("path")), cb_str_new(ctx.path));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
    if (ctx.retry_attempts > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts));
        if (!ctx.retry_reasons.empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons.size()));
            for (const auto& reason : ctx.retry_reasons) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to.value()));
    }
    if (ctx.last_dispatched_from) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from.value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::error_context::http& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::error_context::search& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("index_name")), cb_str_new(ctx.index_name));
    if (ctx.query) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("query")), cb_str_new(ctx.query.value()));
    }
    if (ctx.parameters) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("parameters")), cb_str_new(ctx.parameters.value()));
    }
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
    if (ctx.retry_attempts > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts));
        if (!ctx.retry_reasons.empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons.size()));
            for (const auto& reason : ctx.retry_reasons) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to.value()));
    }
    if (ctx.last_dispatched_from) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from.value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::error_context::search& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

template<typename Future>
static auto
cb_wait_for_future(Future&& f) -> decltype(f.get())
{
    struct arg_pack {
        Future&& f;
        decltype(f.get()) res{};
    } arg{ std::forward<Future>(f) };
    rb_thread_call_without_gvl(
      [](void* param) -> void* {
          auto* pack = static_cast<arg_pack*>(param);
          pack->res = std::move(pack->f.get());
          return nullptr;
      },
      &arg,
      nullptr,
      nullptr);
    return std::move(arg.res);
}

template<typename Request>
static void
cb_extract_timeout(Request& req, VALUE options)
{
    if (!NIL_P(options)) {
        switch (TYPE(options)) {
            case T_HASH:
                return cb_extract_timeout(req, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
            case T_FIXNUM:
            case T_BIGNUM:
                req.timeout = std::chrono::milliseconds(NUM2ULL(options));
                break;
            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("timeout must be an Integer, but given %+" PRIsVALUE, options));
        }
    }
}

static void
cb_extract_timeout(std::chrono::milliseconds& timeout, VALUE options)
{
    if (!NIL_P(options)) {
        switch (TYPE(options)) {
            case T_HASH:
                return cb_extract_timeout(timeout, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
            case T_FIXNUM:
            case T_BIGNUM:
                timeout = std::chrono::milliseconds(NUM2ULL(options));
                break;
            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("timeout must be an Integer, but given %+" PRIsVALUE, options));
        }
    }
}

static void
cb_extract_cas(couchbase::protocol::cas& field, VALUE cas)
{
    switch (TYPE(cas)) {
        case T_FIXNUM:
        case T_BIGNUM:
            field = cb_num_to_cas(cas);
            break;
        default:
            throw ruby_exception(rb_eArgError, rb_sprintf("CAS must be an Integer, but given %+" PRIsVALUE, cas));
    }
}

static void
cb_extract_option_bool(bool& field, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_TRUE:
                field = true;
                break;
            case T_FALSE:
                field = false;
                break;
            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("%s must be a Boolean, but given %+" PRIsVALUE, name, val));
        }
    }
}

static void
cb_extract_option_number(std::size_t& field, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
                field = FIX2ULONG(val);
                break;
            case T_BIGNUM:
                field = NUM2ULL(val);
                break;
            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("%s must be a Integer, but given %+" PRIsVALUE, name, val));
        }
    }
}

static void
cb_extract_option_milliseconds(std::chrono::milliseconds& field, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
                field = std::chrono::milliseconds(FIX2ULONG(val));
                break;
            case T_BIGNUM:
                field = std::chrono::milliseconds(NUM2ULL(val));
                break;
            default:
                throw ruby_exception(rb_eArgError,
                                     rb_sprintf("%s must be a Integer representing milliseconds, but given %+" PRIsVALUE, name, val));
        }
    }
}

static void
cb_extract_option_array(VALUE& val, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) == T_ARRAY) {
            return;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an Array, but given %+" PRIsVALUE, name, val));
    }
}

static VALUE
cb_Backend_open(VALUE self, VALUE connection_string, VALUE credentials, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(connection_string, T_STRING);
    Check_Type(credentials, T_HASH);

    VALUE username = Qnil;
    VALUE password = Qnil;

    VALUE certificate_path = rb_hash_aref(credentials, rb_id2sym(rb_intern("certificate_path")));
    VALUE key_path = rb_hash_aref(credentials, rb_id2sym(rb_intern("key_path")));
    if (NIL_P(certificate_path) || NIL_P(key_path)) {
        username = rb_hash_aref(credentials, rb_id2sym(rb_intern("username")));
        password = rb_hash_aref(credentials, rb_id2sym(rb_intern("password")));
        Check_Type(username, T_STRING);
        Check_Type(password, T_STRING);
    } else {
        Check_Type(certificate_path, T_STRING);
        Check_Type(key_path, T_STRING);
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        auto input = cb_string_new(connection_string);
        auto connstr = couchbase::utils::parse_connection_string(input);
        if (connstr.error) {
            throw ruby_exception(eInvalidArgument,
                                 fmt::format(R"(Failed to parse connection string "{}": {})", input, connstr.error.value()));
        }
        couchbase::cluster_credentials auth{};
        if (NIL_P(certificate_path) || NIL_P(key_path)) {
            auth.username = cb_string_new(username);
            auth.password = cb_string_new(password);
            if (!NIL_P(options)) {
                VALUE allowed_mechanisms = rb_hash_aref(options, rb_id2sym(rb_intern("allowed_sasl_mechanisms")));
                if (!NIL_P(allowed_mechanisms)) {
                    cb_check_type(allowed_mechanisms, T_ARRAY);
                    auto allowed_mechanisms_size = static_cast<size_t>(RARRAY_LEN(allowed_mechanisms));
                    if (allowed_mechanisms_size < 1) {
                        throw ruby_exception(eInvalidArgument, "allowed_sasl_mechanisms list cannot be empty");
                    }
                    auth.allowed_sasl_mechanisms.clear();
                    auth.allowed_sasl_mechanisms.reserve(allowed_mechanisms_size);
                    for (size_t i = 0; i < allowed_mechanisms_size; ++i) {
                        VALUE mechanism = rb_ary_entry(allowed_mechanisms, static_cast<long>(i));
                        if (mechanism == rb_id2sym(rb_intern("scram_sha512"))) {
                            auth.allowed_sasl_mechanisms.emplace_back("SCRAM-SHA512");
                        } else if (mechanism == rb_id2sym(rb_intern("scram_sha256"))) {
                            auth.allowed_sasl_mechanisms.emplace_back("SCRAM-SHA256");
                        } else if (mechanism == rb_id2sym(rb_intern("scram_sha1"))) {
                            auth.allowed_sasl_mechanisms.emplace_back("SCRAM-SHA1");
                        } else if (mechanism == rb_id2sym(rb_intern("plain"))) {
                            auth.allowed_sasl_mechanisms.emplace_back("PLAIN");
                        }
                    }
                }
            }
        } else {
            if (!connstr.tls) {
                throw ruby_exception(eInvalidArgument,
                                     "Certificate authenticator requires TLS connection, check the schema of the connection string");
            }
            auth.certificate_path = cb_string_new(certificate_path);
            auth.key_path = cb_string_new(key_path);
        }
        couchbase::origin origin(auth, connstr);

        cb_extract_option_bool(origin.options().enable_tracing, options, "enable_tracing");
        if (origin.options().enable_tracing) {
            cb_extract_option_milliseconds(origin.options().tracing_options.orphaned_emit_interval, options, "orphaned_emit_interval");
            cb_extract_option_number(origin.options().tracing_options.orphaned_sample_size, options, "orphaned_sample_size");
            cb_extract_option_milliseconds(origin.options().tracing_options.threshold_emit_interval, options, "threshold_emit_interval");
            cb_extract_option_number(origin.options().tracing_options.threshold_sample_size, options, "threshold_sample_size");
            cb_extract_option_milliseconds(origin.options().tracing_options.key_value_threshold, options, "key_value_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.query_threshold, options, "query_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.view_threshold, options, "view_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.search_threshold, options, "search_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.analytics_threshold, options, "analytics_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.management_threshold, options, "management_threshold");
        }
        cb_extract_option_bool(origin.options().enable_metrics, options, "enable_metrics");
        if (origin.options().enable_metrics) {
            cb_extract_option_milliseconds(origin.options().metrics_options.emit_interval, options, "metrics_emit_interval");
        }

        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster->open(origin, [barrier](std::error_code ec) { barrier->set_value(ec); });
        if (auto ec = cb_wait_for_future(f)) {
            throw ruby_exception(cb_map_error_code(ec, fmt::format("unable open cluster at {}", origin.next_address().first)));
        }
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_close(VALUE self)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);
    cb_backend_close(backend);
    return Qnil;
}

static VALUE
cb_Backend_diagnostics(VALUE self, VALUE report_id)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(report_id)) {
        Check_Type(report_id, T_STRING);
    }

    try {
        std::optional<std::string> id;
        if (!NIL_P(report_id)) {
            id.emplace(cb_string_new(report_id));
        }
        auto barrier = std::make_shared<std::promise<couchbase::diag::diagnostics_result>>();
        auto f = barrier->get_future();
        cluster->diagnostics(id, [barrier](couchbase::diag::diagnostics_result&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(resp.id));
        rb_hash_aset(res, rb_id2sym(rb_intern("sdk")), cb_str_new(resp.sdk));
        rb_hash_aset(res, rb_id2sym(rb_intern("version")), INT2FIX(resp.version));
        VALUE services = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("services")), services);
        for (const auto& [service_type, service_infos] : resp.services) {
            VALUE type = Qnil;
            switch (service_type) {
                case couchbase::service_type::key_value:
                    type = rb_id2sym(rb_intern("kv"));
                    break;
                case couchbase::service_type::query:
                    type = rb_id2sym(rb_intern("query"));
                    break;
                case couchbase::service_type::analytics:
                    type = rb_id2sym(rb_intern("analytics"));
                    break;
                case couchbase::service_type::search:
                    type = rb_id2sym(rb_intern("search"));
                    break;
                case couchbase::service_type::view:
                    type = rb_id2sym(rb_intern("views"));
                    break;
                case couchbase::service_type::management:
                    type = rb_id2sym(rb_intern("mgmt"));
                    break;
            }
            VALUE endpoints = rb_ary_new();
            rb_hash_aset(services, type, endpoints);
            for (const auto& svc : service_infos) {
                VALUE service = rb_hash_new();
                if (svc.last_activity) {
                    rb_hash_aset(service, rb_id2sym(rb_intern("last_activity_us")), LL2NUM(svc.last_activity->count()));
                }
                rb_hash_aset(service, rb_id2sym(rb_intern("id")), cb_str_new(svc.id));
                rb_hash_aset(service, rb_id2sym(rb_intern("remote")), cb_str_new(svc.remote));
                rb_hash_aset(service, rb_id2sym(rb_intern("local")), cb_str_new(svc.local));
                VALUE state = Qnil;
                switch (svc.state) {
                    case couchbase::diag::endpoint_state::disconnected:
                        state = rb_id2sym(rb_intern("disconnected"));
                        break;
                    case couchbase::diag::endpoint_state::connecting:
                        state = rb_id2sym(rb_intern("connecting"));
                        break;
                    case couchbase::diag::endpoint_state::connected:
                        state = rb_id2sym(rb_intern("connected"));
                        break;
                    case couchbase::diag::endpoint_state::disconnecting:
                        state = rb_id2sym(rb_intern("disconnecting"));
                        break;
                }
                if (svc.details) {
                    rb_hash_aset(service, rb_id2sym(rb_intern("details")), cb_str_new(svc.details.value()));
                }
                rb_hash_aset(service, rb_id2sym(rb_intern("state")), state);
                rb_ary_push(endpoints, service);
            }
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
        return Qnil;
    }
}

static VALUE
cb_Backend_open_bucket(VALUE self, VALUE bucket, VALUE wait_until_ready)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    bool wait = RTEST(wait_until_ready);

    try {
        std::string name(RSTRING_PTR(bucket), static_cast<size_t>(RSTRING_LEN(bucket)));

        if (wait) {
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            cluster->open_bucket(name, [barrier](std::error_code ec) { barrier->set_value(ec); });
            if (auto ec = cb_wait_for_future(f)) {
                throw ruby_exception(cb_map_error_code(ec, fmt::format("unable open bucket \"{}\"", name)));
            }
        } else {
            cluster->open_bucket(name, [name](std::error_code ec) { LOG_WARNING("unable open bucket \"{}\": {}", name, ec.message()); });
        }
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_array_of_ids(std::vector<couchbase::document_id>& ids, VALUE arg)
{
    if (TYPE(arg) != T_ARRAY) {
        throw ruby_exception(rb_eArgError, rb_sprintf("Type of IDs argument must be an Array, but given %+" PRIsVALUE, arg));
    }
    auto num_of_ids = static_cast<std::size_t>(RARRAY_LEN(arg));
    if (num_of_ids < 1) {
        throw ruby_exception(rb_eArgError, "Array of IDs must not be empty");
    }
    ids.reserve(num_of_ids);
    for (std::size_t i = 0; i < num_of_ids; ++i) {
        VALUE entry = rb_ary_entry(arg, static_cast<long>(i));
        if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 4) {
            throw ruby_exception(
              rb_eArgError,
              rb_sprintf("ID tuple must be represented as an Array[bucket, scope, collection, id], but given %+" PRIsVALUE, entry));
        }
        VALUE bucket = rb_ary_entry(entry, 0);
        if (TYPE(bucket) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Bucket must be a String, but given %+" PRIsVALUE, bucket));
        }
        VALUE scope = rb_ary_entry(entry, 1);
        if (TYPE(scope) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Scope must be a String, but given %+" PRIsVALUE, scope));
        }
        VALUE collection = rb_ary_entry(entry, 2);
        if (TYPE(collection) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Collection must be a String, but given %+" PRIsVALUE, collection));
        }
        VALUE id = rb_ary_entry(entry, 3);
        if (TYPE(id) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
        }
        ids.emplace_back(cb_string_new(bucket), cb_string_new(scope), cb_string_new(collection), cb_string_new(id));
    }
}

static void
cb_extract_array_of_id_content(std::vector<std::tuple<couchbase::document_id, std::string, std::uint32_t>>& id_content, VALUE arg)
{
    if (TYPE(arg) != T_ARRAY) {
        throw ruby_exception(rb_eArgError, rb_sprintf("Type of ID/content tuples must be an Array, but given %+" PRIsVALUE, arg));
    }
    auto num_of_tuples = static_cast<std::size_t>(RARRAY_LEN(arg));
    if (num_of_tuples < 1) {
        throw ruby_exception(rb_eArgError, "Array of ID/content tuples must not be empty");
    }
    id_content.reserve(num_of_tuples);
    for (std::size_t i = 0; i < num_of_tuples; ++i) {
        VALUE entry = rb_ary_entry(arg, static_cast<long>(i));
        if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 6) {
            throw ruby_exception(
              rb_eArgError,
              rb_sprintf(
                "ID/content tuple must be represented as an Array[bucket, scope, collection, id, content, flags], but given %+" PRIsVALUE,
                entry));
        }
        VALUE bucket = rb_ary_entry(entry, 0);
        if (TYPE(bucket) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Bucket must be a String, but given %+" PRIsVALUE, bucket));
        }
        VALUE scope = rb_ary_entry(entry, 1);
        if (TYPE(scope) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Scope must be a String, but given %+" PRIsVALUE, scope));
        }
        VALUE collection = rb_ary_entry(entry, 2);
        if (TYPE(collection) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Collection must be a String, but given %+" PRIsVALUE, collection));
        }
        VALUE id = rb_ary_entry(entry, 3);
        if (TYPE(id) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
        }
        VALUE content = rb_ary_entry(entry, 4);
        if (TYPE(content) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Content must be a String, but given %+" PRIsVALUE, content));
        }
        VALUE flags = rb_ary_entry(entry, 5);
        if (TYPE(flags) != T_FIXNUM) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Flags must be an Integer, but given %+" PRIsVALUE, flags));
        }
        id_content.emplace_back(
          couchbase::document_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
          },
          cb_string_new(content),
          FIX2UINT(flags));
    }
}

static void
cb_extract_array_of_id_cas(std::vector<std::pair<couchbase::document_id, couchbase::protocol::cas>>& id_cas, VALUE arg)
{
    if (TYPE(arg) != T_ARRAY) {
        throw ruby_exception(rb_eArgError, rb_sprintf("Type of ID/CAS tuples must be an Array, but given %+" PRIsVALUE, arg));
    }
    auto num_of_tuples = static_cast<std::size_t>(RARRAY_LEN(arg));
    if (num_of_tuples < 1) {
        rb_raise(rb_eArgError, "Array of ID/CAS tuples must not be empty");
    }
    id_cas.reserve(num_of_tuples);
    for (std::size_t i = 0; i < num_of_tuples; ++i) {
        VALUE entry = rb_ary_entry(arg, static_cast<long>(i));
        if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 5) {
            throw ruby_exception(
              rb_eArgError,
              rb_sprintf("ID/content tuple must be represented as an Array[bucket, scope, collection, id, CAS], but given %+" PRIsVALUE,
                         entry));
        }
        VALUE bucket = rb_ary_entry(entry, 0);
        if (TYPE(bucket) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Bucket must be a String, but given %+" PRIsVALUE, bucket));
        }
        VALUE scope = rb_ary_entry(entry, 1);
        if (TYPE(scope) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Scope must be a String, but given %+" PRIsVALUE, scope));
        }
        VALUE collection = rb_ary_entry(entry, 2);
        if (TYPE(collection) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Collection must be a String, but given %+" PRIsVALUE, collection));
        }
        VALUE id = rb_ary_entry(entry, 3);
        if (TYPE(id) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
        }
        couchbase::protocol::cas cas_val{};
        if (VALUE cas = rb_ary_entry(entry, 4); !NIL_P(cas)) {
            cb_extract_cas(cas_val, cas);
        }

        id_cas.emplace_back(
          couchbase::document_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
          },
          cas_val);
    }
}

static void
cb_extract_option_symbol(VALUE& val, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) == T_SYMBOL) {
            return;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an Symbol, but given %+" PRIsVALUE, name, val));
    }
}

static void
cb_extract_option_string(VALUE& val, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) == T_STRING) {
            return;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an String, but given %+" PRIsVALUE, name, val));
    }
}

static void
cb_extract_option_string(std::string& target, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) == T_STRING) {
            target = cb_string_new(val);
            return;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an String, but given %+" PRIsVALUE, name, val));
    }
}

static void
cb_extract_option_string(std::optional<std::string>& target, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) == T_STRING) {
            target.emplace(cb_string_new(val));
            return;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an String, but given %+" PRIsVALUE, name, val));
    }
}

static void
cb_extract_option_fixnum(VALUE& val, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) == T_FIXNUM) {
            return;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an Integer, but given %+" PRIsVALUE, name, val));
    }
}

template<typename T>
static void
cb_extract_option_uint32(T& field, VALUE options, const char* name)
{
    VALUE val = Qnil;
    cb_extract_option_fixnum(val, options, name);
    if (!NIL_P(val)) {
        field = FIX2UINT(val);
    }
}

static void
cb_extract_option_bignum(VALUE& val, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
            case T_BIGNUM:
                return;
            default:
                break;
        }
        throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an Integer, but given %+" PRIsVALUE, name, val));
    }
}

template<typename T>
static void
cb_extract_option_uint64(T& field, VALUE options, const char* name)
{
    VALUE val = Qnil;
    cb_extract_option_bignum(val, options, name);
    if (!NIL_P(val)) {
        field = NUM2ULL(val);
    }
}

static void
cb_extract_durability(couchbase::protocol::durability_level& output_level, std::optional<std::uint16_t>& output_timeout, VALUE options)
{
    VALUE durability_level = Qnil;
    cb_extract_option_symbol(durability_level, options, "durability_level");
    if (!NIL_P(durability_level)) {
        if (ID level = rb_sym2id(durability_level); level == rb_intern("none")) {
            output_level = couchbase::protocol::durability_level::none;
        } else if (level == rb_intern("majority")) {
            output_level = couchbase::protocol::durability_level::majority;
        } else if (level == rb_intern("majority_and_persist_to_active")) {
            output_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        } else if (level == rb_intern("persist_to_majority")) {
            output_level = couchbase::protocol::durability_level::persist_to_majority;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unknown durability level: %+" PRIsVALUE, durability_level));
        }
        VALUE durability_timeout = Qnil;
        cb_extract_option_fixnum(durability_timeout, options, "durability_timeout");
        if (!NIL_P(durability_timeout)) {
            output_timeout = FIX2UINT(durability_timeout);
        }
    }
}

template<typename Request>
static void
cb_extract_durability(Request& req, VALUE options)
{
    cb_extract_durability(req.durability_level, req.durability_timeout, options);
}

static VALUE
cb_Backend_ping(VALUE self, VALUE bucket, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(bucket)) {
        Check_Type(bucket, T_STRING);
    }

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }
    try {
        VALUE id = Qnil;
        cb_extract_option_string(id, options, "report_id");
        std::optional<std::string> report_id{};
        if (!NIL_P(id)) {
            report_id.emplace(cb_string_new(id));
        }
        std::optional<std::string> bucket_name{};
        if (!NIL_P(bucket)) {
            bucket_name.emplace(cb_string_new(bucket));
        }
        VALUE services = Qnil;
        cb_extract_option_array(services, options, "service_types");
        std::set<couchbase::service_type> selected_services{};
        if (!NIL_P(services)) {
            auto entries_num = static_cast<size_t>(RARRAY_LEN(services));
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(services, static_cast<long>(i));
                if (entry == rb_id2sym(rb_intern("kv"))) {
                    selected_services.insert(couchbase::service_type::key_value);
                } else if (entry == rb_id2sym(rb_intern("query"))) {
                    selected_services.insert(couchbase::service_type::query);
                } else if (entry == rb_id2sym(rb_intern("analytics"))) {
                    selected_services.insert(couchbase::service_type::analytics);
                } else if (entry == rb_id2sym(rb_intern("search"))) {
                    selected_services.insert(couchbase::service_type::search);
                } else if (entry == rb_id2sym(rb_intern("views"))) {
                    selected_services.insert(couchbase::service_type::view);
                }
            }
        }
        auto barrier = std::make_shared<std::promise<couchbase::diag::ping_result>>();
        auto f = barrier->get_future();
        cluster->ping(report_id, bucket_name, selected_services, [barrier](couchbase::diag::ping_result&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(resp.id));
        rb_hash_aset(res, rb_id2sym(rb_intern("sdk")), cb_str_new(resp.sdk));
        rb_hash_aset(res, rb_id2sym(rb_intern("version")), INT2FIX(resp.version));
        services = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("services")), services);
        for (const auto& [service_type, service_infos] : resp.services) {
            VALUE type = Qnil;
            switch (service_type) {
                case couchbase::service_type::key_value:
                    type = rb_id2sym(rb_intern("kv"));
                    break;
                case couchbase::service_type::query:
                    type = rb_id2sym(rb_intern("query"));
                    break;
                case couchbase::service_type::analytics:
                    type = rb_id2sym(rb_intern("analytics"));
                    break;
                case couchbase::service_type::search:
                    type = rb_id2sym(rb_intern("search"));
                    break;
                case couchbase::service_type::view:
                    type = rb_id2sym(rb_intern("views"));
                    break;
                case couchbase::service_type::management:
                    type = rb_id2sym(rb_intern("mgmt"));
                    break;
            }
            VALUE endpoints = rb_ary_new();
            rb_hash_aset(services, type, endpoints);
            for (const auto& svc : service_infos) {
                VALUE service = rb_hash_new();
                rb_hash_aset(service, rb_id2sym(rb_intern("latency")), LL2NUM(svc.latency.count()));
                rb_hash_aset(service, rb_id2sym(rb_intern("id")), cb_str_new(svc.id));
                rb_hash_aset(service, rb_id2sym(rb_intern("remote")), cb_str_new(svc.remote));
                rb_hash_aset(service, rb_id2sym(rb_intern("local")), cb_str_new(svc.local));
                VALUE state = Qnil;
                switch (svc.state) {
                    case couchbase::diag::ping_state::ok:
                        state = rb_id2sym(rb_intern("ok"));
                        break;
                    case couchbase::diag::ping_state::timeout:
                        state = rb_id2sym(rb_intern("timeout"));
                        break;
                    case couchbase::diag::ping_state::error:
                        state = rb_id2sym(rb_intern("error"));
                        if (svc.error) {
                            rb_hash_aset(service, rb_id2sym(rb_intern("error")), cb_str_new(svc.error.value()));
                        }
                        break;
                }
                rb_hash_aset(service, rb_id2sym(rb_intern("state")), state);
                rb_ary_push(endpoints, service);
            }
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::get_request req{ doc_id };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to fetch document");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get_multi(VALUE self, VALUE keys, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        std::chrono::milliseconds timeout{ 0 };
        cb_extract_timeout(timeout, options);

        std::vector<couchbase::document_id> ids{};
        cb_extract_array_of_ids(ids, keys);

        auto num_of_ids = ids.size();
        std::vector<std::shared_ptr<std::promise<couchbase::operations::get_response>>> barriers;
        barriers.reserve(num_of_ids);

        for (auto& id : ids) {
            couchbase::operations::get_request req{ std::move(id) };
            if (timeout.count() > 0) {
                req.timeout = timeout;
            }
            auto barrier = std::make_shared<std::promise<couchbase::operations::get_response>>();
            cluster->execute(req, [barrier](couchbase::operations::get_response&& resp) { barrier->set_value(std::move(resp)); });
            barriers.emplace_back(barrier);
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(num_of_ids));
        for (const auto& barrier : barriers) {
            auto resp = barrier->get_future().get();
            VALUE entry = rb_hash_new();
            if (resp.ctx.ec) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("error")), cb_map_error_code(resp.ctx, "unable to (multi)fetch document"));
            }
            rb_hash_aset(entry, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
            rb_hash_aset(entry, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
            rb_hash_aset(entry, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
            rb_ary_push(res, entry);
        }

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get_projected(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::get_projected_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_option_bool(req.with_expiry, options, "with_expiry");
        cb_extract_option_bool(req.preserve_array_indexes, options, "preserve_array_indexes");
        VALUE projections = Qnil;
        cb_extract_option_array(projections, options, "projections");
        if (!NIL_P(projections)) {
            auto entries_num = static_cast<size_t>(RARRAY_LEN(projections));
            if (entries_num == 0) {
                throw ruby_exception(rb_eArgError, "projections array must not be empty");
            }
            req.projections.reserve(entries_num);
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(projections, static_cast<long>(i));
                cb_check_type(entry, T_STRING);
                req.projections.emplace_back(cb_string_new(entry));
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::get_projected_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::get_projected_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable fetch with projections");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        if (resp.expiry) {
            rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(resp.expiry.value()));
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get_and_lock(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE lock_time, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(lock_time, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::get_and_lock_request req{ doc_id };
        cb_extract_timeout(req, options);
        req.lock_time = NUM2UINT(lock_time);

        auto barrier = std::make_shared<std::promise<couchbase::operations::get_and_lock_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::get_and_lock_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable lock and fetch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get_and_touch(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE expiry, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(expiry, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::get_and_touch_request req{ doc_id };
        cb_extract_timeout(req, options);
        req.expiry = NUM2UINT(expiry);

        auto barrier = std::make_shared<std::promise<couchbase::operations::get_and_touch_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::get_and_touch_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable fetch and touch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

template<typename Response>
static VALUE
cb_extract_mutation_result(Response resp)
{
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    VALUE token = rb_hash_new();
    rb_hash_aset(token, rb_id2sym(rb_intern("partition_uuid")), ULL2NUM(resp.token.partition_uuid));
    rb_hash_aset(token, rb_id2sym(rb_intern("sequence_number")), ULL2NUM(resp.token.sequence_number));
    rb_hash_aset(token, rb_id2sym(rb_intern("partition_id")), UINT2NUM(resp.token.partition_id));
    rb_hash_aset(token, rb_id2sym(rb_intern("bucket_name")), cb_str_new(resp.token.bucket_name));
    rb_hash_aset(res, rb_id2sym(rb_intern("mutation_token")), token);
    return res;
}

static VALUE
cb_Backend_document_touch(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE expiry, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(expiry, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::touch_request req{ doc_id };
        cb_extract_timeout(req, options);
        req.expiry = NUM2UINT(expiry);

        auto barrier = std::make_shared<std::promise<couchbase::operations::touch_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::touch_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to touch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_exists(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::exists_request req{ doc_id };
        cb_extract_timeout(req, options);

        auto barrier = std::make_shared<std::promise<couchbase::operations::exists_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::exists_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec && resp.ctx.ec != couchbase::error::key_value_errc::document_not_found) {
            cb_throw_error_code(resp.ctx, "unable to exists");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("exists")), resp.exists() ? Qtrue : Qfalse);
        rb_hash_aset(res, rb_id2sym(rb_intern("deleted")), resp.deleted ? Qtrue : Qfalse);
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(resp.expiry));
        rb_hash_aset(res, rb_id2sym(rb_intern("sequence_number")), ULL2NUM(resp.sequence_number));
        rb_hash_aset(res, rb_id2sym(rb_intern("datatype")), UINT2NUM(resp.datatype));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_unlock(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE cas, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::unlock_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_cas(req.cas, cas);

        auto barrier = std::make_shared<std::promise<couchbase::operations::unlock_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::unlock_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to unlock");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_upsert(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    Check_Type(flags, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::upsert_request req{ doc_id, value };
        cb_extract_timeout(req, options);
        req.flags = FIX2UINT(flags);

        cb_extract_durability(req, options);
        cb_extract_option_uint32(req.expiry, options, "expiry");
        cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");

        auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::upsert_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to upsert");
        }

        return cb_extract_mutation_result(resp);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_upsert_multi(VALUE self, VALUE id_content, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        std::chrono::milliseconds timeout{ 0 };
        cb_extract_timeout(timeout, options);

        couchbase::protocol::durability_level durability_level{ couchbase::protocol::durability_level::none };
        std::optional<std::uint16_t> durability_timeout{ std::nullopt };
        cb_extract_durability(durability_level, durability_timeout, options);

        VALUE expiry = Qnil;
        cb_extract_option_fixnum(expiry, options, "expiry");

        bool preserve_expiry{ false };
        cb_extract_option_bool(preserve_expiry, options, "preserve_expiry");

        std::vector<std::tuple<couchbase::document_id, std::string, std::uint32_t>> tuples{};
        cb_extract_array_of_id_content(tuples, id_content);

        auto num_of_tuples = tuples.size();
        std::vector<std::shared_ptr<std::promise<couchbase::operations::upsert_response>>> barriers;
        barriers.reserve(num_of_tuples);

        for (auto& [id, content, flags] : tuples) {
            couchbase::operations::upsert_request req{ std::move(id), std::move(content) };
            if (timeout.count() > 0) {
                req.timeout = timeout;
            }
            req.flags = flags;
            req.durability_level = durability_level;
            req.durability_timeout = durability_timeout;
            if (!NIL_P(expiry)) {
                req.expiry = FIX2UINT(expiry);
            }
            req.preserve_expiry = preserve_expiry;
            auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
            cluster->execute(req, [barrier](couchbase::operations::upsert_response&& resp) { barrier->set_value(std::move(resp)); });
            barriers.emplace_back(barrier);
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(num_of_tuples));
        for (const auto& barrier : barriers) {
            auto resp = barrier->get_future().get();
            VALUE entry = cb_extract_mutation_result(resp);
            if (resp.ctx.ec) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("error")), cb_map_error_code(resp.ctx, "unable (multi)upsert"));
            }
            rb_ary_push(res, entry);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_append(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::append_request req{ doc_id, value };
        cb_extract_timeout(req, options);
        cb_extract_durability(req, options);

        auto barrier = std::make_shared<std::promise<couchbase::operations::append_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::append_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to append");
        }

        return cb_extract_mutation_result(resp);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_prepend(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::prepend_request req{ doc_id, value };
        cb_extract_timeout(req, options);
        cb_extract_durability(req, options);

        auto barrier = std::make_shared<std::promise<couchbase::operations::prepend_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::prepend_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to prepend");
        }

        return cb_extract_mutation_result(resp);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_replace(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    Check_Type(flags, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::replace_request req{ doc_id, value };
        cb_extract_timeout(req, options);
        req.flags = FIX2UINT(flags);

        cb_extract_durability(req, options);
        cb_extract_option_uint32(req.expiry, options, "expiry");
        cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");
        VALUE cas = Qnil;
        cb_extract_option_bignum(cas, options, "cas");
        if (!NIL_P(cas)) {
            cb_extract_cas(req.cas, cas);
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::replace_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::replace_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to replace");
        }

        return cb_extract_mutation_result(resp);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_insert(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    Check_Type(flags, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };
        std::string value(RSTRING_PTR(content), static_cast<size_t>(RSTRING_LEN(content)));

        couchbase::operations::insert_request req{ doc_id, value };
        cb_extract_timeout(req, options);
        req.flags = FIX2UINT(flags);

        cb_extract_durability(req, options);
        cb_extract_option_uint32(req.expiry, options, "expiry");

        auto barrier = std::make_shared<std::promise<couchbase::operations::insert_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::insert_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to insert");
        }

        return cb_extract_mutation_result(resp);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_remove(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::remove_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_durability(req, options);
        VALUE cas = Qnil;
        cb_extract_option_bignum(cas, options, "cas");
        if (!NIL_P(cas)) {
            cb_extract_cas(req.cas, cas);
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::remove_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::remove_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to remove");
        }
        return cb_extract_mutation_result(resp);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_remove_multi(VALUE self, VALUE id_cas, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        std::chrono::milliseconds timeout{ 0 };
        cb_extract_timeout(timeout, options);

        couchbase::protocol::durability_level durability_level{ couchbase::protocol::durability_level::none };
        std::optional<std::uint16_t> durability_timeout{ std::nullopt };
        cb_extract_durability(durability_level, durability_timeout, options);

        std::vector<std::pair<couchbase::document_id, couchbase::protocol::cas>> tuples{};
        cb_extract_array_of_id_cas(tuples, id_cas);

        auto num_of_tuples = tuples.size();
        std::vector<std::shared_ptr<std::promise<couchbase::operations::remove_response>>> barriers;
        barriers.reserve(num_of_tuples);

        for (auto& [id, cas] : tuples) {
            couchbase::operations::remove_request req{ std::move(id) };
            req.cas = cas;
            if (timeout.count() > 0) {
                req.timeout = timeout;
            }
            req.durability_level = durability_level;
            req.durability_timeout = durability_timeout;
            auto barrier = std::make_shared<std::promise<couchbase::operations::remove_response>>();
            cluster->execute(req, [barrier](couchbase::operations::remove_response&& resp) { barrier->set_value(std::move(resp)); });
            barriers.emplace_back(barrier);
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(num_of_tuples));
        for (const auto& barrier : barriers) {
            auto resp = barrier->get_future().get();
            VALUE entry = cb_extract_mutation_result(resp);
            if (resp.ctx.ec) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("error")), cb_map_error_code(resp.ctx, "unable (multi)remove"));
            }
            rb_ary_push(res, entry);
        }

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_increment(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::increment_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_durability(req, options);
        cb_extract_option_uint64(req.delta, options, "delta");
        cb_extract_option_uint64(req.initial_value, options, "initial_value");
        cb_extract_option_uint32(req.expiry, options, "expiry");
        cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");

        auto barrier = std::make_shared<std::promise<couchbase::operations::increment_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::increment_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx.ec, fmt::format(R"(unable to increment by {})", req.delta));
        }
        VALUE res = cb_extract_mutation_result(resp);
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_decrement(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::decrement_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_durability(req, options);
        cb_extract_option_uint64(req.delta, options, "delta");
        cb_extract_option_uint64(req.initial_value, options, "initial_value");
        cb_extract_option_uint32(req.expiry, options, "expiry");
        cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");

        auto barrier = std::make_shared<std::promise<couchbase::operations::decrement_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::decrement_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to decrement by {})", req.delta));
        }
        VALUE res = cb_extract_mutation_result(resp);
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_map_subdoc_opcode(couchbase::protocol::subdoc_opcode opcode)
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

        case couchbase::protocol::subdoc_opcode::remove_doc:
            return rb_id2sym(rb_intern("remove_doc"));

        case couchbase::protocol::subdoc_opcode::replace_body_with_xattr:
            return rb_id2sym(rb_intern("replace_body_with_xattr"));
    }
    return rb_id2sym(rb_intern("unknown"));
}

static void
cb_map_subdoc_status(couchbase::protocol::status status, std::size_t index, const std::string& path, VALUE entry)
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
              rb_exc_new_cstr(eBackendError,
                              fmt::format("unknown subdocument error status={}, index={}, path={}", status, index, path).c_str()));
            return;
    }
}

static VALUE
cb_Backend_document_lookup_in(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE specs, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(specs, T_ARRAY);
    if (RARRAY_LEN(specs) <= 0) {
        rb_raise(rb_eArgError, "Array with specs cannot be empty");
        return Qnil;
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::lookup_in_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_option_bool(req.access_deleted, options, "access_deleted");
        auto entries_size = static_cast<size_t>(RARRAY_LEN(specs));
        req.specs.entries.reserve(entries_size);
        for (size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            cb_check_type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, rb_id2sym(rb_intern("opcode")));
            cb_check_type(operation, T_SYMBOL);
            couchbase::protocol::subdoc_opcode opcode{};
            if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
                opcode = couchbase::protocol::subdoc_opcode::get_doc;
            } else if (operation_id == rb_intern("get")) {
                opcode = couchbase::protocol::subdoc_opcode::get;
            } else if (operation_id == rb_intern("exists")) {
                opcode = couchbase::protocol::subdoc_opcode::exists;
            } else if (operation_id == rb_intern("count")) {
                opcode = couchbase::protocol::subdoc_opcode::get_count;
            } else {
                throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
            }
            bool xattr = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("xattr"))));
            VALUE path = rb_hash_aref(entry, rb_id2sym(rb_intern("path")));
            cb_check_type(path, T_STRING);
            req.specs.add_spec(opcode, xattr, cb_string_new(path));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::lookup_in_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::lookup_in_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable fetch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("fields")), fields);
        if (resp.deleted) {
            rb_hash_aset(res, rb_id2sym(rb_intern("deleted")), Qtrue);
        }
        for (size_t i = 0; i < resp.fields.size(); ++i) {
            VALUE entry = rb_hash_new();
            rb_hash_aset(entry, rb_id2sym(rb_intern("index")), ULL2NUM(i));
            rb_hash_aset(entry, rb_id2sym(rb_intern("exists")), resp.fields[i].exists ? Qtrue : Qfalse);
            rb_hash_aset(entry, rb_id2sym(rb_intern("path")), cb_str_new(resp.fields[i].path));
            rb_hash_aset(entry, rb_id2sym(rb_intern("value")), cb_str_new(resp.fields[i].value));
            cb_map_subdoc_status(resp.fields[i].status, i, resp.fields[i].path, entry);
            if (resp.fields[i].opcode == couchbase::protocol::subdoc_opcode::get && resp.fields[i].path.empty()) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("get_doc")));
            } else {
                rb_hash_aset(entry, rb_id2sym(rb_intern("type")), cb_map_subdoc_opcode(resp.fields[i].opcode));
            }
            rb_ary_store(fields, static_cast<long>(i), entry);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_mutate_in(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE specs, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(specs, T_ARRAY);
    if (RARRAY_LEN(specs) <= 0) {
        rb_raise(rb_eArgError, "Array with specs cannot be empty");
        return Qnil;
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::operations::mutate_in_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_durability(req, options);
        VALUE cas = Qnil;
        cb_extract_option_bignum(cas, options, "cas");
        if (!NIL_P(cas)) {
            cb_extract_cas(req.cas, cas);
        }
        cb_extract_option_uint32(req.expiry, options, "expiry");
        cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");
        cb_extract_option_bool(req.access_deleted, options, "access_deleted");
        cb_extract_option_bool(req.create_as_deleted, options, "create_as_deleted");
        VALUE store_semantics = Qnil;
        cb_extract_option_symbol(store_semantics, options, "store_semantics");
        if (ID semantics = rb_sym2id(store_semantics); semantics == rb_intern("replace")) {
            req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::replace;
        } else if (semantics == rb_intern("insert")) {
            req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::insert;
        } else if (semantics == rb_intern("upsert")) {
            req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::upsert;
        }
        auto entries_size = static_cast<size_t>(RARRAY_LEN(specs));
        req.specs.entries.reserve(entries_size);
        for (size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            cb_check_type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, rb_id2sym(rb_intern("opcode")));
            cb_check_type(operation, T_SYMBOL);
            couchbase::protocol::subdoc_opcode opcode{};
            if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("dict_add")) {
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
            } else if (operation_id == rb_intern("remove_doc")) {
                opcode = couchbase::protocol::subdoc_opcode::remove_doc;
            } else {
                throw ruby_exception(eInvalidArgument,
                                     rb_sprintf("unsupported operation for subdocument mutation: %+" PRIsVALUE, operation));
            }
            bool xattr = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("xattr"))));
            bool create_path = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("create_path"))));
            bool expand_macros = RTEST(rb_hash_aref(entry, rb_id2sym(rb_intern("expand_macros"))));
            VALUE path = rb_hash_aref(entry, rb_id2sym(rb_intern("path")));
            cb_check_type(path, T_STRING);
            if (VALUE param = rb_hash_aref(entry, rb_id2sym(rb_intern("param"))); NIL_P(param)) {
                req.specs.add_spec(opcode, xattr, cb_string_new(path));
            } else if (opcode == couchbase::protocol::subdoc_opcode::counter) {
                cb_check_type(param, T_FIXNUM);
                req.specs.add_spec(opcode, xattr, create_path, expand_macros, cb_string_new(path), FIX2LONG(param));
            } else {
                cb_check_type(param, T_STRING);
                req.specs.add_spec(opcode, xattr, create_path, expand_macros, cb_string_new(path), cb_string_new(param));
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::mutate_in_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::mutate_in_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to mutate");
        }

        VALUE res = cb_extract_mutation_result(resp);
        if (resp.first_error_index) {
            rb_hash_aset(res, rb_id2sym(rb_intern("first_error_index")), ULL2NUM(resp.first_error_index.value()));
        }
        if (resp.deleted) {
            rb_hash_aset(res, rb_id2sym(rb_intern("deleted")), Qtrue);
        }
        VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("fields")), fields);
        for (size_t i = 0; i < resp.fields.size(); ++i) {
            VALUE entry = rb_hash_new();
            rb_hash_aset(entry, rb_id2sym(rb_intern("index")), ULL2NUM(i));
            rb_hash_aset(entry, rb_id2sym(rb_intern("path")), cb_str_new(resp.fields[i].path));
            if (resp.fields[i].status == couchbase::protocol::status::success ||
                resp.fields[i].status == couchbase::protocol::status::subdoc_success_deleted) {
                if (resp.fields[i].opcode == couchbase::protocol::subdoc_opcode::counter) {
                    if (!resp.fields[i].value.empty()) {
                        rb_hash_aset(entry, rb_id2sym(rb_intern("value")), LL2NUM(std::stoll(resp.fields[i].value)));
                    }
                } else {
                    rb_hash_aset(entry, rb_id2sym(rb_intern("value")), cb_str_new(resp.fields[i].value));
                }
            }
            cb_map_subdoc_status(resp.fields[i].status, i, resp.fields[i].path, entry);
            rb_hash_aset(entry, rb_id2sym(rb_intern("type")), cb_map_subdoc_opcode(resp.fields[i].opcode));
            rb_ary_store(fields, static_cast<long>(i), entry);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static int
cb_for_each_named_param(VALUE key, VALUE value, VALUE arg)
{
    auto* preq = reinterpret_cast<couchbase::operations::query_request*>(arg);
    try {
        cb_check_type(key, T_STRING);
        cb_check_type(value, T_STRING);
    } catch (const ruby_exception&) {
        return ST_STOP;
    }
    preq->named_parameters.emplace(std::string_view(RSTRING_PTR(key), static_cast<std::size_t>(RSTRING_LEN(key))),
                                   couchbase::utils::json::parse(RSTRING_PTR(value), static_cast<std::size_t>(RSTRING_LEN(value))));
    return ST_CONTINUE;
}

static VALUE
cb_Backend_document_query(VALUE self, VALUE statement, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(statement, T_STRING);
    Check_Type(options, T_HASH);

    try {
        couchbase::operations::query_request req;
        req.statement = cb_string_new(statement);
        if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id"))); !NIL_P(client_context_id)) {
            cb_check_type(client_context_id, T_STRING);
            req.client_context_id = cb_string_new(client_context_id);
        }
        cb_extract_timeout(req, options);
        cb_extract_option_bool(req.adhoc, options, "adhoc");
        cb_extract_option_bool(req.metrics, options, "metrics");
        cb_extract_option_bool(req.readonly, options, "readonly");
        cb_extract_option_bool(req.flex_index, options, "flex_index");
        cb_extract_option_uint64(req.scan_cap, options, "scan_cap");
        cb_extract_option_uint64(req.scan_wait, options, "scan_wait");
        cb_extract_option_uint64(req.max_parallelism, options, "max_parallelism");
        cb_extract_option_uint64(req.pipeline_cap, options, "pipeline_cap");
        cb_extract_option_uint64(req.pipeline_batch, options, "pipeline_batch");
        if (VALUE scope_qualifier = rb_hash_aref(options, rb_id2sym(rb_intern("scope_qualifier")));
            !NIL_P(scope_qualifier) && TYPE(scope_qualifier) == T_STRING) {
            req.scope_qualifier.emplace(cb_string_new(scope_qualifier));
        } else {
            VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
            if (!NIL_P(scope_name) && TYPE(scope_name) == T_STRING) {
                req.scope_name.emplace(cb_string_new(scope_name));
                VALUE bucket_name = rb_hash_aref(options, rb_id2sym(rb_intern("bucket_name")));
                if (NIL_P(bucket_name)) {
                    throw ruby_exception(eInvalidArgument,
                                         fmt::format("bucket must be specified for query in scope \"{}\"", req.scope_name.value()));
                }
                req.bucket_name.emplace(cb_string_new(bucket_name));
            }
        }
        if (VALUE profile = rb_hash_aref(options, rb_id2sym(rb_intern("profile"))); !NIL_P(profile)) {
            cb_check_type(profile, T_SYMBOL);
            ID mode = rb_sym2id(profile);
            if (mode == rb_intern("phases")) {
                req.profile = couchbase::operations::query_request::profile_mode::phases;
            } else if (mode == rb_intern("timings")) {
                req.profile = couchbase::operations::query_request::profile_mode::timings;
            } else if (mode == rb_intern("off")) {
                req.profile = couchbase::operations::query_request::profile_mode::off;
            }
        }
        if (VALUE positional_params = rb_hash_aref(options, rb_id2sym(rb_intern("positional_parameters"))); !NIL_P(positional_params)) {
            cb_check_type(positional_params, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(positional_params));
            req.positional_parameters.reserve(entries_num);
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
                cb_check_type(entry, T_STRING);
                req.positional_parameters.emplace_back(
                  couchbase::utils::json::parse(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry))));
            }
        }
        if (VALUE named_params = rb_hash_aref(options, rb_id2sym(rb_intern("named_parameters"))); !NIL_P(named_params)) {
            cb_check_type(named_params, T_HASH);
            rb_hash_foreach(named_params, INT_FUNC(cb_for_each_named_param), reinterpret_cast<VALUE>(&req));
        }
        if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
            cb_check_type(scan_consistency, T_SYMBOL);
            ID type = rb_sym2id(scan_consistency);
            if (type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::operations::query_request::scan_consistency_type::not_bounded;
            } else if (type == rb_intern("request_plus")) {
                req.scan_consistency = couchbase::operations::query_request::scan_consistency_type::request_plus;
            }
        }
        if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state"))); !NIL_P(mutation_state)) {
            cb_check_type(mutation_state, T_ARRAY);
            auto state_size = static_cast<size_t>(RARRAY_LEN(mutation_state));
            req.mutation_state.reserve(state_size);
            for (size_t i = 0; i < state_size; ++i) {
                VALUE token = rb_ary_entry(mutation_state, static_cast<long>(i));
                cb_check_type(token, T_HASH);
                VALUE bucket_name = rb_hash_aref(token, rb_id2sym(rb_intern("bucket_name")));
                cb_check_type(bucket_name, T_STRING);
                VALUE partition_id = rb_hash_aref(token, rb_id2sym(rb_intern("partition_id")));
                cb_check_type(partition_id, T_FIXNUM);
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
                req.mutation_state.emplace_back(couchbase::mutation_token{ NUM2ULL(partition_uuid),
                                                                           NUM2ULL(sequence_number),
                                                                           gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                                                           cb_string_new(bucket_name) });
            }
        }

        if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters"))); !NIL_P(raw_params)) {
            cb_check_type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb_for_each_named_param), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::query_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::query_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.payload.meta_data.errors && !resp.payload.meta_data.errors->empty()) {
                const auto& first_error = resp.payload.meta_data.errors->front();
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to query ({}: {}))", first_error.code, first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, "unable to query");
            }
        }
        VALUE res = rb_hash_new();
        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.payload.rows.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
        for (const auto& row : resp.payload.rows) {
            rb_ary_push(rows, cb_str_new(row));
        }
        VALUE meta = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
        rb_hash_aset(meta,
                     rb_id2sym(rb_intern("status")),
                     rb_id2sym(rb_intern2(resp.payload.meta_data.status.data(), static_cast<long>(resp.payload.meta_data.status.size()))));
        rb_hash_aset(meta, rb_id2sym(rb_intern("request_id")), cb_str_new(resp.payload.meta_data.request_id));
        rb_hash_aset(meta, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.payload.meta_data.client_context_id));
        if (resp.payload.meta_data.signature) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("signature")), cb_str_new(resp.payload.meta_data.signature.value()));
        }
        if (resp.payload.meta_data.profile) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("profile")), cb_str_new(resp.payload.meta_data.profile.value()));
        }
        VALUE metrics = rb_hash_new();
        rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
        if (!resp.payload.meta_data.metrics.elapsed_time.empty()) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("elapsed_time")), cb_str_new(resp.payload.meta_data.metrics.elapsed_time));
        }
        if (!resp.payload.meta_data.metrics.execution_time.empty()) {
            rb_hash_aset(metrics, rb_id2sym(rb_intern("execution_time")), cb_str_new(resp.payload.meta_data.metrics.execution_time));
        }
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.payload.meta_data.metrics.result_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.payload.meta_data.metrics.result_size));
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
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_generate_bucket_settings(VALUE bucket, couchbase::operations::management::bucket_settings& entry, bool is_create)
{
    if (VALUE bucket_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("bucket_type"))); TYPE(bucket_type) == T_SYMBOL) {
        if (bucket_type == rb_id2sym(rb_intern("couchbase")) || bucket_type == rb_id2sym(rb_intern("membase"))) {
            entry.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::couchbase;
        } else if (bucket_type == rb_id2sym(rb_intern("memcached"))) {
            entry.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::memcached;
        } else if (bucket_type == rb_id2sym(rb_intern("ephemeral"))) {
            entry.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::ephemeral;
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("unknown bucket type, given %+" PRIsVALUE, bucket_type));
        }
    } else {
        throw ruby_exception(rb_eArgError, rb_sprintf("bucket type must be a Symbol, given %+" PRIsVALUE, bucket_type));
    }

    if (VALUE name = rb_hash_aref(bucket, rb_id2sym(rb_intern("name"))); TYPE(name) == T_STRING) {
        entry.name = cb_string_new(name);
    } else {
        throw ruby_exception(rb_eArgError, rb_sprintf("bucket name must be a String, given %+" PRIsVALUE, name));
    }

    if (VALUE quota = rb_hash_aref(bucket, rb_id2sym(rb_intern("ram_quota_mb"))); TYPE(quota) == T_FIXNUM) {
        entry.ram_quota_mb = FIX2ULONG(quota);
    } else {
        throw ruby_exception(rb_eArgError, rb_sprintf("bucket RAM quota must be an Integer, given %+" PRIsVALUE, quota));
    }

    if (VALUE expiry = rb_hash_aref(bucket, rb_id2sym(rb_intern("max_expiry"))); !NIL_P(expiry)) {
        if (TYPE(expiry) == T_FIXNUM) {
            entry.max_expiry = FIX2UINT(expiry);
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("bucket max expiry must be an Integer, given %+" PRIsVALUE, expiry));
        }
    }

    if (VALUE num_replicas = rb_hash_aref(bucket, rb_id2sym(rb_intern("num_replicas"))); !NIL_P(num_replicas)) {
        if (TYPE(num_replicas) == T_FIXNUM) {
            entry.num_replicas = FIX2UINT(num_replicas);
        } else {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("bucket number of replicas must be an Integer, given %+" PRIsVALUE, num_replicas));
        }
    }

    if (VALUE replica_indexes = rb_hash_aref(bucket, rb_id2sym(rb_intern("replica_indexes"))); !NIL_P(replica_indexes)) {
        entry.replica_indexes = RTEST(replica_indexes);
    }

    if (VALUE flush_enabled = rb_hash_aref(bucket, rb_id2sym(rb_intern("flush_enabled"))); !NIL_P(flush_enabled)) {
        entry.flush_enabled = RTEST(flush_enabled);
    }

    if (VALUE compression_mode = rb_hash_aref(bucket, rb_id2sym(rb_intern("compression_mode"))); !NIL_P(compression_mode)) {
        if (TYPE(compression_mode) == T_SYMBOL) {
            if (compression_mode == rb_id2sym(rb_intern("active"))) {
                entry.compression_mode = couchbase::operations::management::bucket_settings::compression_mode::active;
            } else if (compression_mode == rb_id2sym(rb_intern("passive"))) {
                entry.compression_mode = couchbase::operations::management::bucket_settings::compression_mode::passive;
            } else if (compression_mode == rb_id2sym(rb_intern("off"))) {
                entry.compression_mode = couchbase::operations::management::bucket_settings::compression_mode::off;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown compression mode, given %+" PRIsVALUE, compression_mode));
            }
        } else {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("bucket compression mode must be a Symbol, given %+" PRIsVALUE, compression_mode));
        }
    }

    if (VALUE eviction_policy = rb_hash_aref(bucket, rb_id2sym(rb_intern("eviction_policy"))); !NIL_P(eviction_policy)) {
        if (TYPE(eviction_policy) == T_SYMBOL) {
            if (eviction_policy == rb_id2sym(rb_intern("full"))) {
                entry.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::full;
            } else if (eviction_policy == rb_id2sym(rb_intern("value_only"))) {
                entry.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::value_only;
            } else if (eviction_policy == rb_id2sym(rb_intern("no_eviction"))) {
                entry.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::no_eviction;
            } else if (eviction_policy == rb_id2sym(rb_intern("not_recently_used"))) {
                entry.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::not_recently_used;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown eviction policy, given %+" PRIsVALUE, eviction_policy));
            }
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("bucket eviction policy must be a Symbol, given %+" PRIsVALUE, eviction_policy));
        }
    }

    if (VALUE minimum_level = rb_hash_aref(bucket, rb_id2sym(rb_intern("minimum_durability_level"))); !NIL_P(minimum_level)) {
        if (TYPE(minimum_level) == T_SYMBOL) {
            if (minimum_level == rb_id2sym(rb_intern("none"))) {
                entry.minimum_durability_level = couchbase::protocol::durability_level::none;
            } else if (minimum_level == rb_id2sym(rb_intern("majority"))) {
                entry.minimum_durability_level = couchbase::protocol::durability_level::majority;
            } else if (minimum_level == rb_id2sym(rb_intern("majority_and_persist_to_active"))) {
                entry.minimum_durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
            } else if (minimum_level == rb_id2sym(rb_intern("persist_to_majority"))) {
                entry.minimum_durability_level = couchbase::protocol::durability_level::persist_to_majority;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown durability level, given %+" PRIsVALUE, minimum_level));
            }
        } else {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("bucket minimum durability level must be a Symbol, given %+" PRIsVALUE, minimum_level));
        }
    }

    if (is_create) {
        if (VALUE conflict_resolution_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("conflict_resolution_type")));
            !NIL_P(conflict_resolution_type)) {
            if (TYPE(conflict_resolution_type) == T_SYMBOL) {
                if (conflict_resolution_type == rb_id2sym(rb_intern("timestamp"))) {
                    entry.conflict_resolution_type =
                      couchbase::operations::management::bucket_settings::conflict_resolution_type::timestamp;
                } else if (conflict_resolution_type == rb_id2sym(rb_intern("sequence_number"))) {
                    entry.conflict_resolution_type =
                      couchbase::operations::management::bucket_settings::conflict_resolution_type::sequence_number;
                } else {
                    throw ruby_exception(rb_eArgError,
                                         rb_sprintf("unknown conflict resolution type, given %+" PRIsVALUE, conflict_resolution_type));
                }
            } else {
                throw ruby_exception(
                  rb_eArgError,
                  rb_sprintf("bucket conflict resolution type must be a Symbol, given %+" PRIsVALUE, conflict_resolution_type));
            }
        }
    }
}

static VALUE
cb_Backend_bucket_create(VALUE self, VALUE bucket_settings, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_settings, T_HASH);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::bucket_create_request req{};
        cb_extract_timeout(req, options);
        cb_generate_bucket_settings(bucket_settings, req.bucket, true);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::bucket_create_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format("unable to create bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_bucket_update(VALUE self, VALUE bucket_settings, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_settings, T_HASH);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }
    try {
        couchbase::operations::management::bucket_update_request req{};
        cb_extract_timeout(req, options);
        cb_generate_bucket_settings(bucket_settings, req.bucket, false);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_update_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::bucket_update_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format("unable to update bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_bucket_drop(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::bucket_drop_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::bucket_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_bucket_flush(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::bucket_flush_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_flush_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::bucket_flush_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to flush bucket \"{}\" on the cluster", req.name));
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_bucket_settings(const couchbase::operations::management::bucket_settings& entry, VALUE bucket)
{
    switch (entry.bucket_type) {
        case couchbase::operations::management::bucket_settings::bucket_type::couchbase:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("couchbase")));
            break;
        case couchbase::operations::management::bucket_settings::bucket_type::memcached:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("memcached")));
            break;
        case couchbase::operations::management::bucket_settings::bucket_type::ephemeral:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("ephemeral")));
            break;
        case couchbase::operations::management::bucket_settings::bucket_type::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), Qnil);
            break;
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("uuid")), cb_str_new(entry.uuid));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("ram_quota_mb")), ULL2NUM(entry.ram_quota_mb));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("max_expiry")), ULONG2NUM(entry.max_expiry));
    switch (entry.compression_mode) {
        case couchbase::operations::management::bucket_settings::compression_mode::off:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("off")));
            break;
        case couchbase::operations::management::bucket_settings::compression_mode::active:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("active")));
            break;
        case couchbase::operations::management::bucket_settings::compression_mode::passive:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("passive")));
            break;
        case couchbase::operations::management::bucket_settings::compression_mode::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), Qnil);
            break;
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("num_replicas")), ULONG2NUM(entry.num_replicas));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("replica_indexes")), entry.replica_indexes ? Qtrue : Qfalse);
    rb_hash_aset(bucket, rb_id2sym(rb_intern("flush_enabled")), entry.flush_enabled ? Qtrue : Qfalse);
    switch (entry.eviction_policy) {
        case couchbase::operations::management::bucket_settings::eviction_policy::full:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("full")));
            break;
        case couchbase::operations::management::bucket_settings::eviction_policy::value_only:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("value_only")));
            break;
        case couchbase::operations::management::bucket_settings::eviction_policy::no_eviction:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("no_eviction")));
            break;
        case couchbase::operations::management::bucket_settings::eviction_policy::not_recently_used:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("not_recently_used")));
            break;
        case couchbase::operations::management::bucket_settings::eviction_policy::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), Qnil);
            break;
    }
    switch (entry.conflict_resolution_type) {
        case couchbase::operations::management::bucket_settings::conflict_resolution_type::timestamp:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("timestamp")));
            break;
        case couchbase::operations::management::bucket_settings::conflict_resolution_type::sequence_number:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("sequence_number")));
            break;
        case couchbase::operations::management::bucket_settings::conflict_resolution_type::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), Qnil);
            break;
    }
    if (entry.minimum_durability_level) {
        switch (entry.minimum_durability_level.value()) {
            case couchbase::protocol::durability_level::none:
                rb_hash_aset(bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("none")));
                break;
            case couchbase::protocol::durability_level::majority:
                rb_hash_aset(bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("majority")));
                break;
            case couchbase::protocol::durability_level::majority_and_persist_to_active:
                rb_hash_aset(
                  bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("majority_and_persist_to_active")));
                break;
            case couchbase::protocol::durability_level::persist_to_majority:
                rb_hash_aset(bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("persist_to_majority")));
                break;
        }
    }
    VALUE capabilities = rb_ary_new_capa(static_cast<long>(entry.capabilities.size()));
    for (const auto& capa : entry.capabilities) {
        rb_ary_push(capabilities, cb_str_new(capa));
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("capabilities")), capabilities);
    VALUE nodes = rb_ary_new_capa(static_cast<long>(entry.nodes.size()));
    for (const auto& n : entry.nodes) {
        VALUE node = rb_hash_new();
        rb_hash_aset(node, rb_id2sym(rb_intern("status")), cb_str_new(n.status));
        rb_hash_aset(node, rb_id2sym(rb_intern("hostname")), cb_str_new(n.hostname));
        rb_hash_aset(node, rb_id2sym(rb_intern("version")), cb_str_new(n.version));
        rb_ary_push(nodes, node);
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("nodes")), nodes);
}

static VALUE
cb_Backend_bucket_get_all(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::bucket_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::bucket_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to get list of the buckets of the cluster");
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.buckets.size()));
        for (const auto& entry : resp.buckets) {
            VALUE bucket = rb_hash_new();
            cb_extract_bucket_settings(entry, bucket);
            rb_ary_push(res, bucket);
        }

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_bucket_get(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::bucket_get_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::operations::management::bucket_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to locate bucket \"{}\" on the cluster", req.name));
        }

        VALUE res = rb_hash_new();
        cb_extract_bucket_settings(resp.bucket, res);
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_role(const couchbase::operations::management::rbac::role_and_description& entry, VALUE role)
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

static VALUE
cb_Backend_role_get_all(VALUE self, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::role_get_all_request req{};
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::role_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::role_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to fetch roles");
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.roles.size()));
        for (const auto& entry : resp.roles) {
            VALUE role = rb_hash_new();
            cb_extract_role(entry, role);
            rb_ary_push(res, role);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_user(const couchbase::operations::management::rbac::user_and_metadata& entry, VALUE user)
{
    rb_hash_aset(user, rb_id2sym(rb_intern("username")), cb_str_new(entry.username));
    switch (entry.domain) {
        case couchbase::operations::management::rbac::auth_domain::local:
            rb_hash_aset(user, rb_id2sym(rb_intern("domain")), rb_id2sym(rb_intern("local")));
            break;
        case couchbase::operations::management::rbac::auth_domain::external:
            rb_hash_aset(user, rb_id2sym(rb_intern("domain")), rb_id2sym(rb_intern("external")));
            break;
        case couchbase::operations::management::rbac::auth_domain::unknown:
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
        rb_hash_aset(user, rb_id2sym(rb_intern("display_name")), cb_str_new(entry.display_name.value()));
    }
    if (entry.password_changed) {
        rb_hash_aset(user, rb_id2sym(rb_intern("password_changed")), cb_str_new(entry.password_changed.value()));
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

static VALUE
cb_Backend_user_get_all(VALUE self, VALUE domain, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(domain, T_SYMBOL);

    try {
        couchbase::operations::management::user_get_all_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::user_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::user_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to fetch users");
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.users.size()));
        for (const auto& entry : resp.users) {
            VALUE user = rb_hash_new();
            cb_extract_user(entry, user);
            rb_ary_push(res, user);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_user_get(VALUE self, VALUE domain, VALUE username, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(domain, T_SYMBOL);
    Check_Type(username, T_STRING);

    try {
        couchbase::operations::management::user_get_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        req.username = cb_string_new(username);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::user_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::operations::management::user_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to fetch user "{}")", req.username));
        }

        VALUE res = rb_hash_new();
        cb_extract_user(resp.user, res);
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_user_drop(VALUE self, VALUE domain, VALUE username, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(domain, T_SYMBOL);
    Check_Type(username, T_STRING);

    try {
        couchbase::operations::management::user_drop_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        req.username = cb_string_new(username);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::user_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::operations::management::user_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to fetch user "{}")", req.username));
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_user_upsert(VALUE self, VALUE domain, VALUE user, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(domain, T_SYMBOL);
    Check_Type(user, T_HASH);

    try {
        couchbase::operations::management::user_upsert_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::operations::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        VALUE name = rb_hash_aref(user, rb_id2sym(rb_intern("username")));
        if (NIL_P(name) || TYPE(name) != T_STRING) {
            throw ruby_exception(eInvalidArgument, "unable to upsert user: missing name");
        }
        req.user.username = cb_string_new(name);
        if (VALUE display_name = rb_hash_aref(user, rb_id2sym(rb_intern("display_name")));
            !NIL_P(display_name) && TYPE(display_name) == T_STRING) {
            req.user.display_name = cb_string_new(display_name);
        }
        if (VALUE password = rb_hash_aref(user, rb_id2sym(rb_intern("password"))); !NIL_P(password) && TYPE(password) == T_STRING) {
            req.user.password = cb_string_new(password);
        }
        if (VALUE groups = rb_hash_aref(user, rb_id2sym(rb_intern("groups"))); !NIL_P(groups) && TYPE(groups) == T_ARRAY) {
            auto groups_size = static_cast<size_t>(RARRAY_LEN(groups));
            for (size_t i = 0; i < groups_size; ++i) {
                if (VALUE entry = rb_ary_entry(groups, static_cast<long>(i)); TYPE(entry) == T_STRING) {
                    req.user.groups.emplace(cb_string_new(entry));
                }
            }
        }
        if (VALUE roles = rb_hash_aref(user, rb_id2sym(rb_intern("roles"))); !NIL_P(roles) && TYPE(roles) == T_ARRAY) {
            auto roles_size = static_cast<size_t>(RARRAY_LEN(roles));
            req.user.roles.reserve(roles_size);
            for (size_t i = 0; i < roles_size; ++i) {
                VALUE entry = rb_ary_entry(roles, static_cast<long>(i));
                if (TYPE(entry) == T_HASH) {
                    couchbase::operations::management::rbac::role role{};
                    VALUE role_name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
                    role.name = cb_string_new(role_name);
                    VALUE bucket = rb_hash_aref(entry, rb_id2sym(rb_intern("bucket")));
                    if (!NIL_P(bucket) && TYPE(bucket) == T_STRING) {
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

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::user_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::user_upsert_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format(R"(unable to upsert user "{}" ({}))", req.user.username, fmt::join(resp.errors, ", ")));
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_group(const couchbase::operations::management::rbac::group& entry, VALUE group)
{
    rb_hash_aset(group, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
    if (entry.description) {
        rb_hash_aset(group, rb_id2sym(rb_intern("description")), cb_str_new(entry.description.value()));
    }
    if (entry.ldap_group_reference) {
        rb_hash_aset(group, rb_id2sym(rb_intern("ldap_group_reference")), cb_str_new(entry.ldap_group_reference.value()));
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

static VALUE
cb_Backend_group_get_all(VALUE self, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::group_get_all_request req{};
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::group_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::group_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to fetch groups");
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.groups.size()));
        for (const auto& entry : resp.groups) {
            VALUE group = rb_hash_new();
            cb_extract_group(entry, group);
            rb_ary_push(res, group);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_group_get(VALUE self, VALUE name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(name, T_STRING);

    try {
        couchbase::operations::management::group_get_request req{};
        cb_extract_timeout(req, timeout);
        req.name = cb_string_new(name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::group_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::operations::management::group_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to fetch group "{}")", req.name));
        }

        VALUE res = rb_hash_new();
        cb_extract_group(resp.group, res);
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_group_drop(VALUE self, VALUE name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(name, T_STRING);

    try {
        couchbase::operations::management::group_drop_request req{};
        cb_extract_timeout(req, timeout);
        req.name = cb_string_new(name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::group_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::operations::management::group_drop_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to drop group "{}")", req.name));
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_group_upsert(VALUE self, VALUE group, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(group, T_HASH);

    try {
        couchbase::operations::management::group_upsert_request req{};
        cb_extract_timeout(req, timeout);

        if (VALUE name = rb_hash_aref(group, rb_id2sym(rb_intern("name"))); !NIL_P(name) && TYPE(name) == T_STRING) {
            req.group.name = cb_string_new(name);
        } else {
            throw ruby_exception(eInvalidArgument, "unable to upsert group: missing name");
        }
        if (VALUE ldap_group_ref = rb_hash_aref(group, rb_id2sym(rb_intern("ldap_group_reference")));
            !NIL_P(ldap_group_ref) && TYPE(ldap_group_ref) == T_STRING) {
            req.group.ldap_group_reference = cb_string_new(ldap_group_ref);
        }
        if (VALUE description = rb_hash_aref(group, rb_id2sym(rb_intern("description")));
            !NIL_P(description) && TYPE(description) == T_STRING) {
            req.group.description = cb_string_new(description);
        }
        if (VALUE roles = rb_hash_aref(group, rb_id2sym(rb_intern("roles"))); !NIL_P(roles) && TYPE(roles) == T_ARRAY) {
            auto roles_size = static_cast<size_t>(RARRAY_LEN(roles));
            req.group.roles.reserve(roles_size);
            for (size_t i = 0; i < roles_size; ++i) {
                if (VALUE entry = rb_ary_entry(roles, static_cast<long>(i)); TYPE(entry) == T_HASH) {
                    couchbase::operations::management::rbac::role role{};
                    VALUE role_name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
                    role.name = cb_string_new(role_name);
                    if (VALUE bucket = rb_hash_aref(entry, rb_id2sym(rb_intern("bucket"))); !NIL_P(bucket) && TYPE(bucket) == T_STRING) {
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
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::group_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::group_upsert_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to upsert group "{}" ({}))", req.group.name, fmt::join(resp.errors, ", ")));
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_cluster_enable_developer_preview(VALUE self)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::cluster_developer_preview_enable_request req{};
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::cluster_developer_preview_enable_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::cluster_developer_preview_enable_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to enable developer preview for this cluster");
        }
        spdlog::critical(
          "Developer preview cannot be disabled once it is enabled. If you enter developer preview mode you will not be able to "
          "upgrade. DO NOT USE IN PRODUCTION.");
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_scope_get_all(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::scope_get_all_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::scope_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::scope_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to get list of the scopes of the bucket \"{}\"", req.bucket_name));
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("uid")), ULL2NUM(resp.manifest.uid));
        VALUE scopes = rb_ary_new_capa(static_cast<long>(resp.manifest.scopes.size()));
        for (const auto& s : resp.manifest.scopes) {
            VALUE scope = rb_hash_new();
            rb_hash_aset(scope, rb_id2sym(rb_intern("uid")), ULL2NUM(s.uid));
            rb_hash_aset(scope, rb_id2sym(rb_intern("name")), cb_str_new(s.name));
            VALUE collections = rb_ary_new_capa(static_cast<long>(s.collections.size()));
            for (const auto& c : s.collections) {
                VALUE collection = rb_hash_new();
                rb_hash_aset(collection, rb_id2sym(rb_intern("uid")), ULL2NUM(c.uid));
                rb_hash_aset(collection, rb_id2sym(rb_intern("name")), cb_str_new(c.name));
                rb_ary_push(collections, collection);
            }
            rb_hash_aset(scope, rb_id2sym(rb_intern("collections")), collections);
            rb_ary_push(scopes, scope);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("scopes")), scopes);

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collections_manifest_get(VALUE self, VALUE bucket_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);

    try {
        couchbase::operations::management::collections_manifest_get_request req{ couchbase::document_id{
          cb_string_new(bucket_name), "_default", "_default", "" } };
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::collections_manifest_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::collections_manifest_get_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to get collections manifest of the bucket \"{}\"", req.id.bucket()));
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("uid")), ULL2NUM(resp.manifest.uid));
        VALUE scopes = rb_ary_new_capa(static_cast<long>(resp.manifest.scopes.size()));
        for (const auto& s : resp.manifest.scopes) {
            VALUE scope = rb_hash_new();
            rb_hash_aset(scope, rb_id2sym(rb_intern("uid")), ULL2NUM(s.uid));
            rb_hash_aset(scope, rb_id2sym(rb_intern("name")), cb_str_new(s.name));
            VALUE collections = rb_ary_new_capa(static_cast<long>(s.collections.size()));
            for (const auto& c : s.collections) {
                VALUE collection = rb_hash_new();
                rb_hash_aset(collection, rb_id2sym(rb_intern("uid")), ULL2NUM(c.uid));
                rb_hash_aset(collection, rb_id2sym(rb_intern("name")), cb_str_new(c.name));
                rb_ary_push(collections, collection);
            }
            rb_hash_aset(scope, rb_id2sym(rb_intern("collections")), collections);
            rb_ary_push(scopes, scope);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("scopes")), scopes);

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_scope_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::scope_create_request req{ cb_string_new(bucket_name), cb_string_new(scope_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::scope_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::scope_create_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format(R"(unable to create the scope "{}" on the bucket "{}")", req.scope_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_scope_drop(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::scope_drop_request req{ cb_string_new(bucket_name), cb_string_new(scope_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::scope_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::operations::management::scope_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format(R"(unable to drop the scope "{}" on the bucket "{}")", req.scope_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE max_expiry, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::collection_create_request req{ cb_string_new(bucket_name),
                                                                          cb_string_new(scope_name),
                                                                          cb_string_new(collection_name) };
        cb_extract_timeout(req, options);

        if (!NIL_P(max_expiry)) {
            Check_Type(max_expiry, T_FIXNUM);
            req.max_expiry = FIX2UINT(max_expiry);
        }
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::collection_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::collection_create_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable create the collection "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_drop(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::collection_drop_request req{ cb_string_new(bucket_name),
                                                                        cb_string_new(scope_name),
                                                                        cb_string_new(collection_name) };
        cb_extract_timeout(req, options);

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::collection_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::collection_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable to drop the collection  "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_get_all(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::query_index_get_all_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::query_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::query_index_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to get list of the indexes of the bucket \"{}\"", req.bucket_name));
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            rb_hash_aset(index, rb_id2sym(rb_intern("id")), cb_str_new(idx.id));
            rb_hash_aset(index, rb_id2sym(rb_intern("state")), cb_str_new(idx.state));
            rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
            rb_hash_aset(index, rb_id2sym(rb_intern("datastore_id")), cb_str_new(idx.datastore_id));
            rb_hash_aset(index, rb_id2sym(rb_intern("keyspace_id")), cb_str_new(idx.keyspace_id));
            rb_hash_aset(index, rb_id2sym(rb_intern("namespace_id")), cb_str_new(idx.namespace_id));
            rb_hash_aset(index, rb_id2sym(rb_intern("type")), cb_str_new(idx.type));
            rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
            VALUE index_key = rb_ary_new_capa(static_cast<long>(idx.index_key.size()));
            for (const auto& key : idx.index_key) {
                rb_ary_push(index_key, cb_str_new(key));
            }
            rb_hash_aset(index, rb_id2sym(rb_intern("index_key")), index_key);
            if (idx.scope_id) {
                rb_hash_aset(index, rb_id2sym(rb_intern("scope_id")), cb_str_new(idx.scope_id.value()));
            }
            if (idx.bucket_id) {
                rb_hash_aset(index, rb_id2sym(rb_intern("bucket_id")), cb_str_new(idx.bucket_id.value()));
            }
            if (idx.condition) {
                rb_hash_aset(index, rb_id2sym(rb_intern("condition")), cb_str_new(idx.condition.value()));
            }
            if (idx.partition) {
                rb_hash_aset(index, rb_id2sym(rb_intern("partition")), cb_str_new(idx.partition.value()));
            }
            rb_ary_push(indexes, index);
        }

        rb_hash_aset(res, rb_id2sym(rb_intern("indexes")), indexes);

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_create(VALUE self, VALUE bucket_name, VALUE index_name, VALUE fields, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_name, T_STRING);
    Check_Type(fields, T_ARRAY);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::query_index_create_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        req.index_name = cb_string_new(index_name);
        auto fields_num = static_cast<size_t>(RARRAY_LEN(fields));
        req.fields.reserve(fields_num);
        for (size_t i = 0; i < fields_num; ++i) {
            VALUE entry = rb_ary_entry(fields, static_cast<long>(i));
            cb_check_type(entry, T_STRING);
            req.fields.emplace_back(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry)));
        }
        if (!NIL_P(options)) {
            if (VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists"))); ignore_if_exists == Qtrue) {
                req.ignore_if_exists = true;
            } else if (ignore_if_exists == Qfalse) {
                req.ignore_if_exists = false;
            } /* else use backend default */
            if (VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred"))); deferred == Qtrue) {
                req.deferred = true;
            } else if (deferred == Qfalse) {
                req.deferred = false;
            } /* else use backend default */
            if (VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas"))); !NIL_P(num_replicas)) {
                req.num_replicas = NUM2UINT(num_replicas);
            } /* else use backend default */
            if (VALUE condition = rb_hash_aref(options, rb_id2sym(rb_intern("condition"))); !NIL_P(condition)) {
                req.condition.emplace(cb_string_new(condition));
            } /* else use backend default */
            if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name"))); TYPE(scope_name) == T_STRING) {
                req.scope_name = cb_string_new(scope_name);
            }
            if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name"))); TYPE(collection_name) == T_STRING) {
                req.collection_name = cb_string_new(collection_name);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::query_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::query_index_create_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to create index "{}" on the bucket "{}" ({}: {}))",
                                                req.index_name,
                                                req.bucket_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to create index "{}" on the bucket "{}")", req.index_name, req.bucket_name));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_drop(VALUE self, VALUE bucket_name, VALUE index_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(index_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::query_index_drop_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        req.index_name = cb_string_new(index_name);
        if (!NIL_P(options)) {
            if (VALUE ignore_if_does_not_exist = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
                ignore_if_does_not_exist == Qtrue) {
                req.ignore_if_does_not_exist = true;
            } else if (ignore_if_does_not_exist == Qfalse) {
                req.ignore_if_does_not_exist = false;
            } /* else use backend default */
            if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name"))); TYPE(scope_name) == T_STRING) {
                req.scope_name = cb_string_new(scope_name);
            }
            if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name"))); TYPE(collection_name) == T_STRING) {
                req.collection_name = cb_string_new(collection_name);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::query_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::query_index_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to drop index "{}" on the bucket "{}" ({}: {}))",
                                                req.index_name,
                                                req.bucket_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to drop index "{}" on the bucket "{}")", req.index_name, req.bucket_name));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_create_primary(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::query_index_create_request req{};
        cb_extract_timeout(req, options);
        req.is_primary = true;
        req.bucket_name = cb_string_new(bucket_name);
        if (!NIL_P(options)) {
            if (VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists"))); ignore_if_exists == Qtrue) {
                req.ignore_if_exists = true;
            } else if (ignore_if_exists == Qfalse) {
                req.ignore_if_exists = false;
            } /* else use backend default */
            if (VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred"))); deferred == Qtrue) {
                req.deferred = true;
            } else if (deferred == Qfalse) {
                req.deferred = false;
            } /* else use backend default */
            if (VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas"))); !NIL_P(num_replicas)) {
                req.num_replicas = NUM2UINT(num_replicas);
            } /* else use backend default */
            if (VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name"))); TYPE(index_name) == T_STRING) {
                req.index_name = cb_string_new(index_name);
            } /* else use backend default */
            if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name"))); TYPE(scope_name) == T_STRING) {
                req.scope_name = cb_string_new(scope_name);
            }
            if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name"))); TYPE(collection_name) == T_STRING) {
                req.collection_name = cb_string_new(collection_name);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::query_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::query_index_create_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to create primary index on the bucket "{}" ({}: {}))",
                                                req.bucket_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to create primary index on the bucket "{}")", req.bucket_name));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_drop_primary(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::query_index_drop_request req{};
        cb_extract_timeout(req, options);
        req.is_primary = true;
        req.bucket_name = cb_string_new(bucket_name);
        if (!NIL_P(options)) {
            if (VALUE ignore_if_does_not_exist = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
                ignore_if_does_not_exist == Qtrue) {
                req.ignore_if_does_not_exist = true;
            } else if (ignore_if_does_not_exist == Qfalse) {
                req.ignore_if_does_not_exist = false;
            } /* else use backend default */
            if (VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name"))); !NIL_P(index_name)) {
                cb_check_type(options, T_STRING);
                req.is_primary = false;
                req.bucket_name = cb_string_new(index_name);
            }
            if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name"))); TYPE(scope_name) == T_STRING) {
                req.scope_name = cb_string_new(scope_name);
            }
            if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name"))); TYPE(collection_name) == T_STRING) {
                req.collection_name = cb_string_new(collection_name);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::query_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::query_index_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format(
                    R"(unable to drop primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to drop primary index on the bucket "{}")", req.bucket_name));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        if (!resp.errors.empty()) {
            VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
            for (const auto& err : resp.errors) {
                VALUE error = rb_hash_new();
                rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
                rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
                rb_ary_push(errors, error);
            }
            rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_build_deferred(VALUE self, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::query_index_build_deferred_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::query_index_build_deferred_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::query_index_build_deferred_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format(
                    R"(unable to drop primary index on the bucket "{}" ({}: {}))", req.bucket_name, first_error.code, first_error.message));

            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to trigger build for deferred indexes for the bucket \"{}\"", req.bucket_name));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_query_index_watch(VALUE /* self */, VALUE bucket_name, VALUE index_names, VALUE timeout, VALUE options)
{
    Check_Type(bucket_name, T_STRING);
    Check_Type(index_names, T_ARRAY);
    Check_Type(timeout, T_FIXNUM);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    return Qtrue;
}

static void
cb_extract_search_index(VALUE index, const couchbase::operations::management::search_index& idx)
{
    rb_hash_aset(index, rb_id2sym(rb_intern("uuid")), cb_str_new(idx.uuid));
    rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
    rb_hash_aset(index, rb_id2sym(rb_intern("type")), cb_str_new(idx.type));
    if (!idx.params_json.empty()) {
        rb_hash_aset(index, rb_id2sym(rb_intern("params")), cb_str_new(idx.params_json));
    }

    if (!idx.source_uuid.empty()) {
        rb_hash_aset(index, rb_id2sym(rb_intern("source_uuid")), cb_str_new(idx.source_uuid));
    }
    if (!idx.source_name.empty()) {
        rb_hash_aset(index, rb_id2sym(rb_intern("source_name")), cb_str_new(idx.source_name));
    }
    rb_hash_aset(index, rb_id2sym(rb_intern("source_type")), cb_str_new(idx.source_type));
    if (!idx.source_params_json.empty()) {
        rb_hash_aset(index, rb_id2sym(rb_intern("source_params")), cb_str_new(idx.source_params_json));
    }
    if (!idx.plan_params_json.empty()) {
        rb_hash_aset(index, rb_id2sym(rb_intern("plan_params")), cb_str_new(idx.plan_params_json));
    }
}

static VALUE
cb_Backend_search_index_get_all(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::search_index_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::search_index_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to get list of the search indexes");
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        rb_hash_aset(res, rb_id2sym(rb_intern("impl_version")), cb_str_new(resp.impl_version));
        VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            cb_extract_search_index(index, idx);
            rb_ary_push(indexes, index);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("indexes")), indexes);
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_get(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_get_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::search_index_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to get search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format("unable to get search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        cb_extract_search_index(res, resp.index);
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_upsert(VALUE self, VALUE index_definition, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_definition, T_HASH);

    try {
        couchbase::operations::management::search_index_upsert_request req{};
        cb_extract_timeout(req, timeout);

        VALUE index_name = rb_hash_aref(index_definition, rb_id2sym(rb_intern("name")));
        cb_check_type(index_name, T_STRING);
        req.index.name = cb_string_new(index_name);

        VALUE index_type = rb_hash_aref(index_definition, rb_id2sym(rb_intern("type")));
        cb_check_type(index_type, T_STRING);
        req.index.type = cb_string_new(index_type);

        if (VALUE index_uuid = rb_hash_aref(index_definition, rb_id2sym(rb_intern("uuid"))); !NIL_P(index_uuid)) {
            cb_check_type(index_uuid, T_STRING);
            req.index.uuid = cb_string_new(index_uuid);
        }

        if (VALUE index_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("params"))); !NIL_P(index_params)) {
            cb_check_type(index_params, T_STRING);
            req.index.params_json = cb_string_new(index_params);
        }

        if (VALUE source_name = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_name"))); !NIL_P(source_name)) {
            cb_check_type(source_name, T_STRING);
            req.index.source_name = cb_string_new(source_name);
        }

        VALUE source_type = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_type")));
        cb_check_type(source_type, T_STRING);
        req.index.source_type = cb_string_new(source_type);

        if (VALUE source_uuid = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_uuid"))); !NIL_P(source_uuid)) {
            cb_check_type(source_uuid, T_STRING);
            req.index.source_uuid = cb_string_new(source_uuid);
        }

        if (VALUE source_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_params"))); !NIL_P(source_params)) {
            cb_check_type(source_params, T_STRING);
            req.index.source_params_json = cb_string_new(source_params);
        }

        if (VALUE plan_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("plan_params"))); !NIL_P(plan_params)) {
            cb_check_type(plan_params, T_STRING);
            req.index.plan_params_json = cb_string_new(plan_params);
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::search_index_upsert_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to upsert the search index \"{}\"", req.index.name));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format("unable to upsert the search index \"{}\": {}", req.index.name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_drop(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_drop_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::search_index_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to drop the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format("unable to drop the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_get_documents_count(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_get_documents_count_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_get_documents_count_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_get_documents_count_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(
                  resp.ctx, fmt::format("unable to get number of the indexed documents for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format("unable to get number of the indexed documents for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        rb_hash_aset(res, rb_id2sym(rb_intern("count")), ULL2NUM(resp.count));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_get_stats(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_get_stats_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_get_stats_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_get_stats_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to get stats for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to get stats for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        return cb_str_new(resp.stats);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_get_stats(VALUE self, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::search_index_stats_request req{};
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_stats_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::search_index_stats_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to get stats for the search service");
        }
        return cb_str_new(resp.stats);
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_pause_ingest(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_control_ingest_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.pause = true;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_control_ingest_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_control_ingest_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to pause ingest for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to pause ingest for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_resume_ingest(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_control_ingest_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.pause = false;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_control_ingest_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_control_ingest_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to resume ingest for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to resume ingest for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_allow_querying(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_control_query_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.allow = true;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_control_query_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_control_query_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to allow querying for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to allow querying for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_disallow_querying(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_control_query_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.allow = false;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_control_query_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_control_query_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to disallow querying for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to disallow querying for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_freeze_plan(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_control_plan_freeze_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.freeze = true;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_control_plan_freeze_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_control_plan_freeze_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to freeze for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format("unable to freeze for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_unfreeze_plan(VALUE self, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::operations::management::search_index_control_plan_freeze_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.freeze = false;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_control_plan_freeze_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_control_plan_freeze_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to unfreeze plan for the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to unfreeze for the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_analyze_document(VALUE self, VALUE index_name, VALUE encoded_document, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);
    Check_Type(encoded_document, T_STRING);

    try {
        couchbase::operations::management::search_index_analyze_document_request req{};
        cb_extract_timeout(req, timeout);

        req.index_name = cb_string_new(index_name);
        req.encoded_document = cb_string_new(encoded_document);

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::search_index_analyze_document_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::search_index_analyze_document_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to analyze document using the search index \"{}\"", req.index_name));
            } else {
                cb_throw_error_code(
                  resp.ctx, fmt::format("unable to analyze document using the search index \"{}\": {}", req.index_name, resp.error));
            }
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        rb_hash_aset(res, rb_id2sym(rb_intern("analysis")), cb_str_new(resp.analysis));
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_search(VALUE self, VALUE index_name, VALUE query, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);
    Check_Type(query, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::search_request req;
        if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id"))); !NIL_P(client_context_id)) {
            cb_check_type(client_context_id, T_STRING);
            req.client_context_id = cb_string_new(client_context_id);
        }
        cb_extract_timeout(req, options);
        req.index_name = cb_string_new(index_name);
        req.query = couchbase::utils::json::parse(cb_string_new(query));

        cb_extract_option_bool(req.explain, options, "explain");
        cb_extract_option_bool(req.disable_scoring, options, "disable_scoring");

        if (VALUE skip = rb_hash_aref(options, rb_id2sym(rb_intern("skip"))); !NIL_P(skip)) {
            cb_check_type(skip, T_FIXNUM);
            req.skip = FIX2ULONG(skip);
        }
        if (VALUE limit = rb_hash_aref(options, rb_id2sym(rb_intern("limit"))); !NIL_P(limit)) {
            cb_check_type(limit, T_FIXNUM);
            req.limit = FIX2ULONG(limit);
        }
        if (VALUE highlight_style = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_style"))); !NIL_P(highlight_style)) {
            cb_check_type(highlight_style, T_SYMBOL);
            ID type = rb_sym2id(highlight_style);
            if (type == rb_intern("html")) {
                req.highlight_style = couchbase::operations::search_request::highlight_style_type::html;
            } else if (type == rb_intern("ansi")) {
                req.highlight_style = couchbase::operations::search_request::highlight_style_type::ansi;
            }
        }

        if (VALUE highlight_fields = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_fields"))); !NIL_P(highlight_fields)) {
            cb_check_type(highlight_fields, T_ARRAY);
            auto highlight_fields_size = static_cast<size_t>(RARRAY_LEN(highlight_fields));
            req.highlight_fields.reserve(highlight_fields_size);
            for (size_t i = 0; i < highlight_fields_size; ++i) {
                VALUE field = rb_ary_entry(highlight_fields, static_cast<long>(i));
                cb_check_type(field, T_STRING);
                req.highlight_fields.emplace_back(cb_string_new(field));
            }
        }

        if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
            cb_check_type(scan_consistency, T_SYMBOL);
            if (ID type = rb_sym2id(scan_consistency); type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::operations::search_request::scan_consistency_type::not_bounded;
            }
        }

        if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state"))); !NIL_P(mutation_state)) {
            cb_check_type(mutation_state, T_ARRAY);
            auto state_size = static_cast<size_t>(RARRAY_LEN(mutation_state));
            req.mutation_state.reserve(state_size);
            for (size_t i = 0; i < state_size; ++i) {
                VALUE token = rb_ary_entry(mutation_state, static_cast<long>(i));
                cb_check_type(token, T_HASH);
                VALUE bucket_name = rb_hash_aref(token, rb_id2sym(rb_intern("bucket_name")));
                cb_check_type(bucket_name, T_STRING);
                VALUE partition_id = rb_hash_aref(token, rb_id2sym(rb_intern("partition_id")));
                cb_check_type(partition_id, T_FIXNUM);
                VALUE partition_uuid = rb_hash_aref(token, rb_id2sym(rb_intern("partition_uuid")));
                switch (TYPE(partition_uuid)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        break;
                    default:
                        throw ruby_exception(rb_eArgError, "partition_uuid must be an Integer");
                }
                VALUE sequence_number = rb_hash_aref(token, rb_id2sym(rb_intern("sequence_number")));
                switch (TYPE(sequence_number)) {
                    case T_FIXNUM:
                    case T_BIGNUM:
                        break;
                    default:
                        throw ruby_exception(rb_eArgError, "sequence_number must be an Integer");
                }
                req.mutation_state.emplace_back(couchbase::mutation_token{ NUM2ULL(partition_uuid),
                                                                           NUM2ULL(sequence_number),
                                                                           gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                                                           cb_string_new(bucket_name) });
            }
        }

        if (VALUE fields = rb_hash_aref(options, rb_id2sym(rb_intern("fields"))); !NIL_P(fields)) {
            cb_check_type(fields, T_ARRAY);
            auto fields_size = static_cast<size_t>(RARRAY_LEN(fields));
            req.fields.reserve(fields_size);
            for (size_t i = 0; i < fields_size; ++i) {
                VALUE field = rb_ary_entry(fields, static_cast<long>(i));
                cb_check_type(field, T_STRING);
                req.fields.emplace_back(cb_string_new(field));
            }
        }

        if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
            !NIL_P(scope_name) && TYPE(scope_name) == T_STRING) {
            req.scope_name.emplace(cb_string_new(scope_name));
            VALUE collections = rb_hash_aref(options, rb_id2sym(rb_intern("collections")));
            if (!NIL_P(collections)) {
                cb_check_type(collections, T_ARRAY);
                auto collections_size = static_cast<size_t>(RARRAY_LEN(collections));
                req.collections.reserve(collections_size);
                for (size_t i = 0; i < collections_size; ++i) {
                    VALUE collection = rb_ary_entry(collections, static_cast<long>(i));
                    cb_check_type(collection, T_STRING);
                    req.collections.emplace_back(cb_string_new(collection));
                }
            }
        }

        if (VALUE sort = rb_hash_aref(options, rb_id2sym(rb_intern("sort"))); !NIL_P(sort)) {
            cb_check_type(sort, T_ARRAY);
            for (size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(sort)); ++i) {
                VALUE sort_spec = rb_ary_entry(sort, static_cast<long>(i));
                req.sort_specs.emplace_back(cb_string_new(sort_spec));
            }
        }

        if (VALUE facets = rb_hash_aref(options, rb_id2sym(rb_intern("facets"))); !NIL_P(facets)) {
            cb_check_type(facets, T_ARRAY);
            for (size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(facets)); ++i) {
                VALUE facet_pair = rb_ary_entry(facets, static_cast<long>(i));
                cb_check_type(facet_pair, T_ARRAY);
                if (RARRAY_LEN(facet_pair) == 2) {
                    VALUE facet_name = rb_ary_entry(facet_pair, 0);
                    cb_check_type(facet_name, T_STRING);
                    VALUE facet_definition = rb_ary_entry(facet_pair, 1);
                    cb_check_type(facet_definition, T_STRING);
                    req.facets.try_emplace(cb_string_new(facet_name), cb_string_new(facet_definition));
                }
            }
        }

        if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters"))); !NIL_P(raw_params)) {
            cb_check_type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb_for_each_named_param), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::search_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::search_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to perform search query for index \"{}\": {}", req.index_name, resp.error));
        }
        VALUE res = rb_hash_new();

        VALUE meta_data = rb_hash_new();
        rb_hash_aset(meta_data, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.meta_data.client_context_id));

        VALUE metrics = rb_hash_new();
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("took")),
                     LL2NUM(std::chrono::duration_cast<std::chrono::milliseconds>(resp.meta_data.metrics.took).count()));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("total_rows")), ULL2NUM(resp.meta_data.metrics.total_rows));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("max_score")), DBL2NUM(resp.meta_data.metrics.max_score));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("success_partition_count")), ULL2NUM(resp.meta_data.metrics.success_partition_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("error_partition_count")), ULL2NUM(resp.meta_data.metrics.error_partition_count));
        rb_hash_aset(meta_data, rb_id2sym(rb_intern("metrics")), metrics);

        if (!resp.meta_data.errors.empty()) {
            VALUE errors = rb_hash_new();
            for (const auto& [code, message] : resp.meta_data.errors) {
                rb_hash_aset(errors, cb_str_new(code), cb_str_new(message));
            }
            rb_hash_aset(meta_data, rb_id2sym(rb_intern("errors")), errors);
        }

        rb_hash_aset(res, rb_id2sym(rb_intern("meta_data")), meta_data);

        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
        for (const auto& entry : resp.rows) {
            VALUE row = rb_hash_new();
            rb_hash_aset(row, rb_id2sym(rb_intern("index")), cb_str_new(entry.index));
            rb_hash_aset(row, rb_id2sym(rb_intern("id")), cb_str_new(entry.id));
            rb_hash_aset(row, rb_id2sym(rb_intern("score")), DBL2NUM(entry.score));
            VALUE locations = rb_ary_new_capa(static_cast<long>(entry.locations.size()));
            for (const auto& loc : entry.locations) {
                VALUE location = rb_hash_new();
                rb_hash_aset(row, rb_id2sym(rb_intern("field")), cb_str_new(loc.field));
                rb_hash_aset(row, rb_id2sym(rb_intern("term")), cb_str_new(loc.term));
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
                for (const auto& [field, field_fragments] : entry.fragments) {
                    VALUE fragments_list = rb_ary_new_capa(static_cast<long>(field_fragments.size()));
                    for (const auto& fragment : field_fragments) {
                        rb_ary_push(fragments_list, cb_str_new(fragment));
                    }
                    rb_hash_aset(fragments, cb_str_new(field), fragments_list);
                }
                rb_hash_aset(row, rb_id2sym(rb_intern("fragments")), fragments);
            }
            if (!entry.fields.empty()) {
                rb_hash_aset(row, rb_id2sym(rb_intern("fields")), cb_str_new(entry.fields));
            }
            if (!entry.explanation.empty()) {
                rb_hash_aset(row, rb_id2sym(rb_intern("explanation")), cb_str_new(entry.explanation));
            }
            rb_ary_push(rows, row);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);

        if (!resp.facets.empty()) {
            VALUE result_facets = rb_hash_new();
            for (const auto& entry : resp.facets) {
                VALUE facet = rb_hash_new();
                VALUE facet_name = cb_str_new(entry.name);
                rb_hash_aset(facet, rb_id2sym(rb_intern("name")), facet_name);
                rb_hash_aset(facet, rb_id2sym(rb_intern("field")), cb_str_new(entry.field));
                rb_hash_aset(facet, rb_id2sym(rb_intern("total")), ULL2NUM(entry.total));
                rb_hash_aset(facet, rb_id2sym(rb_intern("missing")), ULL2NUM(entry.missing));
                rb_hash_aset(facet, rb_id2sym(rb_intern("other")), ULL2NUM(entry.other));
                if (!entry.terms.empty()) {
                    VALUE terms = rb_ary_new_capa(static_cast<long>(entry.terms.size()));
                    for (const auto& item : entry.terms) {
                        VALUE term = rb_hash_new();
                        rb_hash_aset(term, rb_id2sym(rb_intern("term")), cb_str_new(item.term));
                        rb_hash_aset(term, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
                        rb_ary_push(terms, term);
                    }
                    rb_hash_aset(facet, rb_id2sym(rb_intern("terms")), terms);
                } else if (!entry.date_ranges.empty()) {
                    VALUE date_ranges = rb_ary_new_capa(static_cast<long>(entry.date_ranges.size()));
                    for (const auto& item : entry.date_ranges) {
                        VALUE date_range = rb_hash_new();
                        rb_hash_aset(date_range, rb_id2sym(rb_intern("name")), cb_str_new(item.name));
                        rb_hash_aset(date_range, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
                        if (item.start) {
                            rb_hash_aset(date_range, rb_id2sym(rb_intern("start_time")), cb_str_new(item.start.value()));
                        }
                        if (item.end) {
                            rb_hash_aset(date_range, rb_id2sym(rb_intern("end_time")), cb_str_new(item.end.value()));
                        }
                        rb_ary_push(date_ranges, date_range);
                    }
                    rb_hash_aset(facet, rb_id2sym(rb_intern("date_ranges")), date_ranges);
                } else if (!entry.numeric_ranges.empty()) {
                    VALUE numeric_ranges = rb_ary_new_capa(static_cast<long>(entry.numeric_ranges.size()));
                    for (const auto& item : entry.numeric_ranges) {
                        VALUE numeric_range = rb_hash_new();
                        rb_hash_aset(numeric_range, rb_id2sym(rb_intern("name")), cb_str_new(item.name));
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
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_dns_srv(VALUE self, VALUE hostname, VALUE service)
{
    (void)self;
    Check_Type(hostname, T_STRING);
    Check_Type(service, T_SYMBOL);

    bool tls = false;

    if (ID type = rb_sym2id(service); type == rb_intern("couchbase")) {
        tls = false;
    } else if (type == rb_intern("couchbases")) {
        tls = true;
    } else {
        rb_raise(rb_eArgError, "Unsupported service type: %+" PRIsVALUE, service);
        return Qnil;
    }

    try {
        asio::io_context ctx;

        couchbase::io::dns::dns_client client(ctx);
        std::string host_name = cb_string_new(hostname);
        std::string service_name("_couchbase");
        if (tls) {
            service_name = "_couchbases";
        }
        auto barrier = std::make_shared<std::promise<couchbase::io::dns::dns_client::dns_srv_response>>();
        auto f = barrier->get_future();
        client.query_srv(host_name, service_name, [barrier](couchbase::io::dns::dns_client::dns_srv_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        ctx.run();
        auto resp = cb_wait_for_future(f);
        if (resp.ec) {
            cb_throw_error_code(resp.ec, fmt::format("DNS SRV query failure for name \"{}\" (service: {})", host_name, service_name));
        }

        VALUE res = rb_ary_new();
        for (const auto& target : resp.targets) {
            VALUE addr = rb_hash_new();
            rb_hash_aset(addr, rb_id2sym(rb_intern("hostname")), cb_str_new(target.hostname));
            rb_hash_aset(addr, rb_id2sym(rb_intern("port")), UINT2NUM(target.port));
            rb_ary_push(res, addr);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_get_pending_mutations(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::analytics_get_pending_mutations_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_get_pending_mutations_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_get_pending_mutations_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, "unable to get pending mutations for the analytics service");
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format("unable to get pending mutations for the analytics service ({}: {})", first_error.code, first_error.message));
            }
        }
        VALUE res = rb_hash_new();
        for (const auto& [name, counter] : resp.stats) {
            rb_hash_aset(res, cb_str_new(name), ULL2NUM(counter));
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataset_get_all(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::analytics_dataset_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_dataset_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_dataset_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, "unable to fetch all datasets");
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx, fmt::format("unable to fetch all datasets ({}: {})", first_error.code, first_error.message));
            }
        }
        VALUE res = rb_ary_new_capa(static_cast<long>(resp.datasets.size()));
        for (const auto& ds : resp.datasets) {
            VALUE dataset = rb_hash_new();
            rb_hash_aset(dataset, rb_id2sym(rb_intern("name")), cb_str_new(ds.name));
            rb_hash_aset(dataset, rb_id2sym(rb_intern("dataverse_name")), cb_str_new(ds.dataverse_name));
            rb_hash_aset(dataset, rb_id2sym(rb_intern("link_name")), cb_str_new(ds.link_name));
            rb_hash_aset(dataset, rb_id2sym(rb_intern("bucket_name")), cb_str_new(ds.bucket_name));
            rb_ary_push(res, dataset);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataset_drop(VALUE self, VALUE dataset_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(dataset_name, T_STRING);

    try {
        couchbase::operations::management::analytics_dataset_drop_request req{};
        cb_extract_timeout(req, options);
        req.dataset_name = cb_string_new(dataset_name);
        VALUE dataverse_name = Qnil;
        cb_extract_option_string(dataverse_name, options, "dataverse_name");
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name = cb_string_new(dataverse_name);
        }
        cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_dataset_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_dataset_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to drop dataset `{}`.`{}`", req.dataverse_name, req.dataset_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to drop dataset `{}`.`{}` ({}: {})",
                                                req.dataverse_name,
                                                req.dataset_name,
                                                first_error.code,
                                                first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataset_create(VALUE self, VALUE dataset_name, VALUE bucket_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(dataset_name, T_STRING);
    Check_Type(bucket_name, T_STRING);

    try {
        couchbase::operations::management::analytics_dataset_create_request req{};
        cb_extract_timeout(req, options);
        req.dataset_name = cb_string_new(dataset_name);
        req.bucket_name = cb_string_new(bucket_name);
        cb_extract_option_string(req.condition, options, "condition");
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_dataset_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_dataset_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to create dataset `{}`.`{}`", req.dataverse_name, req.dataset_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to create dataset `{}`.`{}` ({}: {})",
                                                req.dataverse_name,
                                                req.dataset_name,
                                                first_error.code,
                                                first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataverse_drop(VALUE self, VALUE dataverse_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(dataverse_name, T_STRING);

    try {
        couchbase::operations::management::analytics_dataverse_drop_request req{};
        cb_extract_timeout(req, options);
        req.dataverse_name = cb_string_new(dataverse_name);
        cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_dataverse_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_dataverse_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to drop dataverse `{}`", req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format("unable to drop dataverse `{}` ({}: {})", req.dataverse_name, first_error.code, first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_dataverse_create(VALUE self, VALUE dataverse_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(dataverse_name, T_STRING);
    if (!NIL_P(dataverse_name)) {
        Check_Type(dataverse_name, T_STRING);
    }

    try {
        couchbase::operations::management::analytics_dataverse_create_request req{};
        cb_extract_timeout(req, options);
        req.dataverse_name = cb_string_new(dataverse_name);
        cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_dataverse_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_dataverse_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to create dataverse `{}`", req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format("unable to create dataverse `{}` ({}: {})", req.dataverse_name, first_error.code, first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_index_get_all(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::analytics_index_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_index_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, "unable to fetch all indexes");
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx, fmt::format("unable to fetch all indexes ({}: {})", first_error.code, first_error.message));
            }
        }
        VALUE res = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
            rb_hash_aset(index, rb_id2sym(rb_intern("dataset_name")), cb_str_new(idx.dataset_name));
            rb_hash_aset(index, rb_id2sym(rb_intern("dataverse_name")), cb_str_new(idx.dataverse_name));
            rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
            rb_ary_push(res, index);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_index_create(VALUE self, VALUE index_name, VALUE dataset_name, VALUE fields, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);
    Check_Type(dataset_name, T_STRING);
    Check_Type(fields, T_ARRAY);

    try {
        couchbase::operations::management::analytics_index_create_request req{};
        cb_extract_timeout(req, options);
        req.index_name = cb_string_new(index_name);
        req.dataset_name = cb_string_new(dataset_name);
        auto fields_num = static_cast<size_t>(RARRAY_LEN(fields));
        for (size_t i = 0; i < fields_num; ++i) {
            VALUE entry = rb_ary_entry(fields, static_cast<long>(i));
            Check_Type(entry, T_ARRAY);
            if (RARRAY_LEN(entry) == 2) {
                VALUE field = rb_ary_entry(entry, 0);
                VALUE type = rb_ary_entry(entry, 1);
                req.fields.try_emplace(cb_string_new(field), cb_string_new(type));
            }
        }

        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_index_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(
                  resp.ctx, fmt::format("unable to create index `{}` on `{}`.`{}`", req.index_name, req.dataverse_name, req.dataset_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to create index `{}` on `{}`.`{}` ({}: {})",
                                                req.index_name,
                                                req.dataverse_name,
                                                req.dataset_name,
                                                first_error.code,
                                                first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_index_drop(VALUE self, VALUE index_name, VALUE dataset_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);
    Check_Type(dataset_name, T_STRING);

    try {
        couchbase::operations::management::analytics_index_drop_request req{};
        cb_extract_timeout(req, options);
        req.index_name = cb_string_new(index_name);
        req.dataset_name = cb_string_new(dataset_name);
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::analytics_index_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(
                  resp.ctx, fmt::format("unable to drop index `{}`.`{}`.`{}`", req.dataverse_name, req.dataset_name, req.index_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to drop index `{}`.`{}`.`{}` ({}: {})",
                                                req.dataverse_name,
                                                req.dataset_name,
                                                req.index_name,
                                                first_error.code,
                                                first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_connect(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::analytics_link_connect_request req{};
        cb_extract_timeout(req, options);
        cb_extract_option_string(req.link_name, options, "link_name");
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.force, options, "force");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_connect_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_connect_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to connect link `{}` on `{}`", req.link_name, req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to connect link `{}` on `{}` ({}: {})",
                                                req.link_name,
                                                req.dataverse_name,
                                                first_error.code,
                                                first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_disconnect(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::operations::management::analytics_link_disconnect_request req{};
        cb_extract_timeout(req, options);
        cb_extract_option_string(req.link_name, options, "link_name");
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_disconnect_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_disconnect_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to disconnect link `{}` on `{}`", req.link_name, req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to disconnect link `{}` on `{}` ({}: {})",
                                                req.link_name,
                                                req.dataverse_name,
                                                first_error.code,
                                                first_error.message));
            }
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_fill_link(couchbase::operations::management::analytics_link::couchbase_remote& dst, VALUE src)
{
    cb_extract_option_string(dst.link_name, src, "link_name");
    cb_extract_option_string(dst.dataverse, src, "dataverse");
    cb_extract_option_string(dst.hostname, src, "hostname");
    cb_extract_option_string(dst.username, src, "username");
    cb_extract_option_string(dst.password, src, "password");
    VALUE encryption_level = Qnil;
    cb_extract_option_symbol(encryption_level, src, "encryption_level");
    if (NIL_P(encryption_level)) {
        encryption_level = rb_id2sym(rb_intern("none"));
    }
    if (ID level = rb_sym2id(encryption_level); level == rb_intern("none")) {
        dst.encryption.level = couchbase::operations::management::analytics_link::encryption_level::none;
    } else if (level == rb_intern("half")) {
        dst.encryption.level = couchbase::operations::management::analytics_link::encryption_level::half;
    } else if (level == rb_intern("full")) {
        dst.encryption.level = couchbase::operations::management::analytics_link::encryption_level::full;
    }
    cb_extract_option_string(dst.encryption.certificate, src, "certificate");
    cb_extract_option_string(dst.encryption.client_certificate, src, "client_certificate");
    cb_extract_option_string(dst.encryption.client_key, src, "client_key");
}

static void
cb_fill_link(couchbase::operations::management::analytics_link::azure_blob_external& dst, VALUE src)
{
    cb_extract_option_string(dst.link_name, src, "link_name");
    cb_extract_option_string(dst.dataverse, src, "dataverse");
    cb_extract_option_string(dst.connection_string, src, "connection_string");
    cb_extract_option_string(dst.account_name, src, "account_name");
    cb_extract_option_string(dst.account_key, src, "account_key");
    cb_extract_option_string(dst.shared_access_signature, src, "shared_access_signature");
    cb_extract_option_string(dst.blob_endpoint, src, "blob_endpoint");
    cb_extract_option_string(dst.endpoint_suffix, src, "endpoint_suffix");
}

static void
cb_fill_link(couchbase::operations::management::analytics_link::s3_external& dst, VALUE src)
{
    cb_extract_option_string(dst.link_name, src, "link_name");
    cb_extract_option_string(dst.dataverse, src, "dataverse");
    cb_extract_option_string(dst.access_key_id, src, "access_key_id");
    cb_extract_option_string(dst.secret_access_key, src, "secret_access_key");
    cb_extract_option_string(dst.session_token, src, "session_token");
    cb_extract_option_string(dst.region, src, "region");
    cb_extract_option_string(dst.service_endpoint, src, "service_endpoint");
}

static VALUE
cb_Backend_analytics_link_create(VALUE self, VALUE link, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        VALUE link_type = Qnil;
        cb_extract_option_symbol(link_type, link, "type");
        if (ID type = rb_sym2id(link_type); type == rb_intern("couchbase")) {
            couchbase::operations::management::analytics_link_create_request<
              couchbase::operations::management::analytics_link::couchbase_remote>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_create_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_create_response&& resp) {
                barrier->set_value(std::move(resp));
            });

            if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
                if (resp.errors.empty()) {
                    cb_throw_error_code(
                      resp.ctx, fmt::format("unable to create couchbase_remote link `{}` on `{}`", req.link.link_name, req.link.dataverse));
                } else {
                    const auto& first_error = resp.errors.front();
                    cb_throw_error_code(resp.ctx,
                                        fmt::format("unable to create couchbase_remote link `{}` on `{}` ({}: {})",
                                                    req.link.link_name,
                                                    req.link.dataverse,
                                                    first_error.code,
                                                    first_error.message));
                }
            }

        } else if (type == rb_intern("azureblob")) {
            couchbase::operations::management::analytics_link_create_request<
              couchbase::operations::management::analytics_link::azure_blob_external>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_create_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_create_response&& resp) {
                barrier->set_value(std::move(resp));
            });

            if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
                if (resp.errors.empty()) {
                    cb_throw_error_code(
                      resp.ctx,
                      fmt::format("unable to create azure_blob_external link `{}` on `{}`", req.link.link_name, req.link.dataverse));
                } else {
                    const auto& first_error = resp.errors.front();
                    cb_throw_error_code(resp.ctx,
                                        fmt::format("unable to create azure_blob_external link `{}` on `{}` ({}: {})",
                                                    req.link.link_name,
                                                    req.link.dataverse,
                                                    first_error.code,
                                                    first_error.message));
                }
            }

        } else if (type == rb_intern("s3")) {
            couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_create_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_create_response&& resp) {
                barrier->set_value(std::move(resp));
            });

            if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
                if (resp.errors.empty()) {
                    cb_throw_error_code(
                      resp.ctx, fmt::format("unable to create s3_external link `{}` on `{}`", req.link.link_name, req.link.dataverse));
                } else {
                    const auto& first_error = resp.errors.front();
                    cb_throw_error_code(resp.ctx,
                                        fmt::format("unable to create s3_external link `{}` on `{}` ({}: {})",
                                                    req.link.link_name,
                                                    req.link.dataverse,
                                                    first_error.code,
                                                    first_error.message));
                }
            }
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_replace(VALUE self, VALUE link, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        VALUE link_type = Qnil;
        cb_extract_option_symbol(link_type, link, "type");

        if (ID type = rb_sym2id(link_type); type == rb_intern("couchbase")) {
            couchbase::operations::management::analytics_link_replace_request<
              couchbase::operations::management::analytics_link::couchbase_remote>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_replace_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_replace_response&& resp) {
                barrier->set_value(std::move(resp));
            });

            if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
                if (resp.errors.empty()) {
                    cb_throw_error_code(
                      resp.ctx,
                      fmt::format("unable to replace couchbase_remote link `{}` on `{}`", req.link.link_name, req.link.dataverse));
                } else {
                    const auto& first_error = resp.errors.front();
                    cb_throw_error_code(resp.ctx,
                                        fmt::format("unable to replace couchbase_remote link `{}` on `{}` ({}: {})",
                                                    req.link.link_name,
                                                    req.link.dataverse,
                                                    first_error.code,
                                                    first_error.message));
                }
            }

        } else if (type == rb_intern("azureblob")) {
            couchbase::operations::management::analytics_link_replace_request<
              couchbase::operations::management::analytics_link::azure_blob_external>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_replace_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_replace_response&& resp) {
                barrier->set_value(std::move(resp));
            });

            if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
                if (resp.errors.empty()) {
                    cb_throw_error_code(
                      resp.ctx,
                      fmt::format("unable to replace azure_blob_external link `{}` on `{}`", req.link.link_name, req.link.dataverse));
                } else {
                    const auto& first_error = resp.errors.front();
                    cb_throw_error_code(resp.ctx,
                                        fmt::format("unable to replace azure_blob_external link `{}` on `{}` ({}: {})",
                                                    req.link.link_name,
                                                    req.link.dataverse,
                                                    first_error.code,
                                                    first_error.message));
                }
            }

        } else if (type == rb_intern("s3")) {
            couchbase::operations::management::analytics_link_replace_request<
              couchbase::operations::management::analytics_link::s3_external>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_replace_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_replace_response&& resp) {
                barrier->set_value(std::move(resp));
            });

            if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
                if (resp.errors.empty()) {
                    cb_throw_error_code(
                      resp.ctx, fmt::format("unable to replace s3_external link `{}` on `{}`", req.link.link_name, req.link.dataverse));
                } else {
                    const auto& first_error = resp.errors.front();
                    cb_throw_error_code(resp.ctx,
                                        fmt::format("unable to replace s3_external link `{}` on `{}` ({}: {})",
                                                    req.link.link_name,
                                                    req.link.dataverse,
                                                    first_error.code,
                                                    first_error.message));
                }
            }
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_drop(VALUE self, VALUE link, VALUE dataverse, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(link, T_STRING);
    Check_Type(dataverse, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::analytics_link_drop_request req{};
        cb_extract_timeout(req, options);

        req.link_name = cb_string_new(link);
        req.dataverse_name = cb_string_new(dataverse);

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::analytics_link_drop_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(resp.ctx, fmt::format("unable to drop link `{}` on `{}`", req.link_name, req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format(
                    "unable to drop link `{}` on `{}` ({}: {})", req.link_name, req.dataverse_name, first_error.code, first_error.message));
            }
        }

        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_analytics_link_get_all(VALUE self, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::management::analytics_link_get_all_request req{};
        cb_extract_timeout(req, options);

        cb_extract_option_string(req.link_type, options, "link_type");
        cb_extract_option_string(req.link_name, options, "link_name");
        cb_extract_option_string(req.dataverse_name, options, "dataverse");

        auto barrier = std::make_shared<std::promise<couchbase::operations::management::analytics_link_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::management::analytics_link_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);

        if (resp.ctx.ec) {
            if (resp.errors.empty()) {
                cb_throw_error_code(
                  resp.ctx,
                  fmt::format(
                    R"(unable to retrieve links type={}, dataverse="{}",  name="{}")", req.link_type, req.link_name, req.dataverse_name));
            } else {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to retrieve links type={}, dataverse="{}",  name="{}" ({}: {}))",
                                                req.link_type,
                                                req.link_name,
                                                req.dataverse_name,
                                                first_error.code,
                                                first_error.message));
            }
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.couchbase.size() + resp.s3.size() + resp.azure_blob.size()));
        for (const auto& link : resp.couchbase) {
            VALUE row = rb_hash_new();
            rb_hash_aset(row, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("couchbase")));
            rb_hash_aset(row, rb_id2sym(rb_intern("dataverse")), cb_str_new(link.dataverse));
            rb_hash_aset(row, rb_id2sym(rb_intern("link_name")), cb_str_new(link.link_name));
            rb_hash_aset(row, rb_id2sym(rb_intern("hostname")), cb_str_new(link.hostname));
            switch (link.encryption.level) {
                case couchbase::operations::management::analytics_link::encryption_level::none:
                    rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("none")));
                    break;
                case couchbase::operations::management::analytics_link::encryption_level::half:
                    rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("half")));
                    break;
                case couchbase::operations::management::analytics_link::encryption_level::full:
                    rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("full")));
                    break;
            }
            rb_hash_aset(row, rb_id2sym(rb_intern("username")), cb_str_new(link.username));
            rb_hash_aset(row, rb_id2sym(rb_intern("certificate")), cb_str_new(link.encryption.certificate));
            rb_hash_aset(row, rb_id2sym(rb_intern("client_certificate")), cb_str_new(link.encryption.client_certificate));
            rb_ary_push(res, row);
        }
        for (const auto& link : resp.s3) {
            VALUE row = rb_hash_new();
            rb_hash_aset(row, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("s3")));
            rb_hash_aset(row, rb_id2sym(rb_intern("dataverse")), cb_str_new(link.dataverse));
            rb_hash_aset(row, rb_id2sym(rb_intern("link_name")), cb_str_new(link.link_name));
            rb_hash_aset(row, rb_id2sym(rb_intern("access_key_id")), cb_str_new(link.access_key_id));
            rb_hash_aset(row, rb_id2sym(rb_intern("region")), cb_str_new(link.region));
            rb_hash_aset(row, rb_id2sym(rb_intern("service_endpoint")), cb_str_new(link.service_endpoint));
            rb_ary_push(res, row);
        }
        for (const auto& link : resp.azure_blob) {
            VALUE row = rb_hash_new();
            rb_hash_aset(row, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("azureblob")));
            rb_hash_aset(row, rb_id2sym(rb_intern("dataverse")), cb_str_new(link.dataverse));
            rb_hash_aset(row, rb_id2sym(rb_intern("link_name")), cb_str_new(link.link_name));
            rb_hash_aset(row, rb_id2sym(rb_intern("account_name")), cb_str_new(link.account_name));
            rb_hash_aset(row, rb_id2sym(rb_intern("blob_endpoint")), cb_str_new(link.blob_endpoint));
            rb_hash_aset(row, rb_id2sym(rb_intern("endpoint_suffix")), cb_str_new(link.endpoint_suffix));
            rb_ary_push(res, row);
        }

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}
static int
cb_for_each_named_param_analytics(VALUE key, VALUE value, VALUE arg)
{
    auto* preq = reinterpret_cast<couchbase::operations::analytics_request*>(arg);
    cb_check_type(key, T_STRING);
    cb_check_type(value, T_STRING);
    preq->named_parameters.emplace(std::string_view(RSTRING_PTR(key), static_cast<std::size_t>(RSTRING_LEN(key))),
                                   couchbase::utils::json::parse(RSTRING_PTR(value), static_cast<std::size_t>(RSTRING_LEN(value))));
    return ST_CONTINUE;
}

static VALUE
cb_Backend_document_analytics(VALUE self, VALUE statement, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(statement, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::analytics_request req;
        req.statement = cb_string_new(statement);
        if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id"))); !NIL_P(client_context_id)) {
            cb_check_type(client_context_id, T_STRING);
            req.client_context_id = cb_string_new(client_context_id);
        }
        cb_extract_timeout(req, options);
        cb_extract_option_bool(req.readonly, options, "readonly");
        cb_extract_option_bool(req.priority, options, "priority");
        if (VALUE positional_params = rb_hash_aref(options, rb_id2sym(rb_intern("positional_parameters"))); !NIL_P(positional_params)) {
            cb_check_type(positional_params, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(positional_params));
            req.positional_parameters.reserve(entries_num);
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
                cb_check_type(entry, T_STRING);
                req.positional_parameters.emplace_back(
                  couchbase::utils::json::parse(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry))));
            }
        }
        if (VALUE named_params = rb_hash_aref(options, rb_id2sym(rb_intern("named_parameters"))); !NIL_P(named_params)) {
            cb_check_type(named_params, T_HASH);
            rb_hash_foreach(named_params, INT_FUNC(cb_for_each_named_param_analytics), reinterpret_cast<VALUE>(&req));
        }
        if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
            cb_check_type(scan_consistency, T_SYMBOL);
            if (ID type = rb_sym2id(scan_consistency); type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::operations::analytics_request::scan_consistency_type::not_bounded;
            } else if (type == rb_intern("request_plus")) {
                req.scan_consistency = couchbase::operations::analytics_request::scan_consistency_type::request_plus;
            }
        }

        if (VALUE scope_qualifier = rb_hash_aref(options, rb_id2sym(rb_intern("scope_qualifier")));
            !NIL_P(scope_qualifier) && TYPE(scope_qualifier) == T_STRING) {
            req.scope_qualifier.emplace(cb_string_new(scope_qualifier));
        } else {
            VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
            if (!NIL_P(scope_name) && TYPE(scope_name) == T_STRING) {
                req.scope_name.emplace(cb_string_new(scope_name));
                VALUE bucket_name = rb_hash_aref(options, rb_id2sym(rb_intern("bucket_name")));
                if (NIL_P(bucket_name)) {
                    throw ruby_exception(
                      eInvalidArgument,
                      fmt::format("bucket must be specified for analytics query in scope \"{}\"", req.scope_name.value()));
                }
                req.bucket_name.emplace(cb_string_new(bucket_name));
            }
        }

        if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters"))); !NIL_P(raw_params)) {
            cb_check_type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb_for_each_named_param_analytics), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::analytics_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::analytics_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.payload.meta_data.errors && !resp.payload.meta_data.errors->empty()) {
                const auto& first_error = resp.payload.meta_data.errors->front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to execute analytics query ({}: {})", first_error.code, first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, "unable to execute analytics query");
            }
        }
        VALUE res = rb_hash_new();
        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.payload.rows.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
        for (const auto& row : resp.payload.rows) {
            rb_ary_push(rows, cb_str_new(row));
        }
        VALUE meta = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
        rb_hash_aset(meta,
                     rb_id2sym(rb_intern("status")),
                     rb_id2sym(rb_intern2(resp.payload.meta_data.status.data(), static_cast<long>(resp.payload.meta_data.status.size()))));
        rb_hash_aset(meta, rb_id2sym(rb_intern("request_id")), cb_str_new(resp.payload.meta_data.request_id));
        rb_hash_aset(meta, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.payload.meta_data.client_context_id));
        if (resp.payload.meta_data.signature) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("signature")), cb_str_new(resp.payload.meta_data.signature.value()));
        }
        if (resp.payload.meta_data.profile) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("profile")), cb_str_new(resp.payload.meta_data.profile.value()));
        }
        VALUE metrics = rb_hash_new();
        rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
        rb_hash_aset(metrics, rb_id2sym(rb_intern("elapsed_time")), cb_str_new(resp.payload.meta_data.metrics.elapsed_time));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("execution_time")), cb_str_new(resp.payload.meta_data.metrics.execution_time));
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
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
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
        rb_hash_aset(res, rb_id2sym(rb_intern("scheme")), cb_str_new(connstr.scheme));
        rb_hash_aset(res, rb_id2sym(rb_intern("tls")), connstr.tls ? Qtrue : Qfalse);
    }

    VALUE nodes = rb_ary_new_capa(static_cast<long>(connstr.bootstrap_nodes.size()));
    for (const auto& entry : connstr.bootstrap_nodes) {
        VALUE node = rb_hash_new();
        rb_hash_aset(node, rb_id2sym(rb_intern("address")), cb_str_new(entry.address));
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
    for (const auto& [name, value] : connstr.params) {
        rb_hash_aset(params, cb_str_new(name), cb_str_new(value));
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("params")), params);

    if (connstr.default_bucket_name) {
        rb_hash_aset(res, rb_id2sym(rb_intern("default_bucket_name")), cb_str_new(connstr.default_bucket_name.value()));
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
        rb_hash_aset(res, rb_id2sym(rb_intern("error")), cb_str_new(connstr.error.value()));
    }
    return res;
}

static VALUE
cb_Backend_view_index_get_all(VALUE self, VALUE bucket_name, VALUE name_space, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::operations::management::view_index_get_all_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.name_space = ns;
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::view_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::view_index_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to get list of the design documents");
        }
        VALUE res = rb_ary_new_capa(static_cast<long>(resp.design_documents.size()));
        for (const auto& entry : resp.design_documents) {
            VALUE dd = rb_hash_new();
            rb_hash_aset(dd, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
            rb_hash_aset(dd, rb_id2sym(rb_intern("rev")), cb_str_new(entry.rev));
            switch (entry.ns) {
                case couchbase::operations::design_document::name_space::development:
                    rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
                    break;
                case couchbase::operations::design_document::name_space::production:
                    rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("production")));
                    break;
            }
            VALUE views = rb_hash_new();
            for (const auto& [name, view_entry] : entry.views) {
                VALUE view_name = cb_str_new(name);
                VALUE view = rb_hash_new();
                rb_hash_aset(view, rb_id2sym(rb_intern("name")), view_name);
                if (view_entry.map) {
                    rb_hash_aset(view, rb_id2sym(rb_intern("map")), cb_str_new(view_entry.map.value()));
                }
                if (view_entry.reduce) {
                    rb_hash_aset(view, rb_id2sym(rb_intern("reduce")), cb_str_new(view_entry.reduce.value()));
                }
                rb_hash_aset(views, view_name, view);
            }
            rb_hash_aset(dd, rb_id2sym(rb_intern("views")), views);
            rb_ary_push(res, dd);
        }
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_view_index_get(VALUE self, VALUE bucket_name, VALUE document_name, VALUE name_space, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(document_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::operations::management::view_index_get_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document_name = cb_string_new(document_name);
        req.name_space = ns;
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::view_index_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::view_index_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(R"(unable to get design document "{}" ({}) on bucket "{}")", req.document_name, req.name_space, req.bucket_name));
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("name")), cb_str_new(resp.document.name));
        rb_hash_aset(res, rb_id2sym(rb_intern("rev")), cb_str_new(resp.document.rev));
        switch (resp.document.ns) {
            case couchbase::operations::design_document::name_space::development:
                rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
                break;
            case couchbase::operations::design_document::name_space::production:
                rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("production")));
                break;
        }
        VALUE views = rb_hash_new();
        for (const auto& [name, view_entry] : resp.document.views) {
            VALUE view_name = cb_str_new(name);
            VALUE view = rb_hash_new();
            rb_hash_aset(view, rb_id2sym(rb_intern("name")), view_name);
            if (view_entry.map) {
                rb_hash_aset(view, rb_id2sym(rb_intern("map")), cb_str_new(view_entry.map.value()));
            }
            if (view_entry.reduce) {
                rb_hash_aset(view, rb_id2sym(rb_intern("reduce")), cb_str_new(view_entry.reduce.value()));
            }
            rb_hash_aset(views, view_name, view);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("views")), views);
        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_view_index_drop(VALUE self, VALUE bucket_name, VALUE document_name, VALUE name_space, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(document_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::operations::management::view_index_drop_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document_name = cb_string_new(document_name);
        req.name_space = ns;
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::view_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::view_index_drop_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable to drop design document "{}" ({}) on bucket "{}")", req.document_name, req.name_space, req.bucket_name));
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_view_index_upsert(VALUE self, VALUE bucket_name, VALUE document, VALUE name_space, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(document, T_HASH);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::operations::management::view_index_upsert_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document.ns = ns;
        if (VALUE document_name = rb_hash_aref(document, rb_id2sym(rb_intern("name"))); !NIL_P(document_name)) {
            Check_Type(document_name, T_STRING);
            req.document.name = cb_string_new(document_name);
        }
        if (VALUE views = rb_hash_aref(document, rb_id2sym(rb_intern("views"))); !NIL_P(views)) {
            Check_Type(views, T_ARRAY);
            auto entries_num = static_cast<size_t>(RARRAY_LEN(views));
            for (size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(views, static_cast<long>(i));
                Check_Type(entry, T_HASH);
                couchbase::operations::design_document::view view;
                VALUE name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
                Check_Type(name, T_STRING);
                view.name = cb_string_new(name);
                if (VALUE map = rb_hash_aref(entry, rb_id2sym(rb_intern("map"))); !NIL_P(map)) {
                    view.map.emplace(cb_string_new(map));
                }
                if (VALUE reduce = rb_hash_aref(entry, rb_id2sym(rb_intern("reduce"))); !NIL_P(reduce)) {
                    view.reduce.emplace(cb_string_new(reduce));
                }
                req.document.views[view.name] = view;
            }
        }

        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::view_index_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::operations::management::view_index_upsert_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable to store design document "{}" ({}) on bucket "{}")", req.document.name, req.document.ns, req.bucket_name));
        }
        return Qtrue;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_view(VALUE self, VALUE bucket_name, VALUE design_document_name, VALUE view_name, VALUE name_space, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(design_document_name, T_STRING);
    Check_Type(view_name, T_STRING);
    Check_Type(name_space, T_SYMBOL);

    couchbase::operations::design_document::name_space ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::operations::design_document::name_space::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::operations::design_document::name_space::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::operations::document_view_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document_name = cb_string_new(design_document_name);
        req.view_name = cb_string_new(view_name);
        req.name_space = ns;
        cb_extract_timeout(req, options);
        if (!NIL_P(options)) {
            cb_extract_option_bool(req.debug, options, "debug");
            cb_extract_option_uint64(req.limit, options, "limit");
            cb_extract_option_uint64(req.skip, options, "skip");
            if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
                cb_check_type(scan_consistency, T_SYMBOL);
                if (ID consistency = rb_sym2id(scan_consistency); consistency == rb_intern("request_plus")) {
                    req.consistency = couchbase::operations::document_view_request::scan_consistency::request_plus;
                } else if (consistency == rb_intern("update_after")) {
                    req.consistency = couchbase::operations::document_view_request::scan_consistency::update_after;
                } else if (consistency == rb_intern("not_bounded")) {
                    req.consistency = couchbase::operations::document_view_request::scan_consistency::not_bounded;
                }
            }
            if (VALUE key = rb_hash_aref(options, rb_id2sym(rb_intern("key"))); !NIL_P(key)) {
                cb_check_type(key, T_STRING);
                req.key.emplace(cb_string_new(key));
            }
            if (VALUE start_key = rb_hash_aref(options, rb_id2sym(rb_intern("start_key"))); !NIL_P(start_key)) {
                cb_check_type(start_key, T_STRING);
                req.start_key.emplace(cb_string_new(start_key));
            }
            if (VALUE end_key = rb_hash_aref(options, rb_id2sym(rb_intern("end_key"))); !NIL_P(end_key)) {
                cb_check_type(end_key, T_STRING);
                req.end_key.emplace(cb_string_new(end_key));
            }
            if (VALUE start_key_doc_id = rb_hash_aref(options, rb_id2sym(rb_intern("start_key_doc_id"))); !NIL_P(start_key_doc_id)) {
                cb_check_type(start_key_doc_id, T_STRING);
                req.start_key_doc_id.emplace(cb_string_new(start_key_doc_id));
            }
            if (VALUE end_key_doc_id = rb_hash_aref(options, rb_id2sym(rb_intern("end_key_doc_id"))); !NIL_P(end_key_doc_id)) {
                cb_check_type(end_key_doc_id, T_STRING);
                req.end_key_doc_id.emplace(cb_string_new(end_key_doc_id));
            }
            if (VALUE inclusive_end = rb_hash_aref(options, rb_id2sym(rb_intern("inclusive_end"))); !NIL_P(inclusive_end)) {
                req.inclusive_end = RTEST(inclusive_end);
            }
            if (VALUE reduce = rb_hash_aref(options, rb_id2sym(rb_intern("reduce"))); !NIL_P(reduce)) {
                req.reduce = RTEST(reduce);
            }
            if (VALUE group = rb_hash_aref(options, rb_id2sym(rb_intern("group"))); !NIL_P(group)) {
                req.group = RTEST(group);
            }
            if (VALUE group_level = rb_hash_aref(options, rb_id2sym(rb_intern("group_level"))); !NIL_P(group_level)) {
                cb_check_type(group_level, T_FIXNUM);
                req.group_level = FIX2ULONG(group_level);
            }
            if (VALUE sort_order = rb_hash_aref(options, rb_id2sym(rb_intern("order"))); !NIL_P(sort_order)) {
                cb_check_type(sort_order, T_SYMBOL);
                if (ID order = rb_sym2id(sort_order); order == rb_intern("ascending")) {
                    req.order = couchbase::operations::document_view_request::sort_order::ascending;
                } else if (order == rb_intern("descending")) {
                    req.order = couchbase::operations::document_view_request::sort_order::descending;
                }
            }
            if (VALUE keys = rb_hash_aref(options, rb_id2sym(rb_intern("keys"))); !NIL_P(keys)) {
                cb_check_type(keys, T_ARRAY);
                auto entries_num = static_cast<size_t>(RARRAY_LEN(keys));
                req.keys.reserve(entries_num);
                for (size_t i = 0; i < entries_num; ++i) {
                    VALUE entry = rb_ary_entry(keys, static_cast<long>(i));
                    cb_check_type(entry, T_STRING);
                    req.keys.emplace_back(cb_string_new(entry));
                }
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::operations::document_view_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::document_view_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.error) {
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to execute view query {} ({}))", resp.error->code, resp.error->message));
            } else {
                cb_throw_error_code(resp.ctx, "unable to execute view query");
            }
        }
        VALUE res = rb_hash_new();

        VALUE meta = rb_hash_new();
        if (resp.meta_data.total_rows) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("total_rows")), ULL2NUM(*resp.meta_data.total_rows));
        }
        if (resp.meta_data.debug_info) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("debug_info")), cb_str_new(resp.meta_data.debug_info.value()));
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);

        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
        for (const auto& entry : resp.rows) {
            VALUE row = rb_hash_new();
            if (entry.id) {
                rb_hash_aset(row, rb_id2sym(rb_intern("id")), cb_str_new(entry.id.value()));
            }
            rb_hash_aset(row, rb_id2sym(rb_intern("key")), cb_str_new(entry.key));
            rb_hash_aset(row, rb_id2sym(rb_intern("value")), cb_str_new(entry.value));
            rb_ary_push(rows, row);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);

        return res;
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_set_log_level(VALUE self, VALUE log_level)
{
    (void)self;
    Check_Type(log_level, T_SYMBOL);
    if (ID type = rb_sym2id(log_level); type == rb_intern("trace")) {
        spdlog::set_level(spdlog::level::trace);
    } else if (type == rb_intern("debug")) {
        spdlog::set_level(spdlog::level::debug);
    } else if (type == rb_intern("info")) {
        spdlog::set_level(spdlog::level::info);
    } else if (type == rb_intern("warn")) {
        spdlog::set_level(spdlog::level::warn);
    } else if (type == rb_intern("error")) {
        spdlog::set_level(spdlog::level::err);
    } else if (type == rb_intern("critical")) {
        spdlog::set_level(spdlog::level::critical);
    } else if (type == rb_intern("off")) {
        spdlog::set_level(spdlog::level::off);
    } else {
        rb_raise(rb_eArgError, "Unsupported log level type: %+" PRIsVALUE, log_level);
        return Qnil;
    }
    return Qnil;
}

static VALUE
cb_Backend_get_log_level(VALUE self)
{
    (void)self;
    switch (spdlog::get_level()) {
        case spdlog::level::trace:
            return rb_id2sym(rb_intern("trace"));
        case spdlog::level::debug:
            return rb_id2sym(rb_intern("debug"));
        case spdlog::level::info:
            return rb_id2sym(rb_intern("info"));
        case spdlog::level::warn:
            return rb_id2sym(rb_intern("warn"));
        case spdlog::level::err:
            return rb_id2sym(rb_intern("error"));
        case spdlog::level::critical:
            return rb_id2sym(rb_intern("critical"));
        case spdlog::level::off:
            return rb_id2sym(rb_intern("off"));
        case spdlog::level::n_levels:
            return Qnil;
    }
    return Qnil;
}

static VALUE
cb_Backend_snappy_compress(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);

    std::string compressed{};
    std::size_t compressed_size = snappy::Compress(RSTRING_PTR(data), static_cast<std::size_t>(RSTRING_LEN(data)), &compressed);

    return rb_external_str_new(compressed.data(), static_cast<long>(compressed_size));
}

static VALUE
cb_Backend_snappy_uncompress(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);

    std::string uncompressed{};
    if (bool success = snappy::Uncompress(RSTRING_PTR(data), static_cast<std::size_t>(RSTRING_LEN(data)), &uncompressed); success) {
        return cb_str_new(uncompressed);
    }
    rb_raise(rb_eArgError, "Unable to decompress buffer");
    return Qnil;
}

static VALUE
cb_Backend_leb128_encode(VALUE self, VALUE number)
{
    (void)self;
    switch (TYPE(number)) {
        case T_FIXNUM:
        case T_BIGNUM:
            break;
        default:
            rb_raise(rb_eArgError, "The value must be a number");
    }
    couchbase::utils::unsigned_leb128<std::uint64_t> encoded(NUM2ULL(number));
    std::string buf = encoded.get();
    return cb_str_new(buf);
}

static VALUE
cb_Backend_leb128_decode(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    std::string buf(RSTRING_PTR(data), static_cast<std::size_t>(RSTRING_LEN(data)));
    if (buf.empty()) {
        rb_raise(rb_eArgError, "Unable to decode the buffer as LEB128: the buffer is empty");
    }

    auto [value, rest] = couchbase::utils::decode_unsigned_leb128<std::uint64_t>(buf, couchbase::utils::Leb128NoThrow());
    if (rest.data() != nullptr) {
        return ULL2NUM(value);
    }
    rb_raise(rb_eArgError, "Unable to decode the buffer as LEB128");
    return Qnil;
}

static VALUE
cb_Backend_query_escape(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto encoded = couchbase::utils::string_codec::v2::query_escape(cb_string_new(data));
    return cb_str_new(encoded);
}

static VALUE
cb_Backend_path_escape(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto encoded = couchbase::utils::string_codec::v2::path_escape(cb_string_new(data));
    return cb_str_new(encoded);
}

static int
cb_for_each_form_encode_value(VALUE key, VALUE value, VALUE arg)
{
    auto* values = reinterpret_cast<std::map<std::string, std::string>*>(arg);
    VALUE key_str = rb_obj_as_string(key);
    VALUE value_str = rb_obj_as_string(value);
    values->emplace(cb_string_new(key_str), cb_string_new(value_str));
    return ST_CONTINUE;
}

static VALUE
cb_Backend_form_encode(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_HASH);
    std::map<std::string, std::string> values{};
    rb_hash_foreach(data, INT_FUNC(cb_for_each_form_encode_value), reinterpret_cast<VALUE>(&values));
    auto encoded = couchbase::utils::string_codec::v2::form_encode(values);
    return cb_str_new(encoded);
}

static void
init_backend(VALUE mCouchbase)
{
    VALUE cBackend = rb_define_class_under(mCouchbase, "Backend", rb_cBasicObject);
    rb_define_alloc_func(cBackend, cb_Backend_allocate);
    rb_define_method(cBackend, "open", VALUE_FUNC(cb_Backend_open), 3);
    rb_define_method(cBackend, "close", VALUE_FUNC(cb_Backend_close), 0);
    rb_define_method(cBackend, "open_bucket", VALUE_FUNC(cb_Backend_open_bucket), 2);
    rb_define_method(cBackend, "diagnostics", VALUE_FUNC(cb_Backend_diagnostics), 1);
    rb_define_method(cBackend, "ping", VALUE_FUNC(cb_Backend_ping), 2);

    rb_define_method(cBackend, "document_get", VALUE_FUNC(cb_Backend_document_get), 5);
    rb_define_method(cBackend, "document_get_multi", VALUE_FUNC(cb_Backend_document_get_multi), 2);
    rb_define_method(cBackend, "document_get_projected", VALUE_FUNC(cb_Backend_document_get_projected), 5);
    rb_define_method(cBackend, "document_get_and_lock", VALUE_FUNC(cb_Backend_document_get_and_lock), 6);
    rb_define_method(cBackend, "document_get_and_touch", VALUE_FUNC(cb_Backend_document_get_and_touch), 6);
    rb_define_method(cBackend, "document_insert", VALUE_FUNC(cb_Backend_document_insert), 7);
    rb_define_method(cBackend, "document_replace", VALUE_FUNC(cb_Backend_document_replace), 7);
    rb_define_method(cBackend, "document_upsert", VALUE_FUNC(cb_Backend_document_upsert), 7);
    rb_define_method(cBackend, "document_upsert_multi", VALUE_FUNC(cb_Backend_document_upsert_multi), 2);
    rb_define_method(cBackend, "document_append", VALUE_FUNC(cb_Backend_document_append), 6);
    rb_define_method(cBackend, "document_prepend", VALUE_FUNC(cb_Backend_document_prepend), 6);
    rb_define_method(cBackend, "document_remove", VALUE_FUNC(cb_Backend_document_remove), 5);
    rb_define_method(cBackend, "document_remove_multi", VALUE_FUNC(cb_Backend_document_remove_multi), 2);
    rb_define_method(cBackend, "document_lookup_in", VALUE_FUNC(cb_Backend_document_lookup_in), 6);
    rb_define_method(cBackend, "document_mutate_in", VALUE_FUNC(cb_Backend_document_mutate_in), 6);
    rb_define_method(cBackend, "document_query", VALUE_FUNC(cb_Backend_document_query), 2);
    rb_define_method(cBackend, "document_touch", VALUE_FUNC(cb_Backend_document_touch), 6);
    rb_define_method(cBackend, "document_exists", VALUE_FUNC(cb_Backend_document_exists), 5);
    rb_define_method(cBackend, "document_unlock", VALUE_FUNC(cb_Backend_document_unlock), 6);
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

    rb_define_method(cBackend, "role_get_all", VALUE_FUNC(cb_Backend_role_get_all), 1);
    rb_define_method(cBackend, "user_get_all", VALUE_FUNC(cb_Backend_user_get_all), 2);
    rb_define_method(cBackend, "user_get", VALUE_FUNC(cb_Backend_user_get), 3);
    rb_define_method(cBackend, "user_drop", VALUE_FUNC(cb_Backend_user_drop), 3);
    rb_define_method(cBackend, "user_upsert", VALUE_FUNC(cb_Backend_user_upsert), 3);
    rb_define_method(cBackend, "group_get_all", VALUE_FUNC(cb_Backend_group_get_all), 1);
    rb_define_method(cBackend, "group_get", VALUE_FUNC(cb_Backend_group_get), 2);
    rb_define_method(cBackend, "group_drop", VALUE_FUNC(cb_Backend_group_drop), 2);
    rb_define_method(cBackend, "group_upsert", VALUE_FUNC(cb_Backend_group_upsert), 2);

    rb_define_method(cBackend, "cluster_enable_developer_preview!", VALUE_FUNC(cb_Backend_cluster_enable_developer_preview), 0);

    rb_define_method(cBackend, "scope_get_all", VALUE_FUNC(cb_Backend_scope_get_all), 2);
    rb_define_method(cBackend, "scope_create", VALUE_FUNC(cb_Backend_scope_create), 3);
    rb_define_method(cBackend, "scope_drop", VALUE_FUNC(cb_Backend_scope_drop), 3);
    rb_define_method(cBackend, "collection_create", VALUE_FUNC(cb_Backend_collection_create), 5);
    rb_define_method(cBackend, "collection_drop", VALUE_FUNC(cb_Backend_collection_drop), 4);

    rb_define_method(cBackend, "query_index_get_all", VALUE_FUNC(cb_Backend_query_index_get_all), 2);
    rb_define_method(cBackend, "query_index_create", VALUE_FUNC(cb_Backend_query_index_create), 4);
    rb_define_method(cBackend, "query_index_create_primary", VALUE_FUNC(cb_Backend_query_index_create_primary), 2);
    rb_define_method(cBackend, "query_index_drop", VALUE_FUNC(cb_Backend_query_index_drop), 3);
    rb_define_method(cBackend, "query_index_drop_primary", VALUE_FUNC(cb_Backend_query_index_drop_primary), 2);
    rb_define_method(cBackend, "query_index_build_deferred", VALUE_FUNC(cb_Backend_query_index_build_deferred), 2);
    rb_define_method(cBackend, "query_index_watch", VALUE_FUNC(cb_Backend_query_index_watch), 4);

    rb_define_method(cBackend, "search_get_stats", VALUE_FUNC(cb_Backend_search_get_stats), 1);
    rb_define_method(cBackend, "search_index_get_all", VALUE_FUNC(cb_Backend_search_index_get_all), 1);
    rb_define_method(cBackend, "search_index_get", VALUE_FUNC(cb_Backend_search_index_get), 2);
    rb_define_method(cBackend, "search_index_upsert", VALUE_FUNC(cb_Backend_search_index_upsert), 2);
    rb_define_method(cBackend, "search_index_drop", VALUE_FUNC(cb_Backend_search_index_drop), 2);
    rb_define_method(cBackend, "search_index_get_stats", VALUE_FUNC(cb_Backend_search_index_get_stats), 2);
    rb_define_method(cBackend, "search_index_get_documents_count", VALUE_FUNC(cb_Backend_search_index_get_documents_count), 2);
    rb_define_method(cBackend, "search_index_pause_ingest", VALUE_FUNC(cb_Backend_search_index_pause_ingest), 2);
    rb_define_method(cBackend, "search_index_resume_ingest", VALUE_FUNC(cb_Backend_search_index_resume_ingest), 2);
    rb_define_method(cBackend, "search_index_allow_querying", VALUE_FUNC(cb_Backend_search_index_allow_querying), 2);
    rb_define_method(cBackend, "search_index_disallow_querying", VALUE_FUNC(cb_Backend_search_index_disallow_querying), 2);
    rb_define_method(cBackend, "search_index_freeze_plan", VALUE_FUNC(cb_Backend_search_index_freeze_plan), 2);
    rb_define_method(cBackend, "search_index_unfreeze_plan", VALUE_FUNC(cb_Backend_search_index_unfreeze_plan), 2);
    rb_define_method(cBackend, "search_index_analyze_document", VALUE_FUNC(cb_Backend_search_index_analyze_document), 3);

    rb_define_method(cBackend, "analytics_get_pending_mutations", VALUE_FUNC(cb_Backend_analytics_get_pending_mutations), 1);
    rb_define_method(cBackend, "analytics_dataverse_drop", VALUE_FUNC(cb_Backend_analytics_dataverse_drop), 2);
    rb_define_method(cBackend, "analytics_dataverse_create", VALUE_FUNC(cb_Backend_analytics_dataverse_create), 2);
    rb_define_method(cBackend, "analytics_dataset_create", VALUE_FUNC(cb_Backend_analytics_dataset_create), 3);
    rb_define_method(cBackend, "analytics_dataset_drop", VALUE_FUNC(cb_Backend_analytics_dataset_drop), 2);
    rb_define_method(cBackend, "analytics_dataset_get_all", VALUE_FUNC(cb_Backend_analytics_dataset_get_all), 1);
    rb_define_method(cBackend, "analytics_index_get_all", VALUE_FUNC(cb_Backend_analytics_index_get_all), 1);
    rb_define_method(cBackend, "analytics_index_create", VALUE_FUNC(cb_Backend_analytics_index_create), 4);
    rb_define_method(cBackend, "analytics_index_drop", VALUE_FUNC(cb_Backend_analytics_index_drop), 3);
    rb_define_method(cBackend, "analytics_link_connect", VALUE_FUNC(cb_Backend_analytics_link_connect), 1);
    rb_define_method(cBackend, "analytics_link_disconnect", VALUE_FUNC(cb_Backend_analytics_link_disconnect), 1);
    rb_define_method(cBackend, "analytics_link_create", VALUE_FUNC(cb_Backend_analytics_link_create), 2);
    rb_define_method(cBackend, "analytics_link_replace", VALUE_FUNC(cb_Backend_analytics_link_replace), 2);
    rb_define_method(cBackend, "analytics_link_drop", VALUE_FUNC(cb_Backend_analytics_link_drop), 3);
    rb_define_method(cBackend, "analytics_link_get_all", VALUE_FUNC(cb_Backend_analytics_link_get_all), 1);

    rb_define_method(cBackend, "view_index_get_all", VALUE_FUNC(cb_Backend_view_index_get_all), 3);
    rb_define_method(cBackend, "view_index_get", VALUE_FUNC(cb_Backend_view_index_get), 4);
    rb_define_method(cBackend, "view_index_drop", VALUE_FUNC(cb_Backend_view_index_drop), 4);
    rb_define_method(cBackend, "view_index_upsert", VALUE_FUNC(cb_Backend_view_index_upsert), 4);

    /* utility function that are not intended for public usage */
    rb_define_method(cBackend, "collections_manifest_get", VALUE_FUNC(cb_Backend_collections_manifest_get), 2);
    rb_define_singleton_method(cBackend, "dns_srv", VALUE_FUNC(cb_Backend_dns_srv), 2);
    rb_define_singleton_method(cBackend, "parse_connection_string", VALUE_FUNC(cb_Backend_parse_connection_string), 1);
    rb_define_singleton_method(cBackend, "set_log_level", VALUE_FUNC(cb_Backend_set_log_level), 1);
    rb_define_singleton_method(cBackend, "get_log_level", VALUE_FUNC(cb_Backend_get_log_level), 0);
    rb_define_singleton_method(cBackend, "snappy_compress", VALUE_FUNC(cb_Backend_snappy_compress), 1);
    rb_define_singleton_method(cBackend, "snappy_uncompress", VALUE_FUNC(cb_Backend_snappy_uncompress), 1);
    rb_define_singleton_method(cBackend, "leb128_encode", VALUE_FUNC(cb_Backend_leb128_encode), 1);
    rb_define_singleton_method(cBackend, "leb128_decode", VALUE_FUNC(cb_Backend_leb128_decode), 1);
    rb_define_singleton_method(cBackend, "query_escape", VALUE_FUNC(cb_Backend_query_escape), 1);
    rb_define_singleton_method(cBackend, "path_escape", VALUE_FUNC(cb_Backend_path_escape), 1);
    rb_define_singleton_method(cBackend, "form_encode", VALUE_FUNC(cb_Backend_form_encode), 1);
}

void
init_logger()
{
    couchbase::logger::create_console_logger();
    if (auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_LOG_LEVEL"); !env_val.empty()) {
        couchbase::logger::set_log_levels(spdlog::level::from_str(env_val));
    }

    if (auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_DONT_INSTALL_TERMINATE_HANDLER"); env_val.empty()) {
        couchbase::platform::install_backtrace_terminate_handler();
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
