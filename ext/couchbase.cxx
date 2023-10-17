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

#include "core/utils/json.hxx"
#include "ext_build_info.hxx"
#include "ext_build_version.hxx"
#include <core/meta/version.hxx>

#include <asio.hpp>
#include <openssl/crypto.h>
#include <spdlog/cfg/env.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include <snappy.h>

#include <couchbase/fmt/retry_reason.hxx>
#include <couchbase/key_value_status_code.hxx>

#include <core/platform/terminate_handler.h>

#include <core/cluster.hxx>

#include <core/agent_group.hxx>
#include <core/design_document_namespace_fmt.hxx>
#include <core/logger/configuration.hxx>
#include <core/logger/logger.hxx>
#include <core/operations.hxx>

#include <core/operations/management/analytics.hxx>
#include <core/operations/management/bucket.hxx>
#include <core/operations/management/cluster_developer_preview_enable.hxx>
#include <core/operations/management/collections.hxx>
#include <core/operations/management/query.hxx>
#include <core/operations/management/search.hxx>
#include <core/operations/management/user.hxx>
#include <core/operations/management/view.hxx>

#include <core/impl/subdoc/path_flags.hxx>

#include <core/range_scan_options.hxx>
#include <core/range_scan_orchestrator.hxx>
#include <core/range_scan_orchestrator_options.hxx>

#include <core/io/dns_client.hxx>
#include <core/io/dns_config.hxx>
#include <core/utils/connection_string.hxx>
#include <core/utils/unsigned_leb128.hxx>

#include <couchbase/cluster.hxx>

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

#include <gsl/span>

#include <queue>

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

static inline std::vector<std::byte>
cb_binary_new(VALUE str)
{
    return couchbase::core::utils::to_binary(static_cast<const char*>(RSTRING_PTR(str)), static_cast<std::size_t>(RSTRING_LEN(str)));
}

template<typename StringLike>
static inline VALUE
cb_str_new(const StringLike str)
{
    return rb_external_str_new(std::data(str), static_cast<long>(std::size(str)));
}

static inline VALUE
cb_str_new(const std::vector<std::byte>& binary)
{
    return rb_external_str_new(reinterpret_cast<const char*>(binary.data()), static_cast<long>(binary.size()));
}

static inline VALUE
cb_str_new(const std::byte* data, std::size_t size)
{
    return rb_external_str_new(reinterpret_cast<const char*>(data), static_cast<long>(size));
}

static inline VALUE
cb_str_new(const char* data)
{
    return rb_external_str_new_cstr(data);
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
cb_cas_to_num(const couchbase::cas& cas)
{
    return ULL2NUM(cas.value());
}

static inline couchbase::cas
cb_num_to_cas(VALUE num)
{
    return couchbase::cas{ static_cast<std::uint64_t>(NUM2ULL(num)) };
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
    VALUE cb_CoreInfo = rb_hash_new();
    for (const auto& [name, value] : couchbase::core::meta::sdk_build_info()) {
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
    spdlog::debug("couchbase backend build info: {}",
                  std::string_view(RSTRING_PTR(build_info), static_cast<std::size_t>(RSTRING_LEN(build_info))));
}

template<typename Mutex>
class ruby_logger_sink : public spdlog::sinks::base_sink<Mutex>
{
  public:
    explicit ruby_logger_sink(VALUE ruby_logger)
      : ruby_logger_{ ruby_logger }
    {
    }

    void flush_deferred_messages()
    {
        std::lock_guard<Mutex> lock(spdlog::sinks::base_sink<Mutex>::mutex_);
        auto messages_ = std::move(deferred_messages_);
        while (!messages_.empty()) {
            write_message(messages_.front());
            messages_.pop();
        }
    }

    static VALUE map_log_level(spdlog::level::level_enum level)
    {
        switch (level) {
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
            default:
                break;
        }
        return Qnil;
    }

  protected:
    struct log_message_for_ruby {
        spdlog::level::level_enum level;
        spdlog::log_clock::time_point time;
        size_t thread_id;
        std::string payload;
        const char* filename;
        int line;
        const char* funcname;
    };

    void sink_it_(const spdlog::details::log_msg& msg) override
    {
        deferred_messages_.emplace(log_message_for_ruby{
          msg.level,
          msg.time,
          msg.thread_id,
          { msg.payload.begin(), msg.payload.end() },
          msg.source.filename,
          msg.source.line,
          msg.source.funcname,
        });
    }

    void flush_() override
    {
        /* do nothing here, the flush will be initiated by the SDK */
    }

  private:
    struct argument_pack {
        VALUE logger;
        const log_message_for_ruby& msg;
    };

    static VALUE invoke_log(VALUE arg)
    {
        auto* args = reinterpret_cast<argument_pack*>(arg);
        const auto& msg = args->msg;

        VALUE filename = Qnil;
        if (msg.filename != nullptr) {
            filename = cb_str_new(msg.filename);
        }
        VALUE line = Qnil;
        if (msg.line > 0) {
            line = ULL2NUM(msg.line);
        }
        VALUE function_name = Qnil;
        if (msg.funcname != nullptr) {
            function_name = cb_str_new(msg.funcname);
        }
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(msg.time.time_since_epoch());
        auto nanoseconds = msg.time.time_since_epoch() - seconds;
        return rb_funcall(args->logger,
                          rb_intern("log"),
                          8,
                          map_log_level(msg.level),
                          ULL2NUM(msg.thread_id),
                          ULL2NUM(seconds.count()),
                          ULL2NUM(nanoseconds.count()),
                          cb_str_new(msg.payload),
                          filename,
                          line,
                          function_name);
    }

    void write_message(const log_message_for_ruby& msg)
    {
        if (NIL_P(ruby_logger_)) {
            return;
        }
        argument_pack args{ ruby_logger_, msg };
        rb_rescue(invoke_log, reinterpret_cast<VALUE>(&args), nullptr, Qnil);
    }

    VALUE ruby_logger_{ Qnil };
    std::queue<log_message_for_ruby> deferred_messages_{};
};

using ruby_logger_sink_ptr = std::shared_ptr<ruby_logger_sink<std::mutex>>;

static ruby_logger_sink_ptr cb_global_sink{ nullptr };

struct cb_backend_data {
    std::unique_ptr<asio::io_context> ctx;
    std::shared_ptr<couchbase::core::cluster> cluster;
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
        backend->cluster.reset();
        backend->ctx.reset(nullptr);
    }
}

static void
cb_Backend_mark(void* /* ptr */)
{
    /* no embedded ruby objects -- no mark */
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
    backend->cluster = std::make_shared<couchbase::core::cluster>(*backend->ctx);
    backend->worker = std::thread([backend]() { backend->ctx->run(); });
    return obj;
}

static VALUE eClusterClosed;

static inline const std::shared_ptr<couchbase::core::cluster>&
cb_backend_to_cluster(VALUE self)
{
    const cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(eClusterClosed, "Cluster has been closed already");
    }
    return backend->cluster;
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
static VALUE eDocumentNotLocked;
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
static VALUE eMutationTokenOutdated;
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
static VALUE eEventingFunctionCompilationFailure;
static VALUE eEventingFunctionDeployed;
static VALUE eEventingFunctionIdentialKeyspace;
static VALUE eEventingFunctionNotBootstrapped;
static VALUE eEventingFunctionNotDeployed;
static VALUE eEventingFunctionNotFound;
static VALUE eEventingFunctionPaused;

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
    eCouchbaseError = rb_define_class_under(mError, "CouchbaseError", rb_eStandardError);

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
    eDocumentNotLocked = rb_define_class_under(mError, "DocumentNotLocked", eCouchbaseError);
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
    eMutationTokenOutdated = rb_define_class_under(mError, "MutationTokenOutdated", eCouchbaseError);
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
    eEventingFunctionCompilationFailure = rb_define_class_under(mError, "EventingFunctionCompilationFailure", eCouchbaseError);
    eEventingFunctionDeployed = rb_define_class_under(mError, "EventingFunctionDeployed", eCouchbaseError);
    eEventingFunctionIdentialKeyspace = rb_define_class_under(mError, "EventingFunctionIdentialKeyspace", eCouchbaseError);
    eEventingFunctionNotBootstrapped = rb_define_class_under(mError, "EventingFunctionNotBootstrapped", eCouchbaseError);
    eEventingFunctionNotDeployed = rb_define_class_under(mError, "EventingFunctionNotDeployed", eCouchbaseError);
    eEventingFunctionNotFound = rb_define_class_under(mError, "EventingFunctionNotFound", eCouchbaseError);
    eEventingFunctionPaused = rb_define_class_under(mError, "EventingFunctionPaused", eCouchbaseError);

    eBackendError = rb_define_class_under(mError, "BackendError", eCouchbaseError);
    eNetworkError = rb_define_class_under(mError, "NetworkError", eBackendError);
    eResolveFailure = rb_define_class_under(mError, "ResolveFailure", eNetworkError);
    eNoEndpointsLeft = rb_define_class_under(mError, "NoEndpointsLeft", eNetworkError);
    eHandshakeFailure = rb_define_class_under(mError, "HandshakeFailure", eNetworkError);
    eProtocolError = rb_define_class_under(mError, "ProtocolError", eNetworkError);
    eConfigurationNotAvailable = rb_define_class_under(mError, "ConfigurationNotAvailable", eNetworkError);
    eClusterClosed = rb_define_class_under(mError, "ClusterClosed", eCouchbaseError);
}

[[nodiscard]] static VALUE
cb_map_error_code(std::error_code ec, const std::string& message, bool include_error_code = true)
{
    std::string what = message;
    if (include_error_code) {
        what += fmt::format(": {}", ec.message());
    }

    if (ec.category() == couchbase::core::impl::common_category()) {
        switch (static_cast<couchbase::errc::common>(ec.value())) {
            case couchbase::errc::common::unambiguous_timeout:
                return rb_exc_new_cstr(eUnambiguousTimeout, what.c_str());

            case couchbase::errc::common::ambiguous_timeout:
                return rb_exc_new_cstr(eAmbiguousTimeout, what.c_str());

            case couchbase::errc::common::request_canceled:
                return rb_exc_new_cstr(eRequestCanceled, what.c_str());

            case couchbase::errc::common::invalid_argument:
                return rb_exc_new_cstr(eInvalidArgument, what.c_str());

            case couchbase::errc::common::service_not_available:
                return rb_exc_new_cstr(eServiceNotAvailable, what.c_str());

            case couchbase::errc::common::internal_server_failure:
                return rb_exc_new_cstr(eInternalServerFailure, what.c_str());

            case couchbase::errc::common::authentication_failure:
                return rb_exc_new_cstr(eAuthenticationFailure, what.c_str());

            case couchbase::errc::common::temporary_failure:
                return rb_exc_new_cstr(eTemporaryFailure, what.c_str());

            case couchbase::errc::common::parsing_failure:
                return rb_exc_new_cstr(eParsingFailure, what.c_str());

            case couchbase::errc::common::cas_mismatch:
                return rb_exc_new_cstr(eCasMismatch, what.c_str());

            case couchbase::errc::common::bucket_not_found:
                return rb_exc_new_cstr(eBucketNotFound, what.c_str());

            case couchbase::errc::common::scope_not_found:
                return rb_exc_new_cstr(eScopeNotFound, what.c_str());

            case couchbase::errc::common::collection_not_found:
                return rb_exc_new_cstr(eCollectionNotFound, what.c_str());

            case couchbase::errc::common::unsupported_operation:
                return rb_exc_new_cstr(eUnsupportedOperation, what.c_str());

            case couchbase::errc::common::feature_not_available:
                return rb_exc_new_cstr(eFeatureNotAvailable, what.c_str());

            case couchbase::errc::common::encoding_failure:
                return rb_exc_new_cstr(eEncodingFailure, what.c_str());

            case couchbase::errc::common::decoding_failure:
                return rb_exc_new_cstr(eDecodingFailure, what.c_str());

            case couchbase::errc::common::index_not_found:
                return rb_exc_new_cstr(eIndexNotFound, what.c_str());

            case couchbase::errc::common::index_exists:
                return rb_exc_new_cstr(eIndexExists, what.c_str());

            case couchbase::errc::common::rate_limited:
                return rb_exc_new_cstr(eRateLimited, what.c_str());

            case couchbase::errc::common::quota_limited:
                return rb_exc_new_cstr(eQuotaLimited, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::key_value_category()) {
        switch (static_cast<couchbase::errc::key_value>(ec.value())) {
            case couchbase::errc::key_value::document_not_found:
                return rb_exc_new_cstr(eDocumentNotFound, what.c_str());

            case couchbase::errc::key_value::document_irretrievable:
                return rb_exc_new_cstr(eDocumentIrretrievable, what.c_str());

            case couchbase::errc::key_value::document_locked:
                return rb_exc_new_cstr(eDocumentLocked, what.c_str());

            case couchbase::errc::key_value::document_not_locked:
                return rb_exc_new_cstr(eDocumentNotLocked, what.c_str());

            case couchbase::errc::key_value::value_too_large:
                return rb_exc_new_cstr(eValueTooLarge, what.c_str());

            case couchbase::errc::key_value::document_exists:
                return rb_exc_new_cstr(eDocumentExists, what.c_str());

            case couchbase::errc::key_value::durability_level_not_available:
                return rb_exc_new_cstr(eDurabilityLevelNotAvailable, what.c_str());

            case couchbase::errc::key_value::durability_impossible:
                return rb_exc_new_cstr(eDurabilityImpossible, what.c_str());

            case couchbase::errc::key_value::durability_ambiguous:
                return rb_exc_new_cstr(eDurabilityAmbiguous, what.c_str());

            case couchbase::errc::key_value::durable_write_in_progress:
                return rb_exc_new_cstr(eDurableWriteInProgress, what.c_str());

            case couchbase::errc::key_value::durable_write_re_commit_in_progress:
                return rb_exc_new_cstr(eDurableWriteReCommitInProgress, what.c_str());

            case couchbase::errc::key_value::mutation_token_outdated:
                return rb_exc_new_cstr(eMutationTokenOutdated, what.c_str());

            case couchbase::errc::key_value::path_not_found:
                return rb_exc_new_cstr(ePathNotFound, what.c_str());

            case couchbase::errc::key_value::path_mismatch:
                return rb_exc_new_cstr(ePathMismatch, what.c_str());

            case couchbase::errc::key_value::path_invalid:
                return rb_exc_new_cstr(ePathInvalid, what.c_str());

            case couchbase::errc::key_value::path_too_big:
                return rb_exc_new_cstr(ePathTooBig, what.c_str());

            case couchbase::errc::key_value::path_too_deep:
                return rb_exc_new_cstr(ePathTooDeep, what.c_str());

            case couchbase::errc::key_value::value_too_deep:
                return rb_exc_new_cstr(eValueTooDeep, what.c_str());

            case couchbase::errc::key_value::value_invalid:
                return rb_exc_new_cstr(eValueInvalid, what.c_str());

            case couchbase::errc::key_value::document_not_json:
                return rb_exc_new_cstr(eDocumentNotJson, what.c_str());

            case couchbase::errc::key_value::number_too_big:
                return rb_exc_new_cstr(eNumberTooBig, what.c_str());

            case couchbase::errc::key_value::delta_invalid:
                return rb_exc_new_cstr(eDeltaInvalid, what.c_str());

            case couchbase::errc::key_value::path_exists:
                return rb_exc_new_cstr(ePathExists, what.c_str());

            case couchbase::errc::key_value::xattr_unknown_macro:
                return rb_exc_new_cstr(eXattrUnknownMacro, what.c_str());

            case couchbase::errc::key_value::xattr_invalid_key_combo:
                return rb_exc_new_cstr(eXattrInvalidKeyCombo, what.c_str());

            case couchbase::errc::key_value::xattr_unknown_virtual_attribute:
                return rb_exc_new_cstr(eXattrUnknownVirtualAttribute, what.c_str());

            case couchbase::errc::key_value::xattr_cannot_modify_virtual_attribute:
                return rb_exc_new_cstr(eXattrCannotModifyVirtualAttribute, what.c_str());

            case couchbase::errc::key_value::xattr_no_access:
                return rb_exc_new_cstr(eXattrNoAccess, what.c_str());

            case couchbase::errc::key_value::cannot_revive_living_document:
                return rb_exc_new_cstr(eCannotReviveLivingDocument, what.c_str());

            case couchbase::errc::key_value::range_scan_completed:
                // Should not be exposed to the Ruby SDK, map it to a BackendError
                return rb_exc_new_cstr(eBackendError, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::query_category()) {
        switch (static_cast<couchbase::errc::query>(ec.value())) {
            case couchbase::errc::query::planning_failure:
                return rb_exc_new_cstr(ePlanningFailure, what.c_str());

            case couchbase::errc::query::index_failure:
                return rb_exc_new_cstr(eIndexFailure, what.c_str());

            case couchbase::errc::query::prepared_statement_failure:
                return rb_exc_new_cstr(ePreparedStatementFailure, what.c_str());

            case couchbase::errc::query::dml_failure:
                return rb_exc_new_cstr(eDmlFailure, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::search_category()) {
        switch (static_cast<couchbase::errc::search>(ec.value())) {
            case couchbase::errc::search::index_not_ready:
                return rb_exc_new_cstr(eIndexNotReady, what.c_str());
            case couchbase::errc::search::consistency_mismatch:
                return rb_exc_new_cstr(eConsistencyMismatch, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::view_category()) {
        switch (static_cast<couchbase::errc::view>(ec.value())) {
            case couchbase::errc::view::view_not_found:
                return rb_exc_new_cstr(eViewNotFound, what.c_str());

            case couchbase::errc::view::design_document_not_found:
                return rb_exc_new_cstr(eDesignDocumentNotFound, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::analytics_category()) {
        switch (static_cast<couchbase::errc::analytics>(ec.value())) {
            case couchbase::errc::analytics::compilation_failure:
                return rb_exc_new_cstr(eCompilationFailure, what.c_str());

            case couchbase::errc::analytics::job_queue_full:
                return rb_exc_new_cstr(eJobQueueFull, what.c_str());

            case couchbase::errc::analytics::dataset_not_found:
                return rb_exc_new_cstr(eDatasetNotFound, what.c_str());

            case couchbase::errc::analytics::dataverse_not_found:
                return rb_exc_new_cstr(eDataverseNotFound, what.c_str());

            case couchbase::errc::analytics::dataset_exists:
                return rb_exc_new_cstr(eDatasetExists, what.c_str());

            case couchbase::errc::analytics::dataverse_exists:
                return rb_exc_new_cstr(eDataverseExists, what.c_str());

            case couchbase::errc::analytics::link_not_found:
                return rb_exc_new_cstr(eLinkNotFound, what.c_str());

            case couchbase::errc::analytics::link_exists:
                return rb_exc_new_cstr(eLinkExists, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::management_category()) {
        switch (static_cast<couchbase::errc::management>(ec.value())) {
            case couchbase::errc::management::collection_exists:
                return rb_exc_new_cstr(eCollectionExists, what.c_str());

            case couchbase::errc::management::scope_exists:
                return rb_exc_new_cstr(eScopeExists, what.c_str());

            case couchbase::errc::management::user_not_found:
                return rb_exc_new_cstr(eUserNotFound, what.c_str());

            case couchbase::errc::management::group_not_found:
                return rb_exc_new_cstr(eGroupNotFound, what.c_str());

            case couchbase::errc::management::user_exists:
                return rb_exc_new_cstr(eUserExists, what.c_str());

            case couchbase::errc::management::bucket_exists:
                return rb_exc_new_cstr(eBucketExists, what.c_str());

            case couchbase::errc::management::bucket_not_flushable:
                return rb_exc_new_cstr(eBucketNotFlushable, what.c_str());

            case couchbase::errc::management::eventing_function_not_found:
                return rb_exc_new_cstr(eEventingFunctionNotFound, what.c_str());

            case couchbase::errc::management::eventing_function_not_deployed:
                return rb_exc_new_cstr(eEventingFunctionNotDeployed, what.c_str());

            case couchbase::errc::management::eventing_function_compilation_failure:
                return rb_exc_new_cstr(eEventingFunctionCompilationFailure, what.c_str());

            case couchbase::errc::management::eventing_function_identical_keyspace:
                return rb_exc_new_cstr(eEventingFunctionIdentialKeyspace, what.c_str());

            case couchbase::errc::management::eventing_function_not_bootstrapped:
                return rb_exc_new_cstr(eEventingFunctionNotBootstrapped, what.c_str());

            case couchbase::errc::management::eventing_function_deployed:
                return rb_exc_new_cstr(eEventingFunctionDeployed, what.c_str());

            case couchbase::errc::management::eventing_function_paused:
                return rb_exc_new_cstr(eEventingFunctionPaused, what.c_str());
        }
    } else if (ec.category() == couchbase::core::impl::network_category()) {
        switch (static_cast<couchbase::errc::network>(ec.value())) {
            case couchbase::errc::network::resolve_failure:
                return rb_exc_new_cstr(eResolveFailure, what.c_str());

            case couchbase::errc::network::no_endpoints_left:
                return rb_exc_new_cstr(eNoEndpointsLeft, what.c_str());

            case couchbase::errc::network::handshake_failure:
                return rb_exc_new_cstr(eHandshakeFailure, what.c_str());

            case couchbase::errc::network::protocol_error:
                return rb_exc_new_cstr(eProtocolError, what.c_str());

            case couchbase::errc::network::configuration_not_available:
                return rb_exc_new_cstr(eConfigurationNotAvailable, what.c_str());

            case couchbase::errc::network::cluster_closed:
                return rb_exc_new_cstr(eClusterClosed, what.c_str());
        }
    }

    return rb_exc_new_cstr(eBackendError, what.c_str());
}

[[noreturn]] static void
cb_throw_error_code(std::error_code ec, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ec, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::key_value_error_context& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec(), message);
    VALUE error_context = rb_hash_new();
    std::string error(ctx.ec().message());
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("id")), cb_str_new(ctx.id()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("scope")), cb_str_new(ctx.scope()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("collection")), cb_str_new(ctx.collection()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("bucket")), cb_str_new(ctx.bucket()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("opaque")), ULONG2NUM(ctx.opaque()));
    if (ctx.status_code()) {
        std::string status(fmt::format("{}", ctx.status_code().value()));
        rb_hash_aset(error_context, rb_id2sym(rb_intern("status")), cb_str_new(status));
    }
    if (ctx.error_map_info()) {
        VALUE error_map_info = rb_hash_new();
        rb_hash_aset(error_map_info, rb_id2sym(rb_intern("name")), cb_str_new(ctx.error_map_info()->name()));
        rb_hash_aset(error_map_info, rb_id2sym(rb_intern("desc")), cb_str_new(ctx.error_map_info()->description()));
        rb_hash_aset(error_context, rb_id2sym(rb_intern("error_map_info")), error_map_info);
    }
    if (ctx.extended_error_info()) {
        VALUE enhanced_error_info = rb_hash_new();
        rb_hash_aset(enhanced_error_info, rb_id2sym(rb_intern("reference")), cb_str_new(ctx.extended_error_info()->reference()));
        rb_hash_aset(enhanced_error_info, rb_id2sym(rb_intern("context")), cb_str_new(ctx.extended_error_info()->context()));
        rb_hash_aset(error_context, rb_id2sym(rb_intern("extended_error_info")), enhanced_error_info);
    }
    rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts()));
    if (!ctx.retry_reasons().empty()) {
        VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons().size()));
        for (const auto& reason : ctx.retry_reasons()) {
            auto reason_str = fmt::format("{}", reason);
            rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
        }
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
    }
    if (ctx.last_dispatched_to()) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to().value()));
    }
    if (ctx.last_dispatched_from()) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from().value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::subdocument_error_context& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(static_cast<const couchbase::key_value_error_context&>(ctx), message);
    VALUE error_context = rb_iv_get(exc, "@context");
    rb_hash_aset(error_context, rb_id2sym(rb_intern("deleted")), ctx.deleted() ? Qtrue : Qfalse);
    if (ctx.first_error_index()) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("first_error_index")), ULL2NUM(ctx.first_error_index().value()));
    }
    if (ctx.first_error_path()) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("first_error_path")), cb_str_new(ctx.first_error_path().value()));
    }
    return exc;
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::key_value_error_context& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[noreturn]] static void
cb_throw_error_code(const couchbase::subdocument_error_context& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::core::error_context::query& ctx, const std::string& message)
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
cb_throw_error_code(const couchbase::core::error_context::query& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::core::error_context::analytics& ctx, const std::string& message)
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
cb_throw_error_code(const couchbase::core::error_context::analytics& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::core::error_context::view& ctx, const std::string& message)
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
cb_throw_error_code(const couchbase::core::error_context::view& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::core::error_context::http& ctx, const std::string& message)
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
cb_throw_error_code(const couchbase::core::error_context::http& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::manager_error_context& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec(), message);
    VALUE error_context = rb_hash_new();
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(ctx.ec().message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("path")), cb_str_new(ctx.path()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.content()));
    if (ctx.retry_attempts() > 0) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_attempts")), INT2FIX(ctx.retry_attempts()));
        if (!ctx.retry_reasons().empty()) {
            VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons().size()));
            for (const auto& reason : ctx.retry_reasons()) {
                auto reason_str = fmt::format("{}", reason);
                rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
            }
            rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
        }
    }
    if (ctx.last_dispatched_to()) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_to")), cb_str_new(ctx.last_dispatched_to().value()));
    }
    if (ctx.last_dispatched_from()) {
        rb_hash_aset(error_context, rb_id2sym(rb_intern("last_dispatched_from")), cb_str_new(ctx.last_dispatched_from().value()));
    }
    rb_iv_set(exc, "@context", error_context);
    return exc;
}
[[noreturn]] static void
cb_throw_error_code(const couchbase::manager_error_context& ctx, const std::string& message)
{
    throw ruby_exception(cb_map_error_code(ctx, message));
}

[[nodiscard]] static VALUE
cb_map_error_code(const couchbase::core::error_context::search& ctx, const std::string& message)
{
    VALUE exc = cb_map_error_code(ctx.ec, message);
    VALUE error_context = rb_hash_new();
    std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("index_name")), cb_str_new(ctx.index_name));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("query")), cb_str_new(ctx.query));
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
cb_throw_error_code(const couchbase::core::error_context::search& ctx, const std::string& message)
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
    if (cb_global_sink) {
        cb_global_sink->flush_deferred_messages();
    }
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

namespace couchbase
{
namespace ruby
{

template<typename CommandOptions>
static void
set_timeout(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("timeout"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
            case T_BIGNUM:
                opts.timeout(std::chrono::milliseconds(NUM2ULL(val)));
                break;

            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("timeout must be an Integer, but given %+" PRIsVALUE, val));
        }
    }
}

enum class expiry_type {
    none,
    relative,
    absolute,
};

static std::pair<expiry_type, std::chrono::seconds>
unpack_expiry(VALUE val, bool allow_nil = true)
{
    if (TYPE(val) == T_FIXNUM || TYPE(val) == T_BIGNUM) {
        return { expiry_type::relative, std::chrono::seconds(NUM2ULL(val)) };
    }
    if (TYPE(val) != T_ARRAY || RARRAY_LEN(val) != 2) {
        throw ruby_exception(rb_eArgError, rb_sprintf("expected expiry to be Array[Symbol, Integer|nil], given %+" PRIsVALUE, val));
    }
    VALUE expiry = rb_ary_entry(val, 1);
    if (NIL_P(expiry)) {
        if (allow_nil) {
            return { expiry_type::none, {} };
        }
        throw ruby_exception(rb_eArgError, "expiry value must be nil");
    }
    if (TYPE(expiry) != T_FIXNUM && TYPE(expiry) != T_BIGNUM) {
        throw ruby_exception(rb_eArgError, rb_sprintf("expiry value must be an Integer, but given %+" PRIsVALUE, expiry));
    }
    auto duration = std::chrono::seconds(NUM2ULL(expiry));

    VALUE type = rb_ary_entry(val, 0);
    if (TYPE(type) != T_SYMBOL) {
        throw ruby_exception(rb_eArgError, rb_sprintf("expiry type must be a Symbol, but given %+" PRIsVALUE, type));
    }
    static VALUE duration_type = rb_id2sym(rb_intern("duration"));
    static VALUE time_point_type = rb_id2sym(rb_intern("time_point"));
    if (type == duration_type) {
        return { expiry_type::relative, duration };
    }
    if (type == time_point_type) {
        return { expiry_type::absolute, duration };
    }
    throw ruby_exception(rb_eArgError, rb_sprintf("unknown expiry type: %+" PRIsVALUE, type));
}

template<typename CommandOptions>
static void
set_expiry(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("expiry"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }

        switch (auto [type, duration] = unpack_expiry(val); type) {
            case expiry_type::relative:
                opts.expiry(duration);
                break;

            case expiry_type::absolute:
                opts.expiry(std::chrono::system_clock::time_point(duration));
                break;

            case expiry_type::none:
                break;
        }
    }
}

template<typename CommandOptions>
static void
set_access_deleted(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("access_deleted"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_TRUE:
                opts.access_deleted(true);
                break;
            case T_FALSE:
                opts.access_deleted(false);
                break;

            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("access_deleted must be an Boolean, but given %+" PRIsVALUE, val));
        }
    }
}

template<typename CommandOptions>
static void
set_create_as_deleted(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("create_as_deleted"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_TRUE:
                opts.create_as_deleted(true);
                break;
            case T_FALSE:
                opts.create_as_deleted(false);
                break;

            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("create_as_deleted must be an Boolean, but given %+" PRIsVALUE, val));
        }
    }
}

template<typename CommandOptions>
static void
set_preserve_expiry(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("preserve_expiry"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_TRUE:
                opts.preserve_expiry(true);
                break;
            case T_FALSE:
                opts.preserve_expiry(false);
                break;
            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("preserve_expiry must be a Boolean, but given %+" PRIsVALUE, val));
        }
    }
}

template<typename CommandOptions>
static void
set_cas(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("cas"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
            case T_BIGNUM:
                opts.cas(couchbase::cas{ static_cast<std::uint64_t>(NUM2ULL(val)) });
                break;

            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("cas must be an Integer, but given %+" PRIsVALUE, val));
        }
    }
}

template<typename CommandOptions>
static void
set_delta(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("delta"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
            case T_BIGNUM:
                opts.delta(NUM2ULL(val));
                break;

            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("delta must be an Integer, but given %+" PRIsVALUE, val));
        }
    }
}

template<typename CommandOptions>
static void
set_initial_value(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("initial_value"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
            case T_BIGNUM:
                opts.initial(NUM2ULL(val));
                break;

            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("initial_value must be an Integer, but given %+" PRIsVALUE, val));
        }
    }
}

static std::optional<couchbase::durability_level>
extract_durability_level(VALUE options)
{
    static VALUE property_name{ rb_id2sym(rb_intern("durability_level")) };
    if (VALUE val = rb_hash_aref(options, property_name); !NIL_P(val)) {
        ID level = rb_sym2id(val);
        if (level == rb_intern("none")) {
            return {};
        }
        if (level == rb_intern("majority")) {
            return couchbase::durability_level::majority;
        }
        if (level == rb_intern("majority_and_persist_to_active")) {
            return couchbase::durability_level::majority_and_persist_to_active;
        }
        if (level == rb_intern("persist_to_majority")) {
            return couchbase::durability_level::persist_to_majority;
        }
        throw ruby_exception(eInvalidArgument, rb_sprintf("unknown durability level: %+" PRIsVALUE, val));
    }
    return couchbase::durability_level::none;
}

static std::optional<couchbase::persist_to>
extract_legacy_durability_persist_to(VALUE options)
{
    static VALUE property_name{ rb_id2sym(rb_intern("persist_to")) };
    if (VALUE val = rb_hash_aref(options, property_name); !NIL_P(val)) {
        ID mode = rb_sym2id(val);
        if (mode == rb_intern("none")) {
            return {};
        }
        if (mode == rb_intern("active")) {
            return couchbase::persist_to::active;
        }
        if (mode == rb_intern("one")) {
            return couchbase::persist_to::one;
        }
        if (mode == rb_intern("two")) {
            return couchbase::persist_to::two;
        }
        if (mode == rb_intern("three")) {
            return couchbase::persist_to::three;
        }
        if (mode == rb_intern("four")) {
            return couchbase::persist_to::four;
        }
        throw ruby_exception(eInvalidArgument, rb_sprintf("unknown persist_to value: %+" PRIsVALUE, val));
    }
    return couchbase::persist_to::none;
}

static std::optional<couchbase::replicate_to>
extract_legacy_durability_replicate_to(VALUE options)
{
    static VALUE property_name{ rb_id2sym(rb_intern("replicate_to")) };
    if (VALUE val = rb_hash_aref(options, property_name); !NIL_P(val)) {
        ID mode = rb_sym2id(val);
        if (mode == rb_intern("none")) {
            return {};
        }
        if (mode == rb_intern("one")) {
            return couchbase::replicate_to::one;
        }
        if (mode == rb_intern("two")) {
            return couchbase::replicate_to::two;
        }
        if (mode == rb_intern("three")) {
            return couchbase::replicate_to::three;
        }
        throw ruby_exception(eInvalidArgument, rb_sprintf("unknown replicate_to: %+" PRIsVALUE, val));
    }
    return couchbase::replicate_to::none;
}

static std::optional<std::pair<couchbase::persist_to, couchbase::replicate_to>>
extract_legacy_durability_constraints(VALUE options)
{
    auto replicate_to = extract_legacy_durability_replicate_to(options);
    auto persist_to = extract_legacy_durability_persist_to(options);
    if (!persist_to && !replicate_to) {
        return {};
    }
    return { { persist_to.value_or(couchbase::persist_to::none), replicate_to.value_or(couchbase::replicate_to::none) } };
}

template<typename CommandOptions>
static void
set_durability(CommandOptions& opts, VALUE options)
{
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }
        if (auto level = extract_durability_level(options); level.has_value()) {
            opts.durability(level.value());
        }
        if (auto constraints = extract_legacy_durability_constraints(options); constraints.has_value()) {
            auto [persist_to, replicate_to] = constraints.value();
            opts.durability(persist_to, replicate_to);
        }
    }
}

template<typename CommandOptions>
static void
set_store_semantics(CommandOptions& opts, VALUE options)
{
    static VALUE property_name = rb_id2sym(rb_intern("store_semantics"));
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            throw ruby_exception(rb_eArgError, rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
        }

        VALUE val = rb_hash_aref(options, property_name);
        if (NIL_P(val)) {
            return;
        }
        if (TYPE(val) != T_SYMBOL) {
            throw ruby_exception(rb_eArgError, rb_sprintf("store_semantics must be a Symbol, but given %+" PRIsVALUE, val));
        }

        if (ID mode = rb_sym2id(val); mode == rb_intern("replace")) {
            opts.store_semantics(couchbase::store_semantics::replace);
        } else if (mode == rb_intern("insert")) {
            opts.store_semantics(couchbase::store_semantics::insert);
        } else if (mode == rb_intern("upsert")) {
            opts.store_semantics(couchbase::store_semantics::upsert);
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("unexpected store_semantics, given %+" PRIsVALUE, val));
        }
    }
}

static VALUE
to_cas_value(couchbase::cas cas)
{
    return ULL2NUM(cas.value());
}

template<typename Response>
static VALUE
to_mutation_result_value(Response resp)
{
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), to_cas_value(resp.cas()));
    if (resp.mutation_token()) {
        VALUE token = rb_hash_new();
        rb_hash_aset(token, rb_id2sym(rb_intern("partition_uuid")), ULL2NUM(resp.mutation_token()->partition_uuid()));
        rb_hash_aset(token, rb_id2sym(rb_intern("sequence_number")), ULL2NUM(resp.mutation_token()->sequence_number()));
        rb_hash_aset(token, rb_id2sym(rb_intern("partition_id")), UINT2NUM(resp.mutation_token()->partition_id()));
        rb_hash_aset(token, rb_id2sym(rb_intern("bucket_name")), cb_str_new(resp.mutation_token()->bucket_name()));
        rb_hash_aset(res, rb_id2sym(rb_intern("mutation_token")), token);
    }
    return res;
}

struct passthrough_transcoder {
    using document_type = codec::encoded_value;

    static auto decode(const codec::encoded_value& data) -> document_type
    {
        return data;
    }

    static auto encode(codec::encoded_value document) -> codec::encoded_value
    {
        return document;
    }
};
} // namespace ruby

template<>
struct codec::is_transcoder<ruby::passthrough_transcoder> : public std::true_type {
};
} // namespace couchbase

template<typename Field>
static void
cb_extract_duration(Field& field, VALUE options, const char* name)
{
    if (!NIL_P(options)) {
        switch (TYPE(options)) {
            case T_HASH:
                return cb_extract_duration(field, rb_hash_aref(options, rb_id2sym(rb_intern(name))), name);
            case T_FIXNUM:
            case T_BIGNUM:
                field = std::chrono::milliseconds(NUM2ULL(options));
                break;
            default:
                throw ruby_exception(rb_eArgError, rb_sprintf("%s must be an Integer, but given %+" PRIsVALUE, name, options));
        }
    }
}

static void
cb_extract_timeout(std::chrono::milliseconds& field, VALUE options)
{
    cb_extract_duration(field, options, "timeout");
}

static void
cb_extract_timeout(std::optional<std::chrono::milliseconds>& field, VALUE options)
{
    cb_extract_duration(field, options, "timeout");
}

static void
cb_extract_cas(couchbase::cas& field, VALUE cas)
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

template<typename Boolean>
static void
cb_extract_option_bool(Boolean& field, VALUE options, const char* name)
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

template<typename Integer>
static void
cb_extract_option_number(Integer& field, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
                field = static_cast<Integer>(FIX2ULONG(val));
                break;
            case T_BIGNUM:
                field = static_cast<Integer>(NUM2ULL(val));
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

static void
cb_extract_option_string(std::string& target, VALUE options, const char* name);

static void
cb_extract_option_symbol(VALUE& val, VALUE options, const char* name);

static void
cb_extract_dns_config(couchbase::core::io::dns::dns_config& config, VALUE options)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        return;
    }

    auto timeout{ couchbase::core::timeout_defaults::dns_srv_timeout };
    cb_extract_option_milliseconds(timeout, options, "dns_srv_timeout");

    std::string nameserver{ couchbase::core::io::dns::dns_config::default_nameserver };
    cb_extract_option_string(nameserver, options, "dns_srv_nameserver");

    std::uint16_t port{ couchbase::core::io::dns::dns_config::default_port };
    cb_extract_option_number(port, options, "dns_srv_port");

    config = couchbase::core::io::dns::dns_config(nameserver, port, timeout);
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
        auto connstr = couchbase::core::utils::parse_connection_string(input);
        if (connstr.error) {
            throw ruby_exception(eInvalidArgument,
                                 fmt::format(R"(Failed to parse connection string "{}": {})", input, connstr.error.value()));
        }
        couchbase::core::cluster_credentials auth{};
        if (NIL_P(certificate_path) || NIL_P(key_path)) {
            auth.username = cb_string_new(username);
            auth.password = cb_string_new(password);
            if (!NIL_P(options)) {
                VALUE allowed_mechanisms = rb_hash_aref(options, rb_id2sym(rb_intern("allowed_sasl_mechanisms")));
                if (!NIL_P(allowed_mechanisms)) {
                    cb_check_type(allowed_mechanisms, T_ARRAY);
                    auto allowed_mechanisms_size = static_cast<std::size_t>(RARRAY_LEN(allowed_mechanisms));
                    if (allowed_mechanisms_size < 1) {
                        throw ruby_exception(eInvalidArgument, "allowed_sasl_mechanisms list cannot be empty");
                    }
                    std::vector<std::string> mechanisms{};
                    mechanisms.reserve(allowed_mechanisms_size);
                    for (std::size_t i = 0; i < allowed_mechanisms_size; ++i) {
                        VALUE mechanism = rb_ary_entry(allowed_mechanisms, static_cast<long>(i));
                        if (mechanism == rb_id2sym(rb_intern("scram_sha512"))) {
                            mechanisms.emplace_back("SCRAM-SHA512");
                        } else if (mechanism == rb_id2sym(rb_intern("scram_sha256"))) {
                            mechanisms.emplace_back("SCRAM-SHA256");
                        } else if (mechanism == rb_id2sym(rb_intern("scram_sha1"))) {
                            mechanisms.emplace_back("SCRAM-SHA1");
                        } else if (mechanism == rb_id2sym(rb_intern("plain"))) {
                            mechanisms.emplace_back("PLAIN");
                        }
                    }
                    auth.allowed_sasl_mechanisms.emplace(mechanisms);
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
        couchbase::core::origin origin(auth, connstr);

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
        cb_extract_option_milliseconds(origin.options().bootstrap_timeout, options, "bootstrap_timeout");
        cb_extract_option_milliseconds(origin.options().resolve_timeout, options, "resolve_timeout");
        cb_extract_option_milliseconds(origin.options().connect_timeout, options, "connect_timeout");
        cb_extract_option_milliseconds(origin.options().key_value_timeout, options, "key_value_timeout");
        cb_extract_option_milliseconds(origin.options().key_value_durable_timeout, options, "key_value_durable_timeout");
        cb_extract_option_milliseconds(origin.options().view_timeout, options, "view_timeout");
        cb_extract_option_milliseconds(origin.options().query_timeout, options, "query_timeout");
        cb_extract_option_milliseconds(origin.options().analytics_timeout, options, "analytics_timeout");
        cb_extract_option_milliseconds(origin.options().search_timeout, options, "search_timeout");
        cb_extract_option_milliseconds(origin.options().management_timeout, options, "management_timeout");
        cb_extract_option_milliseconds(origin.options().tcp_keep_alive_interval, options, "tcp_keep_alive_interval");
        cb_extract_option_milliseconds(origin.options().config_poll_interval, options, "config_poll_interval");
        cb_extract_option_milliseconds(origin.options().config_poll_floor, options, "config_poll_floor");
        cb_extract_option_milliseconds(origin.options().config_idle_redial_timeout, options, "config_idle_redial_timeout");
        cb_extract_option_milliseconds(origin.options().idle_http_connection_timeout, options, "idle_http_connection_timeout");

        cb_extract_dns_config(origin.options().dns_config, options);

        cb_extract_option_number(origin.options().max_http_connections, options, "max_http_connections");

        cb_extract_option_bool(origin.options().enable_tls, options, "enable_tls");
        cb_extract_option_bool(origin.options().enable_mutation_tokens, options, "enable_mutation_tokens");
        cb_extract_option_bool(origin.options().enable_tcp_keep_alive, options, "enable_tcp_keep_alive");
        cb_extract_option_bool(origin.options().enable_dns_srv, options, "enable_dns_srv");
        cb_extract_option_bool(origin.options().show_queries, options, "show_queries");
        cb_extract_option_bool(origin.options().enable_unordered_execution, options, "enable_unordered_execution");
        cb_extract_option_bool(origin.options().enable_clustermap_notification, options, "enable_clustermap_notification");
        cb_extract_option_bool(origin.options().enable_compression, options, "enable_compression");

        cb_extract_option_string(origin.options().trust_certificate, options, "trust_certificate");
        cb_extract_option_string(origin.options().network, options, "network");

        VALUE proto = Qnil;
        cb_extract_option_symbol(proto, options, "use_ip_protocol");
        if (proto == rb_id2sym(rb_intern("any"))) {
            origin.options().use_ip_protocol = couchbase::core::io::ip_protocol::any;
        } else if (proto == rb_id2sym(rb_intern("force_ipv4"))) {
            origin.options().use_ip_protocol = couchbase::core::io::ip_protocol::force_ipv4;
        } else if (proto == rb_id2sym(rb_intern("force_ipv6"))) {
            origin.options().use_ip_protocol = couchbase::core::io::ip_protocol::force_ipv6;
        } else if (!NIL_P(proto)) {
            throw ruby_exception(eInvalidArgument, "Failed to detect preferred IP protocol");
        }

        VALUE mode = Qnil;
        cb_extract_option_symbol(mode, options, "tls_verify");
        if (mode == rb_id2sym(rb_intern("none"))) {
            origin.options().tls_verify = couchbase::core::tls_verify_mode::none;
        } else if (mode == rb_id2sym(rb_intern("peer"))) {
            origin.options().tls_verify = couchbase::core::tls_verify_mode::peer;
        } else if (!NIL_P(mode)) {
            throw ruby_exception(eInvalidArgument, "Failed to select verification mode for TLS");
        }

        origin.options().user_agent_extra =
          fmt::format("ruby_sdk/{};ssl/{:x}", std::string(EXT_GIT_REVISION).substr(0, 8), OpenSSL_version_num());
#if defined(HAVE_RUBY_VERSION_H)
        origin.options().user_agent_extra.append(
          fmt::format(";ruby_abi/{}.{}.{}", RUBY_API_VERSION_MAJOR, RUBY_API_VERSION_MINOR, RUBY_API_VERSION_TEENY));
#endif

        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster->open(origin, [barrier](std::error_code ec) { barrier->set_value(ec); });
        if (auto ec = cb_wait_for_future(f)) {
            throw ruby_exception(cb_map_error_code(ec, fmt::format("unable open cluster at {}", origin.next_address().first)));
        }
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
    if (cb_global_sink) {
        cb_global_sink->flush_deferred_messages();
    }
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
        auto barrier = std::make_shared<std::promise<couchbase::core::diag::diagnostics_result>>();
        auto f = barrier->get_future();
        cluster->diagnostics(id, [barrier](couchbase::core::diag::diagnostics_result&& resp) { barrier->set_value(std::move(resp)); });
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
                case couchbase::core::service_type::key_value:
                    type = rb_id2sym(rb_intern("kv"));
                    break;
                case couchbase::core::service_type::query:
                    type = rb_id2sym(rb_intern("query"));
                    break;
                case couchbase::core::service_type::analytics:
                    type = rb_id2sym(rb_intern("analytics"));
                    break;
                case couchbase::core::service_type::search:
                    type = rb_id2sym(rb_intern("search"));
                    break;
                case couchbase::core::service_type::view:
                    type = rb_id2sym(rb_intern("views"));
                    break;
                case couchbase::core::service_type::management:
                    type = rb_id2sym(rb_intern("mgmt"));
                    break;
                case couchbase::core::service_type::eventing:
                    type = rb_id2sym(rb_intern("eventing"));
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
                    case couchbase::core::diag::endpoint_state::disconnected:
                        state = rb_id2sym(rb_intern("disconnected"));
                        break;
                    case couchbase::core::diag::endpoint_state::connecting:
                        state = rb_id2sym(rb_intern("connecting"));
                        break;
                    case couchbase::core::diag::endpoint_state::connected:
                        state = rb_id2sym(rb_intern("connected"));
                        break;
                    case couchbase::core::diag::endpoint_state::disconnecting:
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        std::string name(RSTRING_PTR(bucket), static_cast<std::size_t>(RSTRING_LEN(bucket)));

        if (wait) {
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            cluster->open_bucket(name, [barrier](std::error_code ec) { barrier->set_value(ec); });
            if (auto ec = cb_wait_for_future(f)) {
                throw ruby_exception(cb_map_error_code(ec, fmt::format("unable open bucket \"{}\"", name)));
            }
        } else {
            cluster->open_bucket(name, [name](std::error_code ec) { CB_LOG_WARNING("unable open bucket \"{}\": {}", name, ec.message()); });
        }
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_array_of_ids(std::vector<couchbase::core::document_id>& ids, VALUE arg)
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
cb_extract_array_of_id_content(std::vector<std::pair<std::string, couchbase::codec::encoded_value>>& id_content, VALUE arg)
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
        if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 3) {
            throw ruby_exception(
              rb_eArgError,
              rb_sprintf("ID/content tuple must be represented as an Array[id, content, flags], but given %+" PRIsVALUE, entry));
        }
        VALUE id = rb_ary_entry(entry, 0);
        if (TYPE(id) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
        }
        VALUE content = rb_ary_entry(entry, 1);
        if (TYPE(content) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Content must be a String, but given %+" PRIsVALUE, content));
        }
        VALUE flags = rb_ary_entry(entry, 2);
        if (TYPE(flags) != T_FIXNUM) {
            throw ruby_exception(rb_eArgError, rb_sprintf("Flags must be an Integer, but given %+" PRIsVALUE, flags));
        }
        id_content.emplace_back(cb_string_new(id), couchbase::codec::encoded_value{ cb_binary_new(content), FIX2UINT(flags) });
    }
}

static void
cb_extract_array_of_id_cas(std::vector<std::pair<std::string, couchbase::cas>>& id_cas, VALUE arg)
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
        if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 2) {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("ID/content tuple must be represented as an Array[id, CAS], but given %+" PRIsVALUE, entry));
        }
        VALUE id = rb_ary_entry(entry, 0);
        if (TYPE(id) != T_STRING) {
            throw ruby_exception(rb_eArgError, rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
        }
        couchbase::cas cas_val{};
        if (VALUE cas = rb_ary_entry(entry, 1); !NIL_P(cas)) {
            cb_extract_cas(cas_val, cas);
        }

        id_cas.emplace_back(cb_string_new(id), cas_val);
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
        std::set<couchbase::core::service_type> selected_services{};
        if (!NIL_P(services)) {
            auto entries_num = static_cast<std::size_t>(RARRAY_LEN(services));
            for (std::size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(services, static_cast<long>(i));
                if (entry == rb_id2sym(rb_intern("kv"))) {
                    selected_services.insert(couchbase::core::service_type::key_value);
                } else if (entry == rb_id2sym(rb_intern("query"))) {
                    selected_services.insert(couchbase::core::service_type::query);
                } else if (entry == rb_id2sym(rb_intern("analytics"))) {
                    selected_services.insert(couchbase::core::service_type::analytics);
                } else if (entry == rb_id2sym(rb_intern("search"))) {
                    selected_services.insert(couchbase::core::service_type::search);
                } else if (entry == rb_id2sym(rb_intern("views"))) {
                    selected_services.insert(couchbase::core::service_type::view);
                }
            }
        }
        std::optional<std::chrono::milliseconds> timeout{};
        cb_extract_timeout(timeout, options);

        auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
        auto f = barrier->get_future();
        cluster->ping(report_id, bucket_name, selected_services, timeout, [barrier](couchbase::core::diag::ping_result&& resp) {
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
                case couchbase::core::service_type::key_value:
                    type = rb_id2sym(rb_intern("kv"));
                    break;
                case couchbase::core::service_type::query:
                    type = rb_id2sym(rb_intern("query"));
                    break;
                case couchbase::core::service_type::analytics:
                    type = rb_id2sym(rb_intern("analytics"));
                    break;
                case couchbase::core::service_type::search:
                    type = rb_id2sym(rb_intern("search"));
                    break;
                case couchbase::core::service_type::view:
                    type = rb_id2sym(rb_intern("views"));
                    break;
                case couchbase::core::service_type::management:
                    type = rb_id2sym(rb_intern("mgmt"));
                    break;
                case couchbase::core::service_type::eventing:
                    type = rb_id2sym(rb_intern("eventing"));
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
                    case couchbase::core::diag::ping_state::ok:
                        state = rb_id2sym(rb_intern("ok"));
                        break;
                    case couchbase::core::diag::ping_state::timeout:
                        state = rb_id2sym(rb_intern("timeout"));
                        break;
                    case couchbase::core::diag::ping_state::error:
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::get_request req{ doc_id };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable to fetch document");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get_any_replica(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    try {
        couchbase::get_any_replica_options opts;
        couchbase::ruby::set_timeout(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .get_any_replica(cb_string_new(id), opts);
        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to get replica of the document");
        }

        auto value = resp.content_as<couchbase::ruby::passthrough_transcoder>();
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(value.data));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas()));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(value.flags));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_get_all_replicas(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);

    try {
        couchbase::get_all_replicas_options opts;
        couchbase::ruby::set_timeout(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .get_all_replicas(cb_string_new(id), opts);
        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to get all replicas for the document");
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(resp.size()));
        for (const auto& entry : resp) {
            VALUE response = rb_hash_new();
            auto value = entry.content_as<couchbase::ruby::passthrough_transcoder>();
            rb_hash_aset(response, rb_id2sym(rb_intern("content")), cb_str_new(value.data));
            rb_hash_aset(response, rb_id2sym(rb_intern("cas")), cb_cas_to_num(entry.cas()));
            rb_hash_aset(response, rb_id2sym(rb_intern("flags")), UINT2NUM(value.flags));
            rb_ary_push(res, response);
        }
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

        std::vector<couchbase::core::document_id> ids{};
        cb_extract_array_of_ids(ids, keys);

        auto num_of_ids = ids.size();
        std::vector<std::shared_ptr<std::promise<couchbase::core::operations::get_response>>> barriers;
        barriers.reserve(num_of_ids);

        for (auto& id : ids) {
            couchbase::core::operations::get_request req{ std::move(id) };
            if (timeout.count() > 0) {
                req.timeout = timeout;
            }
            auto barrier = std::make_shared<std::promise<couchbase::core::operations::get_response>>();
            cluster->execute(req, [barrier](couchbase::core::operations::get_response&& resp) { barrier->set_value(std::move(resp)); });
            barriers.emplace_back(barrier);
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(num_of_ids));
        for (const auto& barrier : barriers) {
            auto resp = barrier->get_future().get();
            VALUE entry = rb_hash_new();
            if (resp.ctx.ec()) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("error")), cb_map_error_code(resp.ctx, "unable to (multi)fetch document"));
            }
            rb_hash_aset(entry, rb_id2sym(rb_intern("id")), cb_str_new(resp.ctx.id()));
            rb_hash_aset(entry, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
            rb_hash_aset(entry, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
            rb_hash_aset(entry, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
            rb_ary_push(res, entry);
        }

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::get_projected_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_option_bool(req.with_expiry, options, "with_expiry");
        cb_extract_option_bool(req.preserve_array_indexes, options, "preserve_array_indexes");
        VALUE projections = Qnil;
        cb_extract_option_array(projections, options, "projections");
        if (!NIL_P(projections)) {
            auto entries_num = static_cast<std::size_t>(RARRAY_LEN(projections));
            if (entries_num == 0) {
                throw ruby_exception(rb_eArgError, "projections array must not be empty");
            }
            req.projections.reserve(entries_num);
            for (std::size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(projections, static_cast<long>(i));
                cb_check_type(entry, T_STRING);
                req.projections.emplace_back(cb_string_new(entry));
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::get_projected_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::core::operations::get_projected_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::get_and_lock_request req{ doc_id };
        cb_extract_timeout(req, options);
        req.lock_time = NUM2UINT(lock_time);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::get_and_lock_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::core::operations::get_and_lock_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable lock and fetch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::get_and_touch_request req{ doc_id };
        cb_extract_timeout(req, options);
        auto [type, duration] = couchbase::ruby::unpack_expiry(expiry, false);
        req.expiry = static_cast<std::uint32_t>(duration.count());

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::get_and_touch_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::core::operations::get_and_touch_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable fetch and touch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_touch(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE expiry, VALUE options)
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::touch_request req{ doc_id };
        cb_extract_timeout(req, options);
        auto [type, duration] = couchbase::ruby::unpack_expiry(expiry, false);
        req.expiry = static_cast<std::uint32_t>(duration.count());

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::touch_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::touch_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable to touch");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::exists_request req{ doc_id };
        cb_extract_timeout(req, options);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::exists_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::exists_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec() && resp.ctx.ec() != couchbase::errc::key_value::document_not_found) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::unlock_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_cas(req.cas, cas);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::unlock_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::unlock_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable to unlock");
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_upsert(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

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
        couchbase::upsert_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_preserve_expiry(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .upsert(cb_string_new(id), couchbase::codec::encoded_value{ cb_binary_new(content), FIX2UINT(flags) }, opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to upsert");
        }

        return couchbase::ruby::to_mutation_result_value(resp);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_upsert_multi(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id_content, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    try {
        couchbase::upsert_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_preserve_expiry(opts, options);

        auto c = couchbase::cluster(*core).bucket(cb_string_new(bucket)).scope(cb_string_new(scope)).collection(cb_string_new(collection));

        std::vector<std::pair<std::string, couchbase::codec::encoded_value>> tuples{};
        cb_extract_array_of_id_content(tuples, id_content);

        auto num_of_tuples = tuples.size();
        std::vector<std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>> futures;
        futures.reserve(num_of_tuples);

        for (auto& [id, content] : tuples) {
            futures.emplace_back(c.upsert(std::move(id), content, opts));
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(num_of_tuples));
        for (auto& f : futures) {
            auto [ctx, resp] = f.get();
            VALUE entry = couchbase::ruby::to_mutation_result_value(resp);
            if (ctx.ec()) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("error")), cb_map_error_code(ctx, "unable (multi)upsert"));
            }
            rb_hash_aset(entry, rb_id2sym(rb_intern("id")), cb_str_new(ctx.id()));
            rb_ary_push(res, entry);
        }
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_append(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::append_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .binary()
                   .append(cb_string_new(id), cb_binary_new(content), opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to append");
        }

        return couchbase::ruby::to_mutation_result_value(resp);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_prepend(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    Check_Type(content, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::prepend_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .binary()
                   .prepend(cb_string_new(id), cb_binary_new(content), opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to prepend");
        }

        return couchbase::ruby::to_mutation_result_value(resp);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_replace(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

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
        couchbase::replace_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_preserve_expiry(opts, options);
        couchbase::ruby::set_cas(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .replace(cb_string_new(id), couchbase::codec::encoded_value{ cb_binary_new(content), FIX2UINT(flags) }, opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to replace");
        }

        return couchbase::ruby::to_mutation_result_value(resp);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_insert(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE content, VALUE flags, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

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
        couchbase::insert_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_durability(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .insert(cb_string_new(id), couchbase::codec::encoded_value{ cb_binary_new(content), FIX2UINT(flags) }, opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to insert");
        }

        return couchbase::ruby::to_mutation_result_value(resp);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_remove(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::remove_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_cas(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .remove(cb_string_new(id), opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to remove");
        }

        return couchbase::ruby::to_mutation_result_value(resp);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_remove_multi(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id_cas, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::remove_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);

        std::vector<std::pair<std::string, couchbase::cas>> tuples{};
        cb_extract_array_of_id_cas(tuples, id_cas);

        auto c = couchbase::cluster(*core).bucket(cb_string_new(bucket)).scope(cb_string_new(scope)).collection(cb_string_new(collection));

        auto num_of_tuples = tuples.size();
        std::vector<std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>> futures;
        futures.reserve(num_of_tuples);

        for (auto& [id, cas] : tuples) {
            opts.cas(cas);
            futures.emplace_back(c.remove(std::move(id), opts));
        }

        VALUE res = rb_ary_new_capa(static_cast<long>(num_of_tuples));
        for (auto& f : futures) {
            auto [ctx, resp] = f.get();
            VALUE entry = couchbase::ruby::to_mutation_result_value(resp);
            if (ctx.ec()) {
                rb_hash_aset(entry, rb_id2sym(rb_intern("error")), cb_map_error_code(ctx, "unable (multi)remove"));
            }
            rb_hash_aset(entry, rb_id2sym(rb_intern("id")), cb_str_new(ctx.id()));
            rb_ary_push(res, entry);
        }

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_increment(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::increment_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_delta(opts, options);
        couchbase::ruby::set_initial_value(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .binary()
                   .increment(cb_string_new(id), opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to increment");
        }

        VALUE res = couchbase::ruby::to_mutation_result_value(resp);
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content()));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_decrement(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(id, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::decrement_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_delta(opts, options);
        couchbase::ruby::set_initial_value(opts, options);

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .binary()
                   .decrement(cb_string_new(id), opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to decrement");
        }

        VALUE res = couchbase::ruby::to_mutation_result_value(resp);
        rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content()));
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::lookup_in_request req{ doc_id };
        cb_extract_timeout(req, options);
        cb_extract_option_bool(req.access_deleted, options, "access_deleted");

        static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
        static VALUE path_property = rb_id2sym(rb_intern("path"));
        static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));

        auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
        for (std::size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            cb_check_type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, opcode_property);
            cb_check_type(operation, T_SYMBOL);
            bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
            VALUE path = rb_hash_aref(entry, path_property);
            cb_check_type(path, T_STRING);
            auto opcode = couchbase::core::impl::subdoc::opcode{};
            if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
                opcode = couchbase::core::impl::subdoc::opcode::get_doc;
            } else if (operation_id == rb_intern("get")) {
                opcode = couchbase::core::impl::subdoc::opcode::get;
            } else if (operation_id == rb_intern("exists")) {
                opcode = couchbase::core::impl::subdoc::opcode::exists;
            } else if (operation_id == rb_intern("count")) {
                opcode = couchbase::core::impl::subdoc::opcode::get_count;
            } else {
                throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
            }
            cb_check_type(path, T_STRING);

            req.specs.emplace_back(couchbase::core::impl::subdoc::command{
              opcode, cb_string_new(path), {}, couchbase::core::impl::subdoc::build_lookup_in_path_flags(xattr) });
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::lookup_in_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::lookup_in_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable to perform lookup_in operation");
        }

        static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
        static VALUE fields_property = rb_id2sym(rb_intern("fields"));
        static VALUE index_property = rb_id2sym(rb_intern("index"));
        static VALUE exists_property = rb_id2sym(rb_intern("exists"));
        static VALUE cas_property = rb_id2sym(rb_intern("cas"));
        static VALUE value_property = rb_id2sym(rb_intern("value"));
        static VALUE error_property = rb_id2sym(rb_intern("error"));

        VALUE res = rb_hash_new();
        rb_hash_aset(res, cas_property, cb_cas_to_num(resp.cas));
        VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
        rb_hash_aset(res, fields_property, fields);
        rb_hash_aset(res, deleted_property, resp.deleted ? Qtrue : Qfalse);
        for (std::size_t i = 0; i < entries_size; ++i) {
            auto resp_entry = resp.fields.at(i);
            VALUE entry = rb_hash_new();
            rb_hash_aset(entry, index_property, ULL2NUM(resp_entry.original_index));
            rb_hash_aset(entry, exists_property, resp_entry.exists ? Qtrue : Qfalse);
            rb_hash_aset(entry, path_property, cb_str_new(resp_entry.path));
            if (!resp_entry.value.empty()) {
                rb_hash_aset(entry, value_property, cb_str_new(resp_entry.value));
            }
            if (resp_entry.ec) {
                rb_hash_aset(entry,
                             error_property,
                             cb_map_error_code(resp_entry.ec,
                                               fmt::format("error getting result for spec at index {}, path \"{}\"", i, resp_entry.path)));
            }
            rb_ary_store(fields, static_cast<long>(i), entry);
        }
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_lookup_in_any_replica(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE specs, VALUE options)
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::lookup_in_any_replica_request req{ doc_id };
        cb_extract_timeout(req, options);

        static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
        static VALUE path_property = rb_id2sym(rb_intern("path"));
        static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));

        auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
        for (std::size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            cb_check_type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, opcode_property);
            cb_check_type(operation, T_SYMBOL);
            bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
            VALUE path = rb_hash_aref(entry, path_property);
            cb_check_type(path, T_STRING);
            auto opcode = couchbase::core::impl::subdoc::opcode{};
            if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
                opcode = couchbase::core::impl::subdoc::opcode::get_doc;
            } else if (operation_id == rb_intern("get")) {
                opcode = couchbase::core::impl::subdoc::opcode::get;
            } else if (operation_id == rb_intern("exists")) {
                opcode = couchbase::core::impl::subdoc::opcode::exists;
            } else if (operation_id == rb_intern("count")) {
                opcode = couchbase::core::impl::subdoc::opcode::get_count;
            } else {
                throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
            }
            cb_check_type(path, T_STRING);

            req.specs.emplace_back(couchbase::core::impl::subdoc::command{
              opcode, cb_string_new(path), {}, couchbase::core::impl::subdoc::build_lookup_in_path_flags(xattr) });
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::lookup_in_any_replica_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::lookup_in_any_replica_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable to perform lookup_in_any_replica operation");
        }

        static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
        static VALUE fields_property = rb_id2sym(rb_intern("fields"));
        static VALUE index_property = rb_id2sym(rb_intern("index"));
        static VALUE exists_property = rb_id2sym(rb_intern("exists"));
        static VALUE cas_property = rb_id2sym(rb_intern("cas"));
        static VALUE value_property = rb_id2sym(rb_intern("value"));
        static VALUE error_property = rb_id2sym(rb_intern("error"));
        static VALUE is_replica_property = rb_id2sym(rb_intern("is_replica"));

        VALUE res = rb_hash_new();
        rb_hash_aset(res, cas_property, cb_cas_to_num(resp.cas));
        VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
        rb_hash_aset(res, fields_property, fields);
        rb_hash_aset(res, deleted_property, resp.deleted ? Qtrue : Qfalse);
        rb_hash_aset(res, is_replica_property, resp.is_replica ? Qtrue : Qfalse);

        for (std::size_t i = 0; i < entries_size; ++i) {
            auto resp_entry = resp.fields.at(i);
            VALUE entry = rb_hash_new();
            rb_hash_aset(entry, index_property, ULL2NUM(resp_entry.original_index));
            rb_hash_aset(entry, exists_property, resp_entry.exists ? Qtrue : Qfalse);
            rb_hash_aset(entry, path_property, cb_str_new(resp_entry.path));
            if (!resp_entry.value.empty()) {
                rb_hash_aset(entry, value_property, cb_str_new(resp_entry.value));
            }
            if (resp_entry.ec) {
                rb_hash_aset(entry,
                             error_property,
                             cb_map_error_code(resp_entry.ec,
                                               fmt::format("error getting result for spec at index {}, path \"{}\"", i, resp_entry.path)));
            }
            rb_ary_store(fields, static_cast<long>(i), entry);
        }

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_lookup_in_all_replicas(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE specs, VALUE options)
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
        couchbase::core::document_id doc_id{
            cb_string_new(bucket),
            cb_string_new(scope),
            cb_string_new(collection),
            cb_string_new(id),
        };

        couchbase::core::operations::lookup_in_all_replicas_request req{ doc_id };
        cb_extract_timeout(req, options);

        static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
        static VALUE path_property = rb_id2sym(rb_intern("path"));
        static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));

        auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
        for (std::size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            cb_check_type(entry, T_HASH);
            VALUE operation = rb_hash_aref(entry, opcode_property);
            cb_check_type(operation, T_SYMBOL);
            bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
            VALUE path = rb_hash_aref(entry, path_property);
            cb_check_type(path, T_STRING);
            auto opcode = couchbase::core::impl::subdoc::opcode{};
            if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
                opcode = couchbase::core::impl::subdoc::opcode::get_doc;
            } else if (operation_id == rb_intern("get")) {
                opcode = couchbase::core::impl::subdoc::opcode::get;
            } else if (operation_id == rb_intern("exists")) {
                opcode = couchbase::core::impl::subdoc::opcode::exists;
            } else if (operation_id == rb_intern("count")) {
                opcode = couchbase::core::impl::subdoc::opcode::get_count;
            } else {
                throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
            }
            cb_check_type(path, T_STRING);

            req.specs.emplace_back(couchbase::core::impl::subdoc::command{
              opcode, cb_string_new(path), {}, couchbase::core::impl::subdoc::build_lookup_in_path_flags(xattr) });
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::lookup_in_all_replicas_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::lookup_in_all_replicas_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error_code(resp.ctx, "unable to perform lookup_in_all_replicas operation");
        }

        static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
        static VALUE fields_property = rb_id2sym(rb_intern("fields"));
        static VALUE index_property = rb_id2sym(rb_intern("index"));
        static VALUE exists_property = rb_id2sym(rb_intern("exists"));
        static VALUE cas_property = rb_id2sym(rb_intern("cas"));
        static VALUE value_property = rb_id2sym(rb_intern("value"));
        static VALUE error_property = rb_id2sym(rb_intern("error"));
        static VALUE is_replica_property = rb_id2sym(rb_intern("is_replica"));

        auto lookup_in_entries_size = resp.entries.size();
        VALUE res = rb_ary_new_capa(static_cast<long>(lookup_in_entries_size));
        for (std::size_t j = 0; j < lookup_in_entries_size; ++j) {
            auto lookup_in_entry = resp.entries.at(j);
            VALUE lookup_in_entry_res = rb_hash_new();
            rb_hash_aset(lookup_in_entry_res, cas_property, cb_cas_to_num(lookup_in_entry.cas));
            VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
            rb_hash_aset(lookup_in_entry_res, fields_property, fields);
            rb_hash_aset(lookup_in_entry_res, deleted_property, lookup_in_entry.deleted ? Qtrue : Qfalse);
            rb_hash_aset(lookup_in_entry_res, is_replica_property, lookup_in_entry.is_replica ? Qtrue : Qfalse);

            for (std::size_t i = 0; i < entries_size; ++i) {
                auto field_entry = lookup_in_entry.fields.at(i);
                VALUE entry = rb_hash_new();
                rb_hash_aset(entry, index_property, ULL2NUM(field_entry.original_index));
                rb_hash_aset(entry, exists_property, field_entry.exists ? Qtrue : Qfalse);
                rb_hash_aset(entry, path_property, cb_str_new(field_entry.path));
                if (!field_entry.value.empty()) {
                    rb_hash_aset(entry, value_property, cb_str_new(field_entry.value));
                }
                if (field_entry.ec) {
                    rb_hash_aset(
                      entry,
                      error_property,
                      cb_map_error_code(field_entry.ec,
                                        fmt::format("error getting result for spec at index {}, path \"{}\"", i, field_entry.path)));
                }
                rb_ary_store(fields, static_cast<long>(i), entry);
            }
            rb_ary_store(res, static_cast<long>(j), lookup_in_entry_res);
        }

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_mutate_in(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE id, VALUE specs, VALUE options)
{
    const auto& core = cb_backend_to_cluster(self);

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
        couchbase::mutate_in_options opts;
        couchbase::ruby::set_timeout(opts, options);
        couchbase::ruby::set_durability(opts, options);
        couchbase::ruby::set_expiry(opts, options);
        couchbase::ruby::set_preserve_expiry(opts, options);
        couchbase::ruby::set_access_deleted(opts, options);
        couchbase::ruby::set_create_as_deleted(opts, options);
        couchbase::ruby::set_cas(opts, options);
        couchbase::ruby::set_store_semantics(opts, options);

        static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
        static VALUE create_path_property = rb_id2sym(rb_intern("create_path"));
        static VALUE expand_macros_property = rb_id2sym(rb_intern("expand_macros"));
        static VALUE path_property = rb_id2sym(rb_intern("path"));
        static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));
        static VALUE param_property = rb_id2sym(rb_intern("param"));

        couchbase::mutate_in_specs cxx_specs;
        auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
        for (std::size_t i = 0; i < entries_size; ++i) {
            VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
            cb_check_type(entry, T_HASH);
            bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
            bool create_path = RTEST(rb_hash_aref(entry, create_path_property));
            bool expand_macros = RTEST(rb_hash_aref(entry, expand_macros_property));
            VALUE path = rb_hash_aref(entry, path_property);
            cb_check_type(path, T_STRING);
            VALUE operation = rb_hash_aref(entry, opcode_property);
            cb_check_type(operation, T_SYMBOL);
            VALUE param = rb_hash_aref(entry, param_property);
            if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("dict_add")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(couchbase::mutate_in_specs::insert_raw(cb_string_new(path), cb_binary_new(param), expand_macros)
                                      .xattr(xattr)
                                      .create_path(create_path));
            } else if (operation_id == rb_intern("dict_upsert")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(couchbase::mutate_in_specs::upsert_raw(cb_string_new(path), cb_binary_new(param), expand_macros)
                                      .xattr(xattr)
                                      .create_path(create_path));
            } else if (operation_id == rb_intern("remove")) {
                cxx_specs.push_back(couchbase::mutate_in_specs::remove(cb_string_new(path)).xattr(xattr));
            } else if (operation_id == rb_intern("replace")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(
                  couchbase::mutate_in_specs::replace_raw(cb_string_new(path), cb_binary_new(param), expand_macros).xattr(xattr));
            } else if (operation_id == rb_intern("array_push_last")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(couchbase::mutate_in_specs::array_append_raw(cb_string_new(path), cb_binary_new(param))
                                      .xattr(xattr)
                                      .create_path(create_path));
            } else if (operation_id == rb_intern("array_push_first")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(couchbase::mutate_in_specs::array_prepend_raw(cb_string_new(path), cb_binary_new(param))
                                      .xattr(xattr)
                                      .create_path(create_path));
            } else if (operation_id == rb_intern("array_insert")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(couchbase::mutate_in_specs::array_insert_raw(cb_string_new(path), cb_binary_new(param))
                                      .xattr(xattr)
                                      .create_path(create_path));
            } else if (operation_id == rb_intern("array_add_unique")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(
                  couchbase::mutate_in_specs::array_add_unique_raw(cb_string_new(path), cb_binary_new(param), expand_macros)
                    .xattr(xattr)
                    .create_path(create_path));
            } else if (operation_id == rb_intern("counter")) {
                if (TYPE(param) == T_FIXNUM || TYPE(param) == T_BIGNUM) {
                    if (std::int64_t num = NUM2LL(param); num < 0) {
                        cxx_specs.push_back(
                          couchbase::mutate_in_specs::decrement(cb_string_new(path), -1 * num).xattr(xattr).create_path(create_path));
                    } else {
                        cxx_specs.push_back(
                          couchbase::mutate_in_specs::increment(cb_string_new(path), num).xattr(xattr).create_path(create_path));
                    }
                } else {
                    throw ruby_exception(eInvalidArgument,
                                         rb_sprintf("subdocument counter operation expects number, but given: %+" PRIsVALUE, param));
                }
            } else if (operation_id == rb_intern("set_doc")) {
                cb_check_type(param, T_STRING);
                cxx_specs.push_back(couchbase::mutate_in_specs::replace_raw("", cb_binary_new(param), expand_macros).xattr(xattr));
            } else if (operation_id == rb_intern("remove_doc")) {
                cxx_specs.push_back(couchbase::mutate_in_specs::remove("").xattr(xattr));
            } else {
                throw ruby_exception(eInvalidArgument,
                                     rb_sprintf("unsupported operation for subdocument mutation: %+" PRIsVALUE, operation));
            }
        }

        auto f = couchbase::cluster(*core)
                   .bucket(cb_string_new(bucket))
                   .scope(cb_string_new(scope))
                   .collection(cb_string_new(collection))
                   .mutate_in(cb_string_new(id), cxx_specs, opts);

        auto [ctx, resp] = cb_wait_for_future(f);
        if (ctx.ec()) {
            cb_throw_error_code(ctx, "unable to mutate_in");
        }

        static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
        static VALUE fields_property = rb_id2sym(rb_intern("fields"));
        static VALUE index_property = rb_id2sym(rb_intern("index"));
        static VALUE cas_property = rb_id2sym(rb_intern("cas"));
        static VALUE value_property = rb_id2sym(rb_intern("value"));

        VALUE res = couchbase::ruby::to_mutation_result_value(resp);
        rb_hash_aset(res, deleted_property, resp.is_deleted() ? Qtrue : Qfalse);
        if (!ctx.ec()) {
            rb_hash_aset(res, cas_property, cb_cas_to_num(resp.cas()));

            VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
            rb_hash_aset(res, fields_property, fields);
            for (std::size_t i = 0; i < entries_size; ++i) {
                VALUE entry = rb_hash_new();
                rb_hash_aset(entry, index_property, ULL2NUM(i));
                rb_hash_aset(entry, path_property, rb_hash_aref(rb_ary_entry(specs, static_cast<long>(i)), path_property));
                if (resp.has_value(i)) {
                    auto value = resp.content_as<tao::json::value>(i);
                    rb_hash_aset(entry, value_property, cb_str_new(couchbase::core::utils::json::generate(value)));
                }
                rb_ary_store(fields, static_cast<long>(i), entry);
            }
        }
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

struct cb_core_scan_result_data {
    std::unique_ptr<couchbase::core::scan_result> scan_result{};
};

static void
cb_CoreScanResult_mark(void* ptr)
{
    /* No embedded Ruby objects */
}

static void
cb_CoreScanResult_free(void* ptr)
{
    auto* data = static_cast<cb_core_scan_result_data*>(ptr);
    if (data->scan_result != nullptr && !data->scan_result->is_cancelled()) {
        data->scan_result->cancel();
    }
    data->scan_result.reset();
    ruby_xfree(data);
}

static const rb_data_type_t cb_core_scan_result_type {
    .wrap_struct_name = "Couchbase/Backend/CoreScanResult",
    .function = {
      .dmark = cb_CoreScanResult_mark,
      .dfree = cb_CoreScanResult_free,
    },
    .data = nullptr,
#ifdef RUBY_TYPED_FREE_IMMEDIATELY
    .flags = RUBY_TYPED_FREE_IMMEDIATELY,
#endif
};

static VALUE
cb_CoreScanResult_allocate(VALUE klass)
{
    cb_core_scan_result_data* data = nullptr;
    VALUE obj = TypedData_Make_Struct(klass, cb_core_scan_result_data, &cb_core_scan_result_type, data);
    return obj;
}

static VALUE
cb_CoreScanResult_is_cancelled(VALUE self)
{
    cb_core_scan_result_data* data = nullptr;
    TypedData_Get_Struct(self, cb_core_scan_result_data, &cb_core_scan_result_type, data);
    auto resp = data->scan_result->is_cancelled();
    if (resp) {
        return Qtrue;
    } else {
        return Qfalse;
    }
}

static VALUE
cb_CoreScanResult_cancel(VALUE self)
{
    cb_core_scan_result_data* data = nullptr;
    TypedData_Get_Struct(self, cb_core_scan_result_data, &cb_core_scan_result_type, data);
    data->scan_result->cancel();
    return Qnil;
}

static VALUE
cb_CoreScanResult_next_item(VALUE self)
{
    try {
        cb_core_scan_result_data* data = nullptr;
        TypedData_Get_Struct(self, cb_core_scan_result_data, &cb_core_scan_result_type, data);
        auto barrier = std::make_shared<std::promise<tl::expected<couchbase::core::range_scan_item, std::error_code>>>();
        auto f = barrier->get_future();
        data->scan_result->next([barrier](couchbase::core::range_scan_item item, std::error_code ec) {
            if (ec) {
                return barrier->set_value(tl::unexpected(ec));
            } else {
                return barrier->set_value(item);
            }
        });
        auto resp = cb_wait_for_future(f);
        if (!resp.has_value()) {
            // If the error code is range_scan_completed return nil without raising an exception (nil signifies that there
            // are no more items)
            if (resp.error() != couchbase::errc::key_value::range_scan_completed) {
                cb_throw_error_code(resp.error(), "unable to fetch next scan item");
            }
            // Release ownership of scan_result unique pointer
            return Qnil;
        }
        auto item = resp.value();
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(item.key));
        if (item.body.has_value()) {
            auto body = item.body.value();
            rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(item.key));
            rb_hash_aset(res, rb_id2sym(rb_intern("encoded")), cb_str_new(body.value));
            rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(body.cas));
            rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(body.flags));
            rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(body.expiry));
            rb_hash_aset(res, rb_id2sym(rb_intern("id_only")), Qfalse);
        } else {
            rb_hash_aset(res, rb_id2sym(rb_intern("id_only")), Qtrue);
        }
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_scan_create(VALUE self, VALUE bucket, VALUE scope, VALUE collection, VALUE scan_type, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    Check_Type(scope, T_STRING);
    Check_Type(collection, T_STRING);
    Check_Type(scan_type, T_HASH);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::range_scan_orchestrator_options orchestrator_options{};
        cb_extract_timeout(orchestrator_options, options);
        cb_extract_option_bool(orchestrator_options.ids_only, options, "ids_only");
        cb_extract_option_number(orchestrator_options.batch_item_limit, options, "batch_item_limit");
        cb_extract_option_number(orchestrator_options.batch_byte_limit, options, "batch_byte_limit");
        cb_extract_option_number(orchestrator_options.concurrency, options, "concurrency");

        // Extracting the mutation state
        if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state"))); !NIL_P(mutation_state)) {
            cb_check_type(mutation_state, T_ARRAY);
            auto state_size = static_cast<std::size_t>(RARRAY_LEN(mutation_state));

            if (state_size > 0) {
                auto core_mut_state = couchbase::core::mutation_state{};
                core_mut_state.tokens.reserve(state_size);
                for (std::size_t i = 0; i < state_size; ++i) {
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
                    core_mut_state.tokens.emplace_back(NUM2ULL(partition_uuid),
                                                       NUM2ULL(sequence_number),
                                                       gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                                       cb_string_new(bucket_name));
                }

                orchestrator_options.consistent_with = core_mut_state;
            }
        }

        auto bucket_name = cb_string_new(bucket);
        auto scope_name = cb_string_new(scope);
        auto collection_name = cb_string_new(collection);

        // Getting the operation agent
        auto agent_group = couchbase::core::agent_group(cluster->io_context(), couchbase::core::agent_group_config{ { *cluster } });
        agent_group.open_bucket(bucket_name);
        auto agent = agent_group.get_agent(bucket_name);
        if (!agent.has_value()) {
            rb_raise(eCouchbaseError, "Cannot perform scan operation. Unable to get operation agent");
            return Qnil;
        }

        // Getting the vbucket map
        auto barrier = std::make_shared<std::promise<tl::expected<couchbase::core::topology::configuration, std::error_code>>>();
        auto f = barrier->get_future();
        cluster->with_bucket_configuration(bucket_name,
                                           [barrier](std::error_code ec, const couchbase::core::topology::configuration& config) mutable {
                                               if (ec) {
                                                   return barrier->set_value(tl::unexpected(ec));
                                               }
                                               barrier->set_value(config);
                                           });
        auto config = cb_wait_for_future(f);
        if (!config.has_value()) {
            rb_raise(eCouchbaseError, "Cannot perform scan operation. Unable to get bucket configuration");
            return Qnil;
        }
        if (!config->supports_range_scan()) {
            rb_raise(eFeatureNotAvailable, "Server does not support key-value scan operations");
            return Qnil;
        }
        auto vbucket_map = config->vbmap;
        if (!vbucket_map || vbucket_map->empty()) {
            rb_raise(eCouchbaseError, "Cannot perform scan operation. Unable to get vbucket map");
            return Qnil;
        }

        // Constructing the scan type
        std::variant<std::monostate, couchbase::core::range_scan, couchbase::core::prefix_scan, couchbase::core::sampling_scan>
          core_scan_type{};
        ID scan_type_id = rb_sym2id(rb_hash_aref(scan_type, rb_id2sym(rb_intern("scan_type"))));
        if (scan_type_id == rb_intern("range")) {
            auto range_scan = couchbase::core::range_scan{};

            VALUE from_hash = rb_hash_aref(scan_type, rb_id2sym(rb_intern("from")));
            VALUE to_hash = rb_hash_aref(scan_type, rb_id2sym(rb_intern("to")));

            if (!NIL_P(from_hash)) {
                Check_Type(from_hash, T_HASH);
                range_scan.from = couchbase::core::scan_term{};
                cb_extract_option_string(range_scan.from->term, from_hash, "term");
                cb_extract_option_bool(range_scan.from->exclusive, from_hash, "exclusive");
            }
            if (!NIL_P(to_hash)) {
                Check_Type(to_hash, T_HASH);
                range_scan.to = couchbase::core::scan_term{};
                cb_extract_option_string(range_scan.to->term, to_hash, "term");
                cb_extract_option_bool(range_scan.to->exclusive, to_hash, "exclusive");
            }
            core_scan_type = range_scan;
        } else if (scan_type_id == rb_intern("prefix")) {
            auto prefix_scan = couchbase::core::prefix_scan{};
            cb_extract_option_string(prefix_scan.prefix, scan_type, "prefix");
            core_scan_type = prefix_scan;
        } else if (scan_type_id == rb_intern("sampling")) {
            auto sampling_scan = couchbase::core::sampling_scan{};
            cb_extract_option_number(sampling_scan.limit, scan_type, "limit");
            cb_extract_option_number(sampling_scan.seed, scan_type, "seed");
            core_scan_type = sampling_scan;
        } else {
            rb_raise(eInvalidArgument, "Invalid scan operation type");
        }

        auto orchestrator = couchbase::core::range_scan_orchestrator(
          cluster->io_context(), agent.value(), vbucket_map.value(), scope_name, collection_name, core_scan_type, orchestrator_options);

        // Start the scan
        auto resp = orchestrator.scan();
        if (!resp.has_value()) {
            cb_throw_error_code(resp.error(), "unable to start scan");
        }

        // Wrap core scan_result inside Ruby ScanResult
        // Creating a Ruby CoreScanResult object *after* checking that no error occurred during orchestrator.scan()
        VALUE cCoreScanResult = rb_define_class_under(rb_define_module("Couchbase"), "CoreScanResult", rb_cObject);
        rb_define_alloc_func(cCoreScanResult, cb_CoreScanResult_allocate);
        rb_define_method(cCoreScanResult, "next_item", VALUE_FUNC(cb_CoreScanResult_next_item), 0);
        rb_define_method(cCoreScanResult, "cancelled?", VALUE_FUNC(cb_CoreScanResult_is_cancelled), 0);
        rb_define_method(cCoreScanResult, "cancel", VALUE_FUNC(cb_CoreScanResult_cancel), 0);
        VALUE core_scan_result_obj = rb_class_new_instance(0, NULL, cCoreScanResult);
        rb_ivar_set(core_scan_result_obj, rb_intern("@backend"), self);
        cb_core_scan_result_data* data = nullptr;
        TypedData_Get_Struct(core_scan_result_obj, cb_core_scan_result_data, &cb_core_scan_result_type, data);
        data->scan_result = std::make_unique<couchbase::core::scan_result>(resp.value());
        return core_scan_result_obj;

    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static int
cb_for_each_named_param(VALUE key, VALUE value, VALUE arg)
{
    auto* preq = reinterpret_cast<couchbase::core::operations::query_request*>(arg);
    try {
        cb_check_type(key, T_STRING);
        cb_check_type(value, T_STRING);
    } catch (const ruby_exception&) {
        return ST_STOP;
    }
    preq->named_parameters[cb_string_new(key)] = cb_string_new(value);
    return ST_CONTINUE;
}

static VALUE
cb_Backend_document_query(VALUE self, VALUE statement, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(statement, T_STRING);
    Check_Type(options, T_HASH);

    try {
        couchbase::core::operations::query_request req;
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
        cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");
        cb_extract_option_bool(req.use_replica, options, "use_replica");
        cb_extract_option_uint64(req.scan_cap, options, "scan_cap");
        cb_extract_duration(req.scan_wait, options, "scan_wait");
        cb_extract_option_uint64(req.max_parallelism, options, "max_parallelism");
        cb_extract_option_uint64(req.pipeline_cap, options, "pipeline_cap");
        cb_extract_option_uint64(req.pipeline_batch, options, "pipeline_batch");
        if (VALUE query_context = rb_hash_aref(options, rb_id2sym(rb_intern("query_context")));
            !NIL_P(query_context) && TYPE(query_context) == T_STRING) {
            req.query_context.emplace(cb_string_new(query_context));
        }
        if (VALUE profile = rb_hash_aref(options, rb_id2sym(rb_intern("profile"))); !NIL_P(profile)) {
            cb_check_type(profile, T_SYMBOL);
            ID mode = rb_sym2id(profile);
            if (mode == rb_intern("phases")) {
                req.profile = couchbase::query_profile::phases;
            } else if (mode == rb_intern("timings")) {
                req.profile = couchbase::query_profile::timings;
            } else if (mode == rb_intern("off")) {
                req.profile = couchbase::query_profile::off;
            }
        }
        if (VALUE positional_params = rb_hash_aref(options, rb_id2sym(rb_intern("positional_parameters"))); !NIL_P(positional_params)) {
            cb_check_type(positional_params, T_ARRAY);
            auto entries_num = static_cast<std::size_t>(RARRAY_LEN(positional_params));
            req.positional_parameters.reserve(entries_num);
            for (std::size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
                cb_check_type(entry, T_STRING);
                req.positional_parameters.emplace_back(cb_string_new(entry));
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
                req.scan_consistency = couchbase::query_scan_consistency::not_bounded;
            } else if (type == rb_intern("request_plus")) {
                req.scan_consistency = couchbase::query_scan_consistency::request_plus;
            }
        }
        if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state"))); !NIL_P(mutation_state)) {
            cb_check_type(mutation_state, T_ARRAY);
            auto state_size = static_cast<std::size_t>(RARRAY_LEN(mutation_state));
            req.mutation_state.reserve(state_size);
            for (std::size_t i = 0; i < state_size; ++i) {
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
                req.mutation_state.emplace_back(NUM2ULL(partition_uuid),
                                                NUM2ULL(sequence_number),
                                                gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                                cb_string_new(bucket_name));
            }
        }

        if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters"))); !NIL_P(raw_params)) {
            cb_check_type(raw_params, T_HASH);
            rb_hash_foreach(raw_params, INT_FUNC(cb_for_each_named_param), reinterpret_cast<VALUE>(&req));
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::query_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::query_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (resp.meta.errors && !resp.meta.errors->empty()) {
                const auto& first_error = resp.meta.errors->front();
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to query ({}: {}))", first_error.code, first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, "unable to query");
            }
        }
        VALUE res = rb_hash_new();
        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
        for (const auto& row : resp.rows) {
            rb_ary_push(rows, cb_str_new(row));
        }
        VALUE meta = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
        rb_hash_aset(
          meta, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern2(resp.meta.status.data(), static_cast<long>(resp.meta.status.size()))));
        rb_hash_aset(meta, rb_id2sym(rb_intern("request_id")), cb_str_new(resp.meta.request_id));
        rb_hash_aset(meta, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.meta.client_context_id));
        if (resp.meta.signature) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("signature")), cb_str_new(resp.meta.signature.value()));
        }
        if (resp.meta.profile) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("profile")), cb_str_new(resp.meta.profile.value()));
        }
        if (resp.meta.metrics) {
            VALUE metrics = rb_hash_new();
            rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
            rb_hash_aset(metrics, rb_id2sym(rb_intern("elapsed_time")), ULL2NUM(resp.meta.metrics->elapsed_time.count()));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("execution_time")), ULL2NUM(resp.meta.metrics->execution_time.count()));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.meta.metrics->result_count));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.meta.metrics->result_size));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("sort_count")), ULL2NUM(resp.meta.metrics->sort_count));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("mutation_count")), ULL2NUM(resp.meta.metrics->mutation_count));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("error_count")), ULL2NUM(resp.meta.metrics->error_count));
            rb_hash_aset(metrics, rb_id2sym(rb_intern("warning_count")), ULL2NUM(resp.meta.metrics->warning_count));
        }

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_generate_bucket_settings(VALUE bucket, couchbase::core::management::cluster::bucket_settings& entry, bool is_create)
{
    if (VALUE bucket_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("bucket_type"))); !NIL_P(bucket_type)) {
        if (TYPE(bucket_type) == T_SYMBOL) {
            if (bucket_type == rb_id2sym(rb_intern("couchbase")) || bucket_type == rb_id2sym(rb_intern("membase"))) {
                entry.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;
            } else if (bucket_type == rb_id2sym(rb_intern("memcached"))) {
                entry.bucket_type = couchbase::core::management::cluster::bucket_type::memcached;
            } else if (bucket_type == rb_id2sym(rb_intern("ephemeral"))) {
                entry.bucket_type = couchbase::core::management::cluster::bucket_type::ephemeral;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown bucket type, given %+" PRIsVALUE, bucket_type));
            }
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("bucket type must be a Symbol, given %+" PRIsVALUE, bucket_type));
        }
    }

    if (VALUE name = rb_hash_aref(bucket, rb_id2sym(rb_intern("name"))); TYPE(name) == T_STRING) {
        entry.name = cb_string_new(name);
    } else {
        throw ruby_exception(rb_eArgError, rb_sprintf("bucket name must be a String, given %+" PRIsVALUE, name));
    }

    if (VALUE quota = rb_hash_aref(bucket, rb_id2sym(rb_intern("ram_quota_mb"))); !NIL_P(quota)) {
        if (TYPE(quota) == T_FIXNUM) {
            entry.ram_quota_mb = FIX2ULONG(quota);
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("bucket RAM quota must be an Integer, given %+" PRIsVALUE, quota));
        }
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
                entry.compression_mode = couchbase::core::management::cluster::bucket_compression::active;
            } else if (compression_mode == rb_id2sym(rb_intern("passive"))) {
                entry.compression_mode = couchbase::core::management::cluster::bucket_compression::passive;
            } else if (compression_mode == rb_id2sym(rb_intern("off"))) {
                entry.compression_mode = couchbase::core::management::cluster::bucket_compression::off;
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
                entry.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::full;
            } else if (eviction_policy == rb_id2sym(rb_intern("value_only"))) {
                entry.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::value_only;
            } else if (eviction_policy == rb_id2sym(rb_intern("no_eviction"))) {
                entry.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::no_eviction;
            } else if (eviction_policy == rb_id2sym(rb_intern("not_recently_used"))) {
                entry.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown eviction policy, given %+" PRIsVALUE, eviction_policy));
            }
        } else {
            throw ruby_exception(rb_eArgError, rb_sprintf("bucket eviction policy must be a Symbol, given %+" PRIsVALUE, eviction_policy));
        }
    }

    if (VALUE storage_backend = rb_hash_aref(bucket, rb_id2sym(rb_intern("storage_backend"))); !NIL_P(storage_backend)) {
        if (TYPE(storage_backend) == T_SYMBOL) {
            if (storage_backend == rb_id2sym(rb_intern("couchstore"))) {
                entry.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::couchstore;
            } else if (storage_backend == rb_id2sym(rb_intern("magma"))) {
                entry.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::magma;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown storage backend type, given %+" PRIsVALUE, storage_backend));
            }
        } else {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("bucket storage backend type must be a Symbol, given %+" PRIsVALUE, storage_backend));
        }
    }

    if (VALUE minimum_level = rb_hash_aref(bucket, rb_id2sym(rb_intern("minimum_durability_level"))); !NIL_P(minimum_level)) {
        if (TYPE(minimum_level) == T_SYMBOL) {
            if (minimum_level == rb_id2sym(rb_intern("none"))) {
                entry.minimum_durability_level = couchbase::durability_level::none;
            } else if (minimum_level == rb_id2sym(rb_intern("majority"))) {
                entry.minimum_durability_level = couchbase::durability_level::majority;
            } else if (minimum_level == rb_id2sym(rb_intern("majority_and_persist_to_active"))) {
                entry.minimum_durability_level = couchbase::durability_level::majority_and_persist_to_active;
            } else if (minimum_level == rb_id2sym(rb_intern("persist_to_majority"))) {
                entry.minimum_durability_level = couchbase::durability_level::persist_to_majority;
            } else {
                throw ruby_exception(rb_eArgError, rb_sprintf("unknown durability level, given %+" PRIsVALUE, minimum_level));
            }
        } else {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("bucket minimum durability level must be a Symbol, given %+" PRIsVALUE, minimum_level));
        }
    }

    if (VALUE history_retention_collection_default = rb_hash_aref(bucket, rb_id2sym(rb_intern("history_retention_collection_default")));
        !NIL_P(history_retention_collection_default)) {
        entry.history_retention_collection_default = RTEST(history_retention_collection_default);
    }

    if (VALUE history_retention_bytes = rb_hash_aref(bucket, rb_id2sym(rb_intern("history_retention_bytes")));
        !NIL_P(history_retention_bytes)) {
        if (TYPE(history_retention_bytes) == T_FIXNUM) {
            entry.history_retention_bytes = FIX2UINT(history_retention_bytes);
        } else {
            throw ruby_exception(rb_eArgError,
                                 rb_sprintf("history retention bytes must be an Integer, given %+" PRIsVALUE, history_retention_bytes));
        }
    }

    if (VALUE history_retention_duration = rb_hash_aref(bucket, rb_id2sym(rb_intern("history_retention_duration")));
        !NIL_P(history_retention_duration)) {
        if (TYPE(history_retention_duration) == T_FIXNUM) {
            entry.history_retention_duration = FIX2UINT(history_retention_duration);
        } else {
            throw ruby_exception(
              rb_eArgError, rb_sprintf("history retention duration must be an Integer, given %+" PRIsVALUE, history_retention_duration));
        }
    }

    if (is_create) {
        if (VALUE conflict_resolution_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("conflict_resolution_type")));
            !NIL_P(conflict_resolution_type)) {
            if (TYPE(conflict_resolution_type) == T_SYMBOL) {
                if (conflict_resolution_type == rb_id2sym(rb_intern("timestamp"))) {
                    entry.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::timestamp;
                } else if (conflict_resolution_type == rb_id2sym(rb_intern("sequence_number"))) {
                    entry.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::sequence_number;
                } else if (conflict_resolution_type == rb_id2sym(rb_intern("custom"))) {
                    entry.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::custom;
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
        couchbase::core::operations::management::bucket_create_request req{};
        cb_extract_timeout(req, options);
        cb_generate_bucket_settings(bucket_settings, req.bucket, true);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::bucket_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::bucket_create_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format("unable to create bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
        }

        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::bucket_update_request req{};
        cb_extract_timeout(req, options);
        cb_generate_bucket_settings(bucket_settings, req.bucket, false);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::bucket_update_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::bucket_update_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format("unable to update bucket \"{}\" on the cluster ({})", req.bucket.name, resp.error_message));
        }
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::bucket_drop_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::bucket_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::bucket_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
        }
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::bucket_flush_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::bucket_flush_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::bucket_flush_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to flush bucket \"{}\" on the cluster", req.name));
        }

        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_bucket_settings(const couchbase::core::management::cluster::bucket_settings& entry, VALUE bucket)
{
    switch (entry.bucket_type) {
        case couchbase::core::management::cluster::bucket_type::couchbase:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("couchbase")));
            break;
        case couchbase::core::management::cluster::bucket_type::memcached:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("memcached")));
            break;
        case couchbase::core::management::cluster::bucket_type::ephemeral:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("ephemeral")));
            break;
        case couchbase::core::management::cluster::bucket_type::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), Qnil);
            break;
    }
    rb_hash_aset(bucket, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("uuid")), cb_str_new(entry.uuid));
    rb_hash_aset(bucket, rb_id2sym(rb_intern("ram_quota_mb")), ULL2NUM(entry.ram_quota_mb));
    if (const auto& val = entry.max_expiry; val.has_value()) {
        rb_hash_aset(bucket, rb_id2sym(rb_intern("max_expiry")), ULONG2NUM(val.value()));
    }
    switch (entry.compression_mode) {
        case couchbase::core::management::cluster::bucket_compression::off:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("off")));
            break;
        case couchbase::core::management::cluster::bucket_compression::active:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("active")));
            break;
        case couchbase::core::management::cluster::bucket_compression::passive:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("passive")));
            break;
        case couchbase::core::management::cluster::bucket_compression::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), Qnil);
            break;
    }
    if (const auto& val = entry.num_replicas; val.has_value()) {
        rb_hash_aset(bucket, rb_id2sym(rb_intern("num_replicas")), ULONG2NUM(val.value()));
    }
    if (const auto& val = entry.replica_indexes; val.has_value()) {
        rb_hash_aset(bucket, rb_id2sym(rb_intern("replica_indexes")), val.value() ? Qtrue : Qfalse);
    }
    if (const auto& val = entry.flush_enabled; val.has_value()) {
        rb_hash_aset(bucket, rb_id2sym(rb_intern("flush_enabled")), val.value() ? Qtrue : Qfalse);
    }
    switch (entry.eviction_policy) {
        case couchbase::core::management::cluster::bucket_eviction_policy::full:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("full")));
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::value_only:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("value_only")));
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::no_eviction:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("no_eviction")));
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("not_recently_used")));
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), Qnil);
            break;
    }
    switch (entry.conflict_resolution_type) {
        case couchbase::core::management::cluster::bucket_conflict_resolution::timestamp:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("timestamp")));
            break;
        case couchbase::core::management::cluster::bucket_conflict_resolution::sequence_number:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("sequence_number")));
            break;
        case couchbase::core::management::cluster::bucket_conflict_resolution::custom:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("custom")));
            break;
        case couchbase::core::management::cluster::bucket_conflict_resolution::unknown:
            rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), Qnil);
            break;
    }
    if (entry.minimum_durability_level) {
        switch (entry.minimum_durability_level.value()) {
            case couchbase::durability_level::none:
                rb_hash_aset(bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("none")));
                break;
            case couchbase::durability_level::majority:
                rb_hash_aset(bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("majority")));
                break;
            case couchbase::durability_level::majority_and_persist_to_active:
                rb_hash_aset(
                  bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("majority_and_persist_to_active")));
                break;
            case couchbase::durability_level::persist_to_majority:
                rb_hash_aset(bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("persist_to_majority")));
                break;
        }
    }
    if (entry.history_retention_collection_default.has_value()) {
        rb_hash_aset(bucket,
                     rb_id2sym(rb_intern("history_retention_collection_default")),
                     entry.history_retention_collection_default.value() ? Qtrue : Qfalse);
    }
    if (const auto& val = entry.history_retention_bytes; val.has_value()) {
        rb_hash_aset(bucket, rb_id2sym(rb_intern("history_retention_bytes")), ULONG2NUM(val.value()));
    }
    if (const auto& val = entry.history_retention_duration; val.has_value()) {
        rb_hash_aset(bucket, rb_id2sym(rb_intern("history_retention_duration")), ULONG2NUM(val.value()));
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
        couchbase::core::operations::management::bucket_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::bucket_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::bucket_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::bucket_get_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::bucket_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::bucket_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to locate bucket \"{}\" on the cluster", req.name));
        }

        VALUE res = rb_hash_new();
        cb_extract_bucket_settings(resp.bucket, res);
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_role(const couchbase::core::management::rbac::role_and_description& entry, VALUE role)
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
        couchbase::core::operations::management::role_get_all_request req{};
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::role_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::role_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_user(const couchbase::core::management::rbac::user_and_metadata& entry, VALUE user)
{
    rb_hash_aset(user, rb_id2sym(rb_intern("username")), cb_str_new(entry.username));
    switch (entry.domain) {
        case couchbase::core::management::rbac::auth_domain::local:
            rb_hash_aset(user, rb_id2sym(rb_intern("domain")), rb_id2sym(rb_intern("local")));
            break;
        case couchbase::core::management::rbac::auth_domain::external:
            rb_hash_aset(user, rb_id2sym(rb_intern("domain")), rb_id2sym(rb_intern("external")));
            break;
        case couchbase::core::management::rbac::auth_domain::unknown:
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
        couchbase::core::operations::management::user_get_all_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::user_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::user_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::user_get_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        req.username = cb_string_new(username);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::user_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::user_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to fetch user "{}")", req.username));
        }

        VALUE res = rb_hash_new();
        cb_extract_user(resp.user, res);
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::user_drop_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::external;
        } else {
            throw ruby_exception(eInvalidArgument, rb_sprintf("unsupported authentication domain: %+" PRIsVALUE, domain));
        }
        req.username = cb_string_new(username);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::user_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::user_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to fetch user "{}")", req.username));
        }

        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::user_upsert_request req{};
        cb_extract_timeout(req, timeout);
        if (domain == rb_id2sym(rb_intern("local"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::local;
        } else if (domain == rb_id2sym(rb_intern("external"))) {
            req.domain = couchbase::core::management::rbac::auth_domain::external;
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
            auto groups_size = static_cast<std::size_t>(RARRAY_LEN(groups));
            for (std::size_t i = 0; i < groups_size; ++i) {
                if (VALUE entry = rb_ary_entry(groups, static_cast<long>(i)); TYPE(entry) == T_STRING) {
                    req.user.groups.emplace(cb_string_new(entry));
                }
            }
        }
        if (VALUE roles = rb_hash_aref(user, rb_id2sym(rb_intern("roles"))); !NIL_P(roles) && TYPE(roles) == T_ARRAY) {
            auto roles_size = static_cast<std::size_t>(RARRAY_LEN(roles));
            req.user.roles.reserve(roles_size);
            for (std::size_t i = 0; i < roles_size; ++i) {
                VALUE entry = rb_ary_entry(roles, static_cast<long>(i));
                if (TYPE(entry) == T_HASH) {
                    couchbase::core::management::rbac::role role{};
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
                    req.user.roles.emplace_back(role);
                }
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::user_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::user_upsert_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format(R"(unable to upsert user "{}" ({}))", req.user.username, fmt::join(resp.errors, ", ")));
        }

        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_change_password(VALUE self, VALUE new_password, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(new_password, T_STRING);

    try {
        couchbase::core::operations::management::change_password_request req{};
        cb_extract_timeout(req, timeout);
        req.newPassword = cb_string_new(new_password);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::change_password_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::change_password_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to change password");
        }

        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_group(const couchbase::core::management::rbac::group& entry, VALUE group)
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
        couchbase::core::operations::management::group_get_all_request req{};
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::group_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::group_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::group_get_request req{};
        cb_extract_timeout(req, timeout);
        req.name = cb_string_new(name);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::group_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::group_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to fetch group "{}")", req.name));
        }

        VALUE res = rb_hash_new();
        cb_extract_group(resp.group, res);
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::group_drop_request req{};
        cb_extract_timeout(req, timeout);
        req.name = cb_string_new(name);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::group_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::group_drop_response&& resp) { barrier->set_value(std::move(resp)); });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to drop group "{}")", req.name));
        }
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::group_upsert_request req{};
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
            auto roles_size = static_cast<std::size_t>(RARRAY_LEN(roles));
            req.group.roles.reserve(roles_size);
            for (std::size_t i = 0; i < roles_size; ++i) {
                if (VALUE entry = rb_ary_entry(roles, static_cast<long>(i)); TYPE(entry) == T_HASH) {
                    couchbase::core::management::rbac::role role{};
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
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::group_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::group_upsert_response&& resp) { barrier->set_value(std::move(resp)); });
        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format(R"(unable to upsert group "{}" ({}))", req.group.name, fmt::join(resp.errors, ", ")));
        }
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::cluster_developer_preview_enable_request req{};
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::cluster_developer_preview_enable_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::cluster_developer_preview_enable_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to enable developer preview for this cluster");
        }
        spdlog::critical(
          "Developer preview cannot be disabled once it is enabled. If you enter developer preview mode you will not be able to "
          "upgrade. DO NOT USE IN PRODUCTION.");
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::scope_get_all_request req{ cb_string_new(bucket_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::scope_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::scope_get_all_response&& resp) { barrier->set_value(std::move(resp)); });
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
                rb_hash_aset(collection, rb_id2sym(rb_intern("max_expiry")), LONG2NUM(c.max_expiry));
                if (c.history.has_value()) {
                    rb_hash_aset(collection, rb_id2sym(rb_intern("history")), c.history.value() ? Qtrue : Qfalse);
                }
                rb_ary_push(collections, collection);
            }
            rb_hash_aset(scope, rb_id2sym(rb_intern("collections")), collections);
            rb_ary_push(scopes, scope);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("scopes")), scopes);

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::collections_manifest_get_request req{ couchbase::core::document_id{
          cb_string_new(bucket_name), "_default", "_default", "" } };
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::collections_manifest_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::collections_manifest_get_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::scope_create_request req{ cb_string_new(bucket_name), cb_string_new(scope_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::scope_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::scope_create_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format(R"(unable to create the scope "{}" on the bucket "{}")", req.scope_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::scope_drop_request req{ cb_string_new(bucket_name), cb_string_new(scope_name) };
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::scope_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::scope_drop_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx,
                                fmt::format(R"(unable to drop the scope "{}" on the bucket "{}")", req.scope_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE settings, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(settings)) {
        Check_Type(settings, T_HASH);
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::collection_create_request req{ cb_string_new(bucket_name),
                                                                                cb_string_new(scope_name),
                                                                                cb_string_new(collection_name) };
        cb_extract_timeout(req, options);

        if (!NIL_P(settings)) {
            if (VALUE max_expiry = rb_hash_aref(settings, rb_id2sym(rb_intern("max_expiry"))); !NIL_P(max_expiry)) {
                if (TYPE(max_expiry) == T_FIXNUM) {
                    req.max_expiry = FIX2INT(max_expiry);
                    if (req.max_expiry < -1) {
                        throw ruby_exception(
                          eInvalidArgument,
                          rb_sprintf("collection max expiry must be greater than or equal to -1, given %+" PRIsVALUE, max_expiry));
                    }
                } else {
                    throw ruby_exception(rb_eArgError,
                                         rb_sprintf("collection max expiry must be an Integer, given %+" PRIsVALUE, max_expiry));
                }
            }
            if (VALUE history = rb_hash_aref(settings, rb_id2sym(rb_intern("history"))); !NIL_P(history)) {
                req.history = RTEST(history);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::collection_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::collection_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable create the collection "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_update(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE settings, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(settings)) {
        Check_Type(settings, T_HASH);
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::collection_update_request req{ cb_string_new(bucket_name),
                                                                                cb_string_new(scope_name),
                                                                                cb_string_new(collection_name) };
        cb_extract_timeout(req, options);

        if (!NIL_P(settings)) {
            if (VALUE max_expiry = rb_hash_aref(settings, rb_id2sym(rb_intern("max_expiry"))); !NIL_P(max_expiry)) {
                if (TYPE(max_expiry) == T_FIXNUM) {
                    req.max_expiry = FIX2INT(max_expiry);
                    if (req.max_expiry < -1) {
                        throw ruby_exception(
                          eInvalidArgument,
                          rb_sprintf("collection max expiry must be greater than or equal to -1, given %+" PRIsVALUE, max_expiry));
                    }
                } else {
                    throw ruby_exception(rb_eArgError,
                                         rb_sprintf("collection max expiry must be an Integer, given %+" PRIsVALUE, max_expiry));
                }
            }
            if (VALUE history = rb_hash_aref(settings, rb_id2sym(rb_intern("history"))); !NIL_P(history)) {
                req.history = RTEST(history);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::collection_update_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::collection_update_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable update the collection "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::collection_drop_request req{ cb_string_new(bucket_name),
                                                                              cb_string_new(scope_name),
                                                                              cb_string_new(collection_name) };
        cb_extract_timeout(req, options);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::collection_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::collection_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable to drop the collection  "{}.{}" on the bucket "{}")", req.scope_name, req.collection_name, req.bucket_name));
        }
        return ULL2NUM(resp.uid);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::query_index_get_all_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        cb_extract_timeout(req, options);
        if (!NIL_P(options)) {
            if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name"))); TYPE(scope_name) == T_STRING) {
                req.scope_name = cb_string_new(scope_name);
            }
            if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name"))); TYPE(collection_name) == T_STRING) {
                req.collection_name = cb_string_new(collection_name);
            }
        }
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to get list of the indexes of the bucket \"{}\"", req.bucket_name));
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            rb_hash_aset(index, rb_id2sym(rb_intern("state")), rb_id2sym(rb_intern(idx.state.c_str())));
            rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
            rb_hash_aset(index, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern(idx.type.c_str())));
            rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
            VALUE index_key = rb_ary_new_capa(static_cast<long>(idx.index_key.size()));
            for (const auto& key : idx.index_key) {
                rb_ary_push(index_key, cb_str_new(key));
            }
            rb_hash_aset(index, rb_id2sym(rb_intern("index_key")), index_key);
            if (idx.collection_name) {
                rb_hash_aset(index, rb_id2sym(rb_intern("collection_name")), cb_str_new(idx.collection_name.value()));
            }
            if (idx.scope_name) {
                rb_hash_aset(index, rb_id2sym(rb_intern("scope_name")), cb_str_new(idx.scope_name.value()));
            }
            rb_hash_aset(index, rb_id2sym(rb_intern("bucket_name")), cb_str_new(idx.bucket_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::query_index_create_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        req.index_name = cb_string_new(index_name);
        auto fields_num = static_cast<std::size_t>(RARRAY_LEN(fields));
        req.fields.reserve(fields_num);
        for (std::size_t i = 0; i < fields_num; ++i) {
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::query_index_drop_request req{};
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::query_index_create_request req{};
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::query_index_drop_request req{};
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::query_index_build_deferred_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);

        if (!NIL_P(options)) {
            if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name"))); TYPE(scope_name) == T_STRING) {
                req.scope_name = cb_string_new(scope_name);
            }
            if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name"))); TYPE(collection_name) == T_STRING) {
                req.collection_name = cb_string_new(collection_name);
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_build_deferred_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_build_deferred_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to build deferred indexes on the bucket "{}" ({}: {}))",
                                                req.bucket_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to build deferred indexes on the bucket "{}")", req.bucket_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_query_index_get_all(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::query_index_get_all_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.scope_name = cb_string_new(scope_name);
        req.collection_name = cb_string_new(collection_name);
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to get list of the indexes of the collection \"{}\"", req.collection_name));
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
        VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
        for (const auto& idx : resp.indexes) {
            VALUE index = rb_hash_new();
            rb_hash_aset(index, rb_id2sym(rb_intern("state")), rb_id2sym(rb_intern(idx.state.c_str())));
            rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
            rb_hash_aset(index, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern(idx.type.c_str())));
            rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
            VALUE index_key = rb_ary_new_capa(static_cast<long>(idx.index_key.size()));
            for (const auto& key : idx.index_key) {
                rb_ary_push(index_key, cb_str_new(key));
            }
            rb_hash_aset(index, rb_id2sym(rb_intern("index_key")), index_key);
            if (idx.collection_name) {
                rb_hash_aset(index, rb_id2sym(rb_intern("collection_name")), cb_str_new(idx.collection_name.value()));
            }
            if (idx.scope_name) {
                rb_hash_aset(index, rb_id2sym(rb_intern("scope_name")), cb_str_new(idx.scope_name.value()));
            }
            rb_hash_aset(index, rb_id2sym(rb_intern("bucket_name")), cb_str_new(idx.bucket_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_query_index_create(VALUE self,
                                         VALUE bucket_name,
                                         VALUE scope_name,
                                         VALUE collection_name,
                                         VALUE index_name,
                                         VALUE fields,
                                         VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    Check_Type(index_name, T_STRING);
    Check_Type(fields, T_ARRAY);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::query_index_create_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        req.scope_name = cb_string_new(scope_name);
        req.collection_name = cb_string_new(collection_name);
        req.index_name = cb_string_new(index_name);
        auto fields_num = static_cast<std::size_t>(RARRAY_LEN(fields));
        req.fields.reserve(fields_num);
        for (std::size_t i = 0; i < fields_num; ++i) {
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to create index "{}" on the collection "{}" ({}: {}))",
                                                req.index_name,
                                                req.collection_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(
                  resp.ctx, fmt::format(R"(unable to create index "{}" on the collection "{}")", req.index_name, req.collection_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_query_index_drop(VALUE self,
                                       VALUE bucket_name,
                                       VALUE scope_name,
                                       VALUE collection_name,
                                       VALUE index_name,
                                       VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    Check_Type(index_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::query_index_drop_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        req.scope_name = cb_string_new(scope_name);
        req.collection_name = cb_string_new(collection_name);
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to drop index "{}" on the collection "{}" ({}: {}))",
                                                req.index_name,
                                                req.collection_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(
                  resp.ctx, fmt::format(R"(unable to drop index "{}" on the collection "{}")", req.index_name, req.collection_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_query_index_create_primary(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::query_index_create_request req{};
        cb_extract_timeout(req, options);
        req.is_primary = true;
        req.bucket_name = cb_string_new(bucket_name);
        req.scope_name = cb_string_new(scope_name);
        req.collection_name = cb_string_new(collection_name);
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_create_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to create primary index on the collection "{}" ({}: {}))",
                                                req.collection_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to create primary index on the collection "{}")", req.collection_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_query_index_drop_primary(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::query_index_drop_request req{};
        cb_extract_timeout(req, options);
        req.is_primary = true;
        req.bucket_name = cb_string_new(bucket_name);
        req.scope_name = cb_string_new(scope_name);
        req.collection_name = cb_string_new(collection_name);
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to drop primary index on the collection "{}" ({}: {}))",
                                                req.collection_name,
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, fmt::format(R"(unable to drop primary index on the collection "{}")", req.collection_name));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_collection_query_index_build_deferred(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE collection_name, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);
    Check_Type(scope_name, T_STRING);
    Check_Type(collection_name, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::management::query_index_build_deferred_request req{};
        cb_extract_timeout(req, options);
        req.bucket_name = cb_string_new(bucket_name);
        req.scope_name = cb_string_new(scope_name);
        req.collection_name = cb_string_new(collection_name);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::query_index_build_deferred_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::query_index_build_deferred_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.errors.empty()) {
                const auto& first_error = resp.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to build deferred indexes on the collection "{}" ({}: {}))",
                                                req.collection_name.value(),
                                                first_error.code,
                                                first_error.message));
            } else {
                cb_throw_error_code(resp.ctx,
                                    fmt::format(R"(unable to build deferred indexes on the collection "{}")", req.collection_name.value()));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_extract_search_index(VALUE index, const couchbase::core::management::search::index& idx)
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
cb_Backend_search_index_get_all(VALUE self, VALUE bucket, VALUE scope, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    try {
        couchbase::core::operations::management::search_index_get_all_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }

        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_get(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_get_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_get_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_upsert(VALUE self, VALUE bucket, VALUE scope, VALUE index_definition, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_definition, T_HASH);

    try {
        couchbase::core::operations::management::search_index_upsert_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_upsert_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_drop(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_drop_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_get_documents_count(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_get_documents_count_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_get_documents_count_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_get_documents_count_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::search_index_get_stats_request req{};
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_get_stats_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_get_stats_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::search_get_stats_request req{};
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_get_stats_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_get_stats_response&& resp) {
            barrier->set_value(std::move(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, "unable to get stats for the search service");
        }
        return cb_str_new(resp.stats);
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_pause_ingest(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_control_ingest_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.pause = true;
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_control_ingest_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_control_ingest_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_resume_ingest(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_control_ingest_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.pause = false;
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_control_ingest_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_control_ingest_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_allow_querying(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_control_query_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.allow = true;
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_control_query_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_control_query_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_disallow_querying(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_control_query_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.allow = false;
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_control_query_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_control_query_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_freeze_plan(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_control_plan_freeze_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.freeze = true;
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_control_plan_freeze_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_control_plan_freeze_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_unfreeze_plan(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);

    try {
        couchbase::core::operations::management::search_index_control_plan_freeze_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);
        req.index_name = cb_string_new(index_name);
        req.freeze = false;
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_control_plan_freeze_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_control_plan_freeze_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_search_index_analyze_document(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE encoded_document, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);
    Check_Type(encoded_document, T_STRING);

    try {
        couchbase::core::operations::management::search_index_analyze_document_request req{};
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        cb_extract_timeout(req, timeout);

        req.index_name = cb_string_new(index_name);
        req.encoded_document = cb_string_new(encoded_document);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::search_index_analyze_document_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::search_index_analyze_document_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_document_search(VALUE self, VALUE bucket, VALUE scope, VALUE index_name, VALUE query, VALUE search_request, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(index_name, T_STRING);
    Check_Type(query, T_STRING);
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::search_request req;
        if (!NIL_P(bucket)) {
            cb_check_type(bucket, T_STRING);
            req.bucket_name = cb_string_new(bucket);
        }
        if (!NIL_P(scope)) {
            cb_check_type(scope, T_STRING);
            req.scope_name = cb_string_new(scope);
        }
        if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id"))); !NIL_P(client_context_id)) {
            cb_check_type(client_context_id, T_STRING);
            req.client_context_id = cb_string_new(client_context_id);
        }
        cb_extract_timeout(req, options);
        req.index_name = cb_string_new(index_name);
        req.query = cb_string_new(query);

        cb_extract_option_bool(req.explain, options, "explain");
        cb_extract_option_bool(req.disable_scoring, options, "disable_scoring");
        cb_extract_option_bool(req.include_locations, options, "include_locations");
        cb_extract_option_bool(req.show_request, options, "show_request");

        if (VALUE vector_options = rb_hash_aref(search_request, rb_id2sym(rb_intern("vector_search"))); !NIL_P(vector_options)) {
            cb_check_type(vector_options, T_HASH);
            if (VALUE vector_queries = rb_hash_aref(vector_options, rb_id2sym(rb_intern("vector_queries"))); !NIL_P(vector_queries)) {
                cb_check_type(vector_queries, T_STRING);
                req.vector_search = cb_string_new(vector_queries);
            }
            if (VALUE vector_query_combination = rb_hash_aref(vector_options, rb_id2sym(rb_intern("vector_query_combination")));
                !NIL_P(vector_query_combination)) {
                cb_check_type(vector_query_combination, T_SYMBOL);
                ID type = rb_sym2id(vector_query_combination);
                if (type == rb_intern("and")) {
                    req.vector_query_combination = couchbase::core::vector_query_combination::combination_and;
                } else if (type == rb_intern("or")) {
                    req.vector_query_combination = couchbase::core::vector_query_combination::combination_or;
                }
            }
        }

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
                req.highlight_style = couchbase::core::search_highlight_style::html;
            } else if (type == rb_intern("ansi")) {
                req.highlight_style = couchbase::core::search_highlight_style::ansi;
            }
        }

        if (VALUE highlight_fields = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_fields"))); !NIL_P(highlight_fields)) {
            cb_check_type(highlight_fields, T_ARRAY);
            auto highlight_fields_size = static_cast<std::size_t>(RARRAY_LEN(highlight_fields));
            req.highlight_fields.reserve(highlight_fields_size);
            for (std::size_t i = 0; i < highlight_fields_size; ++i) {
                VALUE field = rb_ary_entry(highlight_fields, static_cast<long>(i));
                cb_check_type(field, T_STRING);
                req.highlight_fields.emplace_back(cb_string_new(field));
            }
        }

        if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
            cb_check_type(scan_consistency, T_SYMBOL);
            if (ID type = rb_sym2id(scan_consistency); type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::core::search_scan_consistency::not_bounded;
            }
        }

        if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state"))); !NIL_P(mutation_state)) {
            cb_check_type(mutation_state, T_ARRAY);
            auto state_size = static_cast<std::size_t>(RARRAY_LEN(mutation_state));
            req.mutation_state.reserve(state_size);
            for (std::size_t i = 0; i < state_size; ++i) {
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
                req.mutation_state.emplace_back(NUM2ULL(partition_uuid),
                                                NUM2ULL(sequence_number),
                                                gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                                cb_string_new(bucket_name));
            }
        }

        if (VALUE fields = rb_hash_aref(options, rb_id2sym(rb_intern("fields"))); !NIL_P(fields)) {
            cb_check_type(fields, T_ARRAY);
            auto fields_size = static_cast<std::size_t>(RARRAY_LEN(fields));
            req.fields.reserve(fields_size);
            for (std::size_t i = 0; i < fields_size; ++i) {
                VALUE field = rb_ary_entry(fields, static_cast<long>(i));
                cb_check_type(field, T_STRING);
                req.fields.emplace_back(cb_string_new(field));
            }
        }

        VALUE collections = rb_hash_aref(options, rb_id2sym(rb_intern("collections")));
        if (!NIL_P(collections)) {
            cb_check_type(collections, T_ARRAY);
            auto collections_size = static_cast<std::size_t>(RARRAY_LEN(collections));
            req.collections.reserve(collections_size);
            for (std::size_t i = 0; i < collections_size; ++i) {
                VALUE collection = rb_ary_entry(collections, static_cast<long>(i));
                cb_check_type(collection, T_STRING);
                req.collections.emplace_back(cb_string_new(collection));
            }
        }

        if (VALUE sort = rb_hash_aref(options, rb_id2sym(rb_intern("sort"))); !NIL_P(sort)) {
            cb_check_type(sort, T_ARRAY);
            for (std::size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(sort)); ++i) {
                VALUE sort_spec = rb_ary_entry(sort, static_cast<long>(i));
                req.sort_specs.emplace_back(cb_string_new(sort_spec));
            }
        }

        if (VALUE facets = rb_hash_aref(options, rb_id2sym(rb_intern("facets"))); !NIL_P(facets)) {
            cb_check_type(facets, T_ARRAY);
            for (std::size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(facets)); ++i) {
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::search_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::search_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(resp.ctx, fmt::format("unable to perform search query for index \"{}\": {}", req.index_name, resp.error));
        }
        VALUE res = rb_hash_new();

        VALUE meta_data = rb_hash_new();
        rb_hash_aset(meta_data, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.meta.client_context_id));

        VALUE metrics = rb_hash_new();
        rb_hash_aset(metrics,
                     rb_id2sym(rb_intern("took")),
                     LL2NUM(std::chrono::duration_cast<std::chrono::milliseconds>(resp.meta.metrics.took).count()));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("total_rows")), ULL2NUM(resp.meta.metrics.total_rows));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("max_score")), DBL2NUM(resp.meta.metrics.max_score));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("success_partition_count")), ULL2NUM(resp.meta.metrics.success_partition_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("error_partition_count")), ULL2NUM(resp.meta.metrics.error_partition_count));
        rb_hash_aset(meta_data, rb_id2sym(rb_intern("metrics")), metrics);

        if (!resp.meta.errors.empty()) {
            VALUE errors = rb_hash_new();
            for (const auto& [code, message] : resp.meta.errors) {
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
                rb_hash_aset(location, rb_id2sym(rb_intern("field")), cb_str_new(loc.field));
                rb_hash_aset(location, rb_id2sym(rb_intern("term")), cb_str_new(loc.term));
                rb_hash_aset(location, rb_id2sym(rb_intern("pos")), ULL2NUM(loc.position));
                rb_hash_aset(location, rb_id2sym(rb_intern("start_offset")), ULL2NUM(loc.start_offset));
                rb_hash_aset(location, rb_id2sym(rb_intern("end_offset")), ULL2NUM(loc.end_offset));
                if (loc.array_positions) {
                    VALUE ap = rb_ary_new_capa(static_cast<long>(loc.array_positions->size()));
                    for (const auto& pos : *loc.array_positions) {
                        rb_ary_push(ap, ULL2NUM(pos));
                    }
                    rb_hash_aset(location, rb_id2sym(rb_intern("array_positions")), ap);
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

        couchbase::core::io::dns::dns_client client(ctx);
        std::string host_name = cb_string_new(hostname);
        std::string service_name("_couchbase");
        if (tls) {
            service_name = "_couchbases";
        }
        auto barrier = std::make_shared<std::promise<couchbase::core::io::dns::dns_srv_response>>();
        auto f = barrier->get_future();
        client.query_srv(host_name,
                         service_name,
                         couchbase::core::io::dns::dns_config::system_config(),
                         [barrier](couchbase::core::io::dns::dns_srv_response&& resp) { barrier->set_value(std::move(resp)); });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_get_pending_mutations_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_get_pending_mutations_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_get_pending_mutations_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_dataset_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_dataset_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_dataset_get_all_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_dataset_drop_request req{};
        cb_extract_timeout(req, options);
        req.dataset_name = cb_string_new(dataset_name);
        VALUE dataverse_name = Qnil;
        cb_extract_option_string(dataverse_name, options, "dataverse_name");
        if (!NIL_P(dataverse_name)) {
            req.dataverse_name = cb_string_new(dataverse_name);
        }
        cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_dataset_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_dataset_drop_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_dataset_create_request req{};
        cb_extract_timeout(req, options);
        req.dataset_name = cb_string_new(dataset_name);
        req.bucket_name = cb_string_new(bucket_name);
        cb_extract_option_string(req.condition, options, "condition");
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_dataset_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_dataset_create_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_dataverse_drop_request req{};
        cb_extract_timeout(req, options);
        req.dataverse_name = cb_string_new(dataverse_name);
        cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_dataverse_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_dataverse_drop_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_dataverse_create_request req{};
        cb_extract_timeout(req, options);
        req.dataverse_name = cb_string_new(dataverse_name);
        cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_dataverse_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_dataverse_create_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_index_get_all_request req{};
        cb_extract_timeout(req, options);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_index_get_all_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_index_create_request req{};
        cb_extract_timeout(req, options);
        req.index_name = cb_string_new(index_name);
        req.dataset_name = cb_string_new(dataset_name);
        auto fields_num = static_cast<std::size_t>(RARRAY_LEN(fields));
        for (std::size_t i = 0; i < fields_num; ++i) {
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
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_index_create_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_index_create_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_index_drop_request req{};
        cb_extract_timeout(req, options);
        req.index_name = cb_string_new(index_name);
        req.dataset_name = cb_string_new(dataset_name);
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_link_connect_request req{};
        cb_extract_timeout(req, options);
        cb_extract_option_string(req.link_name, options, "link_name");
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        cb_extract_option_bool(req.force, options, "force");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_connect_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_connect_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_link_disconnect_request req{};
        cb_extract_timeout(req, options);
        cb_extract_option_string(req.link_name, options, "link_name");
        cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_disconnect_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_disconnect_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static void
cb_fill_link(couchbase::core::management::analytics::couchbase_remote_link& dst, VALUE src)
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
        dst.encryption.level = couchbase::core::management::analytics::couchbase_link_encryption_level::none;
    } else if (level == rb_intern("half")) {
        dst.encryption.level = couchbase::core::management::analytics::couchbase_link_encryption_level::half;
    } else if (level == rb_intern("full")) {
        dst.encryption.level = couchbase::core::management::analytics::couchbase_link_encryption_level::full;
    }
    cb_extract_option_string(dst.encryption.certificate, src, "certificate");
    cb_extract_option_string(dst.encryption.client_certificate, src, "client_certificate");
    cb_extract_option_string(dst.encryption.client_key, src, "client_key");
}

static void
cb_fill_link(couchbase::core::management::analytics::azure_blob_external_link& dst, VALUE src)
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
cb_fill_link(couchbase::core::management::analytics::s3_external_link& dst, VALUE src)
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
            couchbase::core::operations::management::analytics_link_create_request<
              couchbase::core::management::analytics::couchbase_remote_link>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_create_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_create_response&& resp) {
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
            couchbase::core::operations::management::analytics_link_create_request<
              couchbase::core::management::analytics::azure_blob_external_link>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_create_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_create_response&& resp) {
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
            couchbase::core::operations::management::analytics_link_create_request<couchbase::core::management::analytics::s3_external_link>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_create_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_create_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
            couchbase::core::operations::management::analytics_link_replace_request<
              couchbase::core::management::analytics::couchbase_remote_link>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_replace_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_replace_response&& resp) {
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
            couchbase::core::operations::management::analytics_link_replace_request<
              couchbase::core::management::analytics::azure_blob_external_link>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_replace_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_replace_response&& resp) {
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
            couchbase::core::operations::management::analytics_link_replace_request<
              couchbase::core::management::analytics::s3_external_link>
              req{};
            cb_extract_timeout(req, options);
            cb_fill_link(req.link, link);

            auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_replace_response>>();
            auto f = barrier->get_future();
            cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_replace_response&& resp) {
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_link_drop_request req{};
        cb_extract_timeout(req, options);

        req.link_name = cb_string_new(link);
        req.dataverse_name = cb_string_new(dataverse);

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });

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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        cb_extract_timeout(req, options);

        cb_extract_option_string(req.link_type, options, "link_type");
        cb_extract_option_string(req.link_name, options, "link_name");
        cb_extract_option_string(req.dataverse_name, options, "dataverse");

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::analytics_link_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::analytics_link_get_all_response&& resp) {
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
                case couchbase::core::management::analytics::couchbase_link_encryption_level::none:
                    rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("none")));
                    break;
                case couchbase::core::management::analytics::couchbase_link_encryption_level::half:
                    rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("half")));
                    break;
                case couchbase::core::management::analytics::couchbase_link_encryption_level::full:
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static const char*
cb_analytics_status_str(couchbase::core::operations::analytics_response::analytics_status status)
{
    switch (status) {
        case couchbase::core::operations::analytics_response::running:
            return "running";
        case couchbase::core::operations::analytics_response::success:
            return "success";
        case couchbase::core::operations::analytics_response::errors:
            return "errors";
        case couchbase::core::operations::analytics_response::completed:
            return "completed";
        case couchbase::core::operations::analytics_response::stopped:
            return "stopped";
        case couchbase::core::operations::analytics_response::timedout:
            return "timedout";
        case couchbase::core::operations::analytics_response::closed:
            return "closed";
        case couchbase::core::operations::analytics_response::fatal:
            return "fatal";
        case couchbase::core::operations::analytics_response::aborted:
            return "aborted";
        case couchbase::core::operations::analytics_response::unknown:
            return "unknown";
        default:
            break;
    }
    return "unknown";
}

static int
cb_for_each_named_param_analytics(VALUE key, VALUE value, VALUE arg)
{
    auto* preq = reinterpret_cast<couchbase::core::operations::analytics_request*>(arg);
    cb_check_type(key, T_STRING);
    cb_check_type(value, T_STRING);
    preq->named_parameters[cb_string_new(key)] = cb_string_new(value);
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
        couchbase::core::operations::analytics_request req;
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
            auto entries_num = static_cast<std::size_t>(RARRAY_LEN(positional_params));
            req.positional_parameters.reserve(entries_num);
            for (std::size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
                cb_check_type(entry, T_STRING);
                req.positional_parameters.emplace_back(cb_string_new(entry));
            }
        }
        if (VALUE named_params = rb_hash_aref(options, rb_id2sym(rb_intern("named_parameters"))); !NIL_P(named_params)) {
            cb_check_type(named_params, T_HASH);
            rb_hash_foreach(named_params, INT_FUNC(cb_for_each_named_param_analytics), reinterpret_cast<VALUE>(&req));
        }
        if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
            cb_check_type(scan_consistency, T_SYMBOL);
            if (ID type = rb_sym2id(scan_consistency); type == rb_intern("not_bounded")) {
                req.scan_consistency = couchbase::core::analytics_scan_consistency::not_bounded;
            } else if (type == rb_intern("request_plus")) {
                req.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
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

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::analytics_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::analytics_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            if (!resp.meta.errors.empty()) {
                const auto& first_error = resp.meta.errors.front();
                cb_throw_error_code(resp.ctx,
                                    fmt::format("unable to execute analytics query ({}: {})", first_error.code, first_error.message));
            } else {
                cb_throw_error_code(resp.ctx, "unable to execute analytics query");
            }
        }
        VALUE res = rb_hash_new();
        VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
        rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
        for (const auto& row : resp.rows) {
            rb_ary_push(rows, cb_str_new(row));
        }
        VALUE meta = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
        rb_hash_aset(meta, rb_id2sym(rb_intern("status")), rb_id2sym(rb_intern(cb_analytics_status_str(resp.meta.status))));
        rb_hash_aset(meta, rb_id2sym(rb_intern("request_id")), cb_str_new(resp.meta.request_id));
        rb_hash_aset(meta, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.meta.client_context_id));
        if (resp.meta.signature) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("signature")), cb_str_new(resp.meta.signature.value()));
        }
        VALUE metrics = rb_hash_new();
        rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
        rb_hash_aset(metrics, rb_id2sym(rb_intern("elapsed_time")), resp.meta.metrics.elapsed_time.count());
        rb_hash_aset(metrics, rb_id2sym(rb_intern("execution_time")), resp.meta.metrics.execution_time.count());
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.meta.metrics.result_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.meta.metrics.result_size));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("error_count")), ULL2NUM(resp.meta.metrics.error_count));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("processed_objects")), ULL2NUM(resp.meta.metrics.processed_objects));
        rb_hash_aset(metrics, rb_id2sym(rb_intern("warning_count")), ULL2NUM(resp.meta.metrics.warning_count));

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

    std::string input(RSTRING_PTR(connection_string), static_cast<std::size_t>(RSTRING_LEN(connection_string)));
    auto connstr = couchbase::core::utils::parse_connection_string(input);

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
            case couchbase::core::utils::connection_string::bootstrap_mode::gcccp:
                rb_hash_aset(node, rb_id2sym(rb_intern("mode")), rb_id2sym(rb_intern("gcccp")));
                break;
            case couchbase::core::utils::connection_string::bootstrap_mode::http:
                rb_hash_aset(node, rb_id2sym(rb_intern("mode")), rb_id2sym(rb_intern("http")));
                break;
            case couchbase::core::utils::connection_string::bootstrap_mode::unspecified:
                break;
        }
        switch (entry.type) {
            case couchbase::core::utils::connection_string::address_type::ipv4:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("ipv4")));
                break;
            case couchbase::core::utils::connection_string::address_type::ipv6:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("ipv6")));
                break;
            case couchbase::core::utils::connection_string::address_type::dns:
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
        case couchbase::core::utils::connection_string::bootstrap_mode::gcccp:
            rb_hash_aset(res, rb_id2sym(rb_intern("default_mode")), rb_id2sym(rb_intern("gcccp")));
            break;
        case couchbase::core::utils::connection_string::bootstrap_mode::http:
            rb_hash_aset(res, rb_id2sym(rb_intern("default_mode")), rb_id2sym(rb_intern("http")));
            break;
        case couchbase::core::utils::connection_string::bootstrap_mode::unspecified:
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

    couchbase::core::design_document_namespace ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::core::design_document_namespace::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::core::design_document_namespace::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::core::operations::management::view_index_get_all_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.ns = ns;
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::view_index_get_all_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::view_index_get_all_response&& resp) {
            barrier->set_value(std::move(resp));
        });
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
                case couchbase::core::design_document_namespace::development:
                    rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
                    break;
                case couchbase::core::design_document_namespace::production:
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

    couchbase::core::design_document_namespace ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::core::design_document_namespace::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::core::design_document_namespace::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::core::operations::management::view_index_get_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document_name = cb_string_new(document_name);
        req.ns = ns;
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::view_index_get_response>>();
        auto f = barrier->get_future();
        cluster->execute(
          req, [barrier](couchbase::core::operations::management::view_index_get_response&& resp) { barrier->set_value(std::move(resp)); });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(R"(unable to get design document "{}" ({}) on bucket "{}")", req.document_name, req.ns, req.bucket_name));
        }
        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("name")), cb_str_new(resp.document.name));
        rb_hash_aset(res, rb_id2sym(rb_intern("rev")), cb_str_new(resp.document.rev));
        switch (resp.document.ns) {
            case couchbase::core::design_document_namespace::development:
                rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
                break;
            case couchbase::core::design_document_namespace::production:
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

    couchbase::core::design_document_namespace ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::core::design_document_namespace::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::core::design_document_namespace::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::core::operations::management::view_index_drop_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document_name = cb_string_new(document_name);
        req.ns = ns;
        cb_extract_timeout(req, timeout);
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::view_index_drop_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::view_index_drop_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(R"(unable to drop design document "{}" ({}) on bucket "{}")", req.document_name, req.ns, req.bucket_name));
        }
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

    couchbase::core::design_document_namespace ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::core::design_document_namespace::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::core::design_document_namespace::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }

    try {
        couchbase::core::operations::management::view_index_upsert_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document.ns = ns;
        if (VALUE document_name = rb_hash_aref(document, rb_id2sym(rb_intern("name"))); !NIL_P(document_name)) {
            Check_Type(document_name, T_STRING);
            req.document.name = cb_string_new(document_name);
        }
        if (VALUE views = rb_hash_aref(document, rb_id2sym(rb_intern("views"))); !NIL_P(views)) {
            Check_Type(views, T_ARRAY);
            auto entries_num = static_cast<std::size_t>(RARRAY_LEN(views));
            for (std::size_t i = 0; i < entries_num; ++i) {
                VALUE entry = rb_ary_entry(views, static_cast<long>(i));
                Check_Type(entry, T_HASH);
                couchbase::core::management::views::design_document::view view;
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
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::management::view_index_upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::core::operations::management::view_index_upsert_response&& resp) {
            barrier->set_value(std::move(resp));
        });

        if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
            cb_throw_error_code(
              resp.ctx,
              fmt::format(
                R"(unable to store design document "{}" ({}) on bucket "{}")", req.document.name, req.document.ns, req.bucket_name));
        }
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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

    couchbase::core::design_document_namespace ns{};
    if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
        ns = couchbase::core::design_document_namespace::development;
    } else if (type == rb_intern("production")) {
        ns = couchbase::core::design_document_namespace::production;
    } else {
        rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
        return Qnil;
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        couchbase::core::operations::document_view_request req{};
        req.bucket_name = cb_string_new(bucket_name);
        req.document_name = cb_string_new(design_document_name);
        req.view_name = cb_string_new(view_name);
        req.ns = ns;
        cb_extract_timeout(req, options);
        if (!NIL_P(options)) {
            cb_extract_option_bool(req.debug, options, "debug");
            cb_extract_option_uint64(req.limit, options, "limit");
            cb_extract_option_uint64(req.skip, options, "skip");
            if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency"))); !NIL_P(scan_consistency)) {
                cb_check_type(scan_consistency, T_SYMBOL);
                if (ID consistency = rb_sym2id(scan_consistency); consistency == rb_intern("request_plus")) {
                    req.consistency = couchbase::core::view_scan_consistency::request_plus;
                } else if (consistency == rb_intern("update_after")) {
                    req.consistency = couchbase::core::view_scan_consistency::update_after;
                } else if (consistency == rb_intern("not_bounded")) {
                    req.consistency = couchbase::core::view_scan_consistency::not_bounded;
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
                    req.order = couchbase::core::view_sort_order::ascending;
                } else if (order == rb_intern("descending")) {
                    req.order = couchbase::core::view_sort_order::descending;
                }
            }
            if (VALUE keys = rb_hash_aref(options, rb_id2sym(rb_intern("keys"))); !NIL_P(keys)) {
                cb_check_type(keys, T_ARRAY);
                auto entries_num = static_cast<std::size_t>(RARRAY_LEN(keys));
                req.keys.reserve(entries_num);
                for (std::size_t i = 0; i < entries_num; ++i) {
                    VALUE entry = rb_ary_entry(keys, static_cast<long>(i));
                    cb_check_type(entry, T_STRING);
                    req.keys.emplace_back(cb_string_new(entry));
                }
            }
        }

        auto barrier = std::make_shared<std::promise<couchbase::core::operations::document_view_response>>();
        auto f = barrier->get_future();
        cluster->execute(req,
                         [barrier](couchbase::core::operations::document_view_response&& resp) { barrier->set_value(std::move(resp)); });
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
        if (resp.meta.total_rows) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("total_rows")), ULL2NUM(*resp.meta.total_rows));
        }
        if (resp.meta.debug_info) {
            rb_hash_aset(meta, rb_id2sym(rb_intern("debug_info")), cb_str_new(resp.meta.debug_info.value()));
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
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

static VALUE
cb_Backend_set_log_level(VALUE /* self */, VALUE log_level)
{
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
cb_Backend_get_log_level(VALUE /* self */)
{
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
cb_Backend_install_logger_shim(VALUE self, VALUE logger, VALUE log_level)
{
    couchbase::core::logger::reset();
    rb_iv_set(self, "@__logger_shim", logger);
    if (NIL_P(logger)) {
        return Qnil;
    }
    Check_Type(log_level, T_SYMBOL);
    couchbase::core::logger::level level{ couchbase::core::logger::level::off };
    if (ID type = rb_sym2id(log_level); type == rb_intern("trace")) {
        level = couchbase::core::logger::level::trace;
    } else if (type == rb_intern("debug")) {
        level = couchbase::core::logger::level::debug;
    } else if (type == rb_intern("info")) {
        level = couchbase::core::logger::level::info;
    } else if (type == rb_intern("warn")) {
        level = couchbase::core::logger::level::warn;
    } else if (type == rb_intern("error")) {
        level = couchbase::core::logger::level::err;
    } else if (type == rb_intern("critical")) {
        level = couchbase::core::logger::level::critical;
    } else {
        rb_iv_set(self, "__logger_shim", Qnil);
        return Qnil;
    }

    auto sink = std::make_shared<ruby_logger_sink<std::mutex>>(logger);
    couchbase::core::logger::configuration configuration;
    configuration.console = false;
    configuration.log_level = level;
    configuration.sink = sink;
    couchbase::core::logger::create_file_logger(configuration);
    cb_global_sink = sink;
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
    couchbase::core::utils::unsigned_leb128<std::uint64_t> encoded(NUM2ULL(number));
    return cb_str_new(encoded.data(), encoded.size());
}

static VALUE
cb_Backend_leb128_decode(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto buf = cb_binary_new(data);
    if (buf.empty()) {
        rb_raise(rb_eArgError, "Unable to decode the buffer as LEB128: the buffer is empty");
    }

    auto [value, rest] = couchbase::core::utils::decode_unsigned_leb128<std::uint64_t>(buf, couchbase::core::utils::leb_128_no_throw());
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
    auto encoded = couchbase::core::utils::string_codec::v2::query_escape(cb_string_new(data));
    return cb_str_new(encoded);
}

static VALUE
cb_Backend_path_escape(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto encoded = couchbase::core::utils::string_codec::v2::path_escape(cb_string_new(data));
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
    auto encoded = couchbase::core::utils::string_codec::v2::form_encode(values);
    return cb_str_new(encoded);
}

static VALUE
cb_Backend_enable_protocol_logger_to_save_network_traffic_to_file(VALUE /* self */, VALUE path)
{
    Check_Type(path, T_STRING);
    couchbase::core::logger::configuration configuration{};
    configuration.filename = cb_string_new(path);
    couchbase::core::logger::create_protocol_logger(configuration);
    return Qnil;
}

static void
init_backend(VALUE mCouchbase)
{
    VALUE cBackend = rb_define_class_under(mCouchbase, "Backend", rb_cObject);
    rb_define_alloc_func(cBackend, cb_Backend_allocate);
    rb_define_method(cBackend, "open", VALUE_FUNC(cb_Backend_open), 3);
    rb_define_method(cBackend, "close", VALUE_FUNC(cb_Backend_close), 0);
    rb_define_method(cBackend, "open_bucket", VALUE_FUNC(cb_Backend_open_bucket), 2);
    rb_define_method(cBackend, "diagnostics", VALUE_FUNC(cb_Backend_diagnostics), 1);
    rb_define_method(cBackend, "ping", VALUE_FUNC(cb_Backend_ping), 2);

    rb_define_method(cBackend, "document_get", VALUE_FUNC(cb_Backend_document_get), 5);
    rb_define_method(cBackend, "document_get_any_replica", VALUE_FUNC(cb_Backend_document_get_any_replica), 5);
    rb_define_method(cBackend, "document_get_all_replicas", VALUE_FUNC(cb_Backend_document_get_all_replicas), 5);
    rb_define_method(cBackend, "document_get_multi", VALUE_FUNC(cb_Backend_document_get_multi), 2);
    rb_define_method(cBackend, "document_get_projected", VALUE_FUNC(cb_Backend_document_get_projected), 5);
    rb_define_method(cBackend, "document_get_and_lock", VALUE_FUNC(cb_Backend_document_get_and_lock), 6);
    rb_define_method(cBackend, "document_get_and_touch", VALUE_FUNC(cb_Backend_document_get_and_touch), 6);
    rb_define_method(cBackend, "document_insert", VALUE_FUNC(cb_Backend_document_insert), 7);
    rb_define_method(cBackend, "document_replace", VALUE_FUNC(cb_Backend_document_replace), 7);
    rb_define_method(cBackend, "document_upsert", VALUE_FUNC(cb_Backend_document_upsert), 7);
    rb_define_method(cBackend, "document_upsert_multi", VALUE_FUNC(cb_Backend_document_upsert_multi), 5);
    rb_define_method(cBackend, "document_append", VALUE_FUNC(cb_Backend_document_append), 6);
    rb_define_method(cBackend, "document_prepend", VALUE_FUNC(cb_Backend_document_prepend), 6);
    rb_define_method(cBackend, "document_remove", VALUE_FUNC(cb_Backend_document_remove), 5);
    rb_define_method(cBackend, "document_remove_multi", VALUE_FUNC(cb_Backend_document_remove_multi), 5);
    rb_define_method(cBackend, "document_lookup_in", VALUE_FUNC(cb_Backend_document_lookup_in), 6);
    rb_define_method(cBackend, "document_lookup_in_any_replica", VALUE_FUNC(cb_Backend_document_lookup_in_any_replica), 6);
    rb_define_method(cBackend, "document_lookup_in_all_replicas", VALUE_FUNC(cb_Backend_document_lookup_in_all_replicas), 6);
    rb_define_method(cBackend, "document_mutate_in", VALUE_FUNC(cb_Backend_document_mutate_in), 6);
    rb_define_method(cBackend, "document_scan_create", VALUE_FUNC(cb_Backend_document_scan_create), 5);
    rb_define_method(cBackend, "document_query", VALUE_FUNC(cb_Backend_document_query), 2);
    rb_define_method(cBackend, "document_touch", VALUE_FUNC(cb_Backend_document_touch), 6);
    rb_define_method(cBackend, "document_exists", VALUE_FUNC(cb_Backend_document_exists), 5);
    rb_define_method(cBackend, "document_unlock", VALUE_FUNC(cb_Backend_document_unlock), 6);
    rb_define_method(cBackend, "document_increment", VALUE_FUNC(cb_Backend_document_increment), 5);
    rb_define_method(cBackend, "document_decrement", VALUE_FUNC(cb_Backend_document_decrement), 5);
    rb_define_method(cBackend, "document_search", VALUE_FUNC(cb_Backend_document_search), 6);
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

    rb_define_method(cBackend, "change_password", VALUE_FUNC(cb_Backend_change_password), 2);

    rb_define_method(cBackend, "cluster_enable_developer_preview!", VALUE_FUNC(cb_Backend_cluster_enable_developer_preview), 0);

    rb_define_method(cBackend, "scope_get_all", VALUE_FUNC(cb_Backend_scope_get_all), 2);
    rb_define_method(cBackend, "scope_create", VALUE_FUNC(cb_Backend_scope_create), 3);
    rb_define_method(cBackend, "scope_drop", VALUE_FUNC(cb_Backend_scope_drop), 3);
    rb_define_method(cBackend, "collection_create", VALUE_FUNC(cb_Backend_collection_create), 5);
    rb_define_method(cBackend, "collection_update", VALUE_FUNC(cb_Backend_collection_update), 5);
    rb_define_method(cBackend, "collection_drop", VALUE_FUNC(cb_Backend_collection_drop), 4);

    rb_define_method(cBackend, "query_index_get_all", VALUE_FUNC(cb_Backend_query_index_get_all), 2);
    rb_define_method(cBackend, "query_index_create", VALUE_FUNC(cb_Backend_query_index_create), 4);
    rb_define_method(cBackend, "query_index_create_primary", VALUE_FUNC(cb_Backend_query_index_create_primary), 2);
    rb_define_method(cBackend, "query_index_drop", VALUE_FUNC(cb_Backend_query_index_drop), 3);
    rb_define_method(cBackend, "query_index_drop_primary", VALUE_FUNC(cb_Backend_query_index_drop_primary), 2);
    rb_define_method(cBackend, "query_index_build_deferred", VALUE_FUNC(cb_Backend_query_index_build_deferred), 2);

    rb_define_method(cBackend, "collection_query_index_get_all", VALUE_FUNC(cb_Backend_collection_query_index_get_all), 4);
    rb_define_method(cBackend, "collection_query_index_create", VALUE_FUNC(cb_Backend_collection_query_index_create), 6);
    rb_define_method(cBackend, "collection_query_index_create_primary", VALUE_FUNC(cb_Backend_collection_query_index_create_primary), 4);
    rb_define_method(cBackend, "collection_query_index_drop", VALUE_FUNC(cb_Backend_collection_query_index_drop), 5);
    rb_define_method(cBackend, "collection_query_index_drop_primary", VALUE_FUNC(cb_Backend_collection_query_index_drop_primary), 4);
    rb_define_method(cBackend, "collection_query_index_build_deferred", VALUE_FUNC(cb_Backend_collection_query_index_build_deferred), 4);

    rb_define_method(cBackend, "search_get_stats", VALUE_FUNC(cb_Backend_search_get_stats), 1);
    rb_define_method(cBackend, "search_index_get_all", VALUE_FUNC(cb_Backend_search_index_get_all), 3);
    rb_define_method(cBackend, "search_index_get", VALUE_FUNC(cb_Backend_search_index_get), 4);
    rb_define_method(cBackend, "search_index_upsert", VALUE_FUNC(cb_Backend_search_index_upsert), 4);
    rb_define_method(cBackend, "search_index_drop", VALUE_FUNC(cb_Backend_search_index_drop), 4);
    rb_define_method(cBackend, "search_index_get_stats", VALUE_FUNC(cb_Backend_search_index_get_stats), 2);
    rb_define_method(cBackend, "search_index_get_documents_count", VALUE_FUNC(cb_Backend_search_index_get_documents_count), 4);
    rb_define_method(cBackend, "search_index_pause_ingest", VALUE_FUNC(cb_Backend_search_index_pause_ingest), 4);
    rb_define_method(cBackend, "search_index_resume_ingest", VALUE_FUNC(cb_Backend_search_index_resume_ingest), 4);
    rb_define_method(cBackend, "search_index_allow_querying", VALUE_FUNC(cb_Backend_search_index_allow_querying), 4);
    rb_define_method(cBackend, "search_index_disallow_querying", VALUE_FUNC(cb_Backend_search_index_disallow_querying), 4);
    rb_define_method(cBackend, "search_index_freeze_plan", VALUE_FUNC(cb_Backend_search_index_freeze_plan), 4);
    rb_define_method(cBackend, "search_index_unfreeze_plan", VALUE_FUNC(cb_Backend_search_index_unfreeze_plan), 4);
    rb_define_method(cBackend, "search_index_analyze_document", VALUE_FUNC(cb_Backend_search_index_analyze_document), 5);

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
    rb_define_singleton_method(cBackend, "install_logger_shim", VALUE_FUNC(cb_Backend_install_logger_shim), 2);
    rb_define_singleton_method(cBackend, "snappy_compress", VALUE_FUNC(cb_Backend_snappy_compress), 1);
    rb_define_singleton_method(cBackend, "snappy_uncompress", VALUE_FUNC(cb_Backend_snappy_uncompress), 1);
    rb_define_singleton_method(cBackend, "leb128_encode", VALUE_FUNC(cb_Backend_leb128_encode), 1);
    rb_define_singleton_method(cBackend, "leb128_decode", VALUE_FUNC(cb_Backend_leb128_decode), 1);
    rb_define_singleton_method(cBackend, "query_escape", VALUE_FUNC(cb_Backend_query_escape), 1);
    rb_define_singleton_method(cBackend, "path_escape", VALUE_FUNC(cb_Backend_path_escape), 1);
    rb_define_singleton_method(cBackend, "form_encode", VALUE_FUNC(cb_Backend_form_encode), 1);
    rb_define_singleton_method(cBackend,
                               "enable_protocol_logger_to_save_network_traffic_to_file",
                               VALUE_FUNC(cb_Backend_enable_protocol_logger_to_save_network_traffic_to_file),
                               1);
}

void
init_logger()
{
    if (auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_DONT_INSTALL_TERMINATE_HANDLER"); env_val.empty()) {
        couchbase::core::platform::install_backtrace_terminate_handler();
    }
    if (auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_DONT_USE_BUILTIN_LOGGER"); env_val.empty()) {
        couchbase::core::logger::create_console_logger();
        if (env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_LOG_LEVEL"); !env_val.empty()) {
            couchbase::core::logger::set_log_levels(couchbase::core::logger::level_from_str(env_val));
        }
    }
}

extern "C" {
#if defined(_WIN32)
__declspec(dllexport)
#endif
  void Init_libcouchbase(void)
{
    init_logger();

    VALUE mCouchbase = rb_define_module("Couchbase");
    init_versions(mCouchbase);
    init_backend(mCouchbase);
    init_exceptions(mCouchbase);
}
}
