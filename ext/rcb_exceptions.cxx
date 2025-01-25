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

#include <couchbase/error.hxx>
#include <couchbase/error_codes.hxx>

#include <core/error_context/analytics.hxx>
#include <core/error_context/http.hxx>
#include <core/error_context/key_value_error_context.hxx>
#include <core/error_context/query.hxx>
#include <core/error_context/search.hxx>
#include <core/error_context/view.hxx>
#include <core/fmt/key_value_status_code.hxx>

#include <spdlog/fmt/bundled/core.h>

#include <couchbase/fmt/retry_reason.hxx>

#include "rcb_exceptions.hxx"
#include "rcb_utils.hxx"
#include "ruby/internal/arithmetic/long.h"

#include <ruby.h>

namespace couchbase::ruby
{
namespace
{

VALUE eClusterClosed;

VALUE eCouchbaseError;

VALUE eAmbiguousTimeout;
VALUE eAuthenticationFailure;
VALUE eBucketExists;
VALUE eBucketNotFlushable;
VALUE eBucketNotFound;
VALUE eCasMismatch;
VALUE eCollectionExists;
VALUE eCollectionNotFound;
VALUE eCompilationFailure;
VALUE eConsistencyMismatch;
VALUE eDatasetExists;
VALUE eDatasetNotFound;
VALUE eDataverseExists;
VALUE eDataverseNotFound;
VALUE eDecodingFailure;
VALUE eDeltaInvalid;
VALUE eDesignDocumentNotFound;
VALUE eDocumentExists;
VALUE eDocumentIrretrievable;
VALUE eDocumentLocked;
VALUE eDocumentNotFound;
VALUE eDocumentNotLocked;
VALUE eDocumentNotJson;
VALUE eDurabilityAmbiguous;
VALUE eDurabilityImpossible;
VALUE eDurabilityLevelNotAvailable;
VALUE eDurableWriteInProgress;
VALUE eDurableWriteReCommitInProgress;
VALUE eEncodingFailure;
VALUE eFeatureNotAvailable;
VALUE eGroupNotFound;
VALUE eIndexExists;
VALUE eIndexFailure;
VALUE eIndexNotFound;
VALUE eIndexNotReady;
VALUE eInternalServerFailure;
VALUE eInvalidArgument;
VALUE eJobQueueFull;
VALUE eLinkNotFound;
VALUE eLinkExists;
VALUE eMutationTokenOutdated;
VALUE eNumberTooBig;
VALUE eParsingFailure;
VALUE ePathExists;
VALUE ePathInvalid;
VALUE ePathMismatch;
VALUE ePathNotFound;
VALUE ePathTooBig;
VALUE ePathTooDeep;
VALUE ePlanningFailure;
VALUE ePreparedStatementFailure;
VALUE eRequestCanceled;
VALUE eScopeExists;
VALUE eScopeNotFound;
VALUE eServiceNotAvailable;
VALUE eTemporaryFailure;
VALUE eUnambiguousTimeout;
VALUE eUnsupportedOperation;
VALUE eUserNotFound;
VALUE eUserExists;
VALUE eValueInvalid;
VALUE eValueTooDeep;
VALUE eValueTooLarge;
VALUE eViewNotFound;
VALUE eXattrCannotModifyVirtualAttribute;
VALUE eXattrInvalidKeyCombo;
VALUE eXattrUnknownMacro;
VALUE eXattrUnknownVirtualAttribute;
VALUE eRateLimited;
VALUE eQuotaLimited;
VALUE eXattrNoAccess;
VALUE eCannotReviveLivingDocument;
VALUE eDmlFailure;
VALUE eEventingFunctionCompilationFailure;
VALUE eEventingFunctionDeployed;
VALUE eEventingFunctionIdentialKeyspace;
VALUE eEventingFunctionNotBootstrapped;
VALUE eEventingFunctionNotDeployed;
VALUE eEventingFunctionNotFound;
VALUE eEventingFunctionPaused;

VALUE eBackendError;
VALUE eNetworkError;
VALUE eResolveFailure;
VALUE eNoEndpointsLeft;
VALUE eHandshakeFailure;
VALUE eProtocolError;
VALUE eConfigurationNotAvailable;
VALUE eEndOfStream;
VALUE eNeedMoreData;
VALUE eOperationQueueClosed;
VALUE eOperationQueueFull;
VALUE eRequestAlreadyQueued;
VALUE eNetworkRequestCanceled;
VALUE eBucketClosed;

} // namespace

ruby_exception::ruby_exception(VALUE exc)
  : std::runtime_error("ruby_exception")
  , exc_{ exc }
{
}

ruby_exception::ruby_exception(VALUE exc_type, VALUE exc_message)
  : std::runtime_error("ruby_exception")
  , exc_{ rb_exc_new_str(exc_type, exc_message) }
{
}

ruby_exception::ruby_exception(VALUE exc_type, const std::string& exc_message)
  : std::runtime_error("ruby_exception")
  , exc_{ rb_exc_new_cstr(exc_type, exc_message.c_str()) }
{
}

VALUE
ruby_exception::exception_object() const
{
  return exc_;
}

void
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
  eDesignDocumentNotFound =
    rb_define_class_under(mError, "DesignDocumentNotFound", eCouchbaseError);
  eDocumentExists = rb_define_class_under(mError, "DocumentExists", eCouchbaseError);
  eDocumentIrretrievable = rb_define_class_under(mError, "DocumentIrretrievable", eCouchbaseError);
  eDocumentLocked = rb_define_class_under(mError, "DocumentLocked", eCouchbaseError);
  eDocumentNotFound = rb_define_class_under(mError, "DocumentNotFound", eCouchbaseError);
  eDocumentNotLocked = rb_define_class_under(mError, "DocumentNotLocked", eCouchbaseError);
  eDocumentNotJson = rb_define_class_under(mError, "DocumentNotJson", eCouchbaseError);
  eDurabilityAmbiguous = rb_define_class_under(mError, "DurabilityAmbiguous", eCouchbaseError);
  eDurabilityImpossible = rb_define_class_under(mError, "DurabilityImpossible", eCouchbaseError);
  eDurabilityLevelNotAvailable =
    rb_define_class_under(mError, "DurabilityLevelNotAvailable", eCouchbaseError);
  eDurableWriteInProgress =
    rb_define_class_under(mError, "DurableWriteInProgress", eCouchbaseError);
  eDurableWriteReCommitInProgress =
    rb_define_class_under(mError, "DurableWriteReCommitInProgress", eCouchbaseError);
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
  ePreparedStatementFailure =
    rb_define_class_under(mError, "PreparedStatementFailure", eCouchbaseError);
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
  eXattrCannotModifyVirtualAttribute =
    rb_define_class_under(mError, "XattrCannotModifyVirtualAttribute", eCouchbaseError);
  eXattrInvalidKeyCombo = rb_define_class_under(mError, "XattrInvalidKeyCombo", eCouchbaseError);
  eXattrUnknownMacro = rb_define_class_under(mError, "XattrUnknownMacro", eCouchbaseError);
  eXattrUnknownVirtualAttribute =
    rb_define_class_under(mError, "XattrUnknownVirtualAttribute", eCouchbaseError);
  eRateLimited = rb_define_class_under(mError, "RateLimited", eCouchbaseError);
  eQuotaLimited = rb_define_class_under(mError, "QuotaLimited", eCouchbaseError);
  eXattrNoAccess = rb_define_class_under(mError, "XattrNoAccess", eCouchbaseError);
  eCannotReviveLivingDocument =
    rb_define_class_under(mError, "CannotReviveLivingDocument", eCouchbaseError);
  eDmlFailure = rb_define_class_under(mError, "DmlFailure", eCouchbaseError);
  eEventingFunctionCompilationFailure =
    rb_define_class_under(mError, "EventingFunctionCompilationFailure", eCouchbaseError);
  eEventingFunctionDeployed =
    rb_define_class_under(mError, "EventingFunctionDeployed", eCouchbaseError);
  eEventingFunctionIdentialKeyspace =
    rb_define_class_under(mError, "EventingFunctionIdentialKeyspace", eCouchbaseError);
  eEventingFunctionNotBootstrapped =
    rb_define_class_under(mError, "EventingFunctionNotBootstrapped", eCouchbaseError);
  eEventingFunctionNotDeployed =
    rb_define_class_under(mError, "EventingFunctionNotDeployed", eCouchbaseError);
  eEventingFunctionNotFound =
    rb_define_class_under(mError, "EventingFunctionNotFound", eCouchbaseError);
  eEventingFunctionPaused =
    rb_define_class_under(mError, "EventingFunctionPaused", eCouchbaseError);

  eBackendError = rb_define_class_under(mError, "BackendError", eCouchbaseError);
  eNetworkError = rb_define_class_under(mError, "NetworkError", eBackendError);
  eResolveFailure = rb_define_class_under(mError, "ResolveFailure", eNetworkError);
  eNoEndpointsLeft = rb_define_class_under(mError, "NoEndpointsLeft", eNetworkError);
  eHandshakeFailure = rb_define_class_under(mError, "HandshakeFailure", eNetworkError);
  eProtocolError = rb_define_class_under(mError, "ProtocolError", eNetworkError);
  eConfigurationNotAvailable =
    rb_define_class_under(mError, "ConfigurationNotAvailable", eNetworkError);
  eClusterClosed = rb_define_class_under(mError, "ClusterClosed", eCouchbaseError);
  eEndOfStream = rb_define_class_under(mError, "EndOfStream", eCouchbaseError);
  eNeedMoreData = rb_define_class_under(mError, "NeedMoreData", eCouchbaseError);
  eOperationQueueClosed = rb_define_class_under(mError, "OperationQueueClosed", eCouchbaseError);
  eOperationQueueFull = rb_define_class_under(mError, "OperationQueueFull", eCouchbaseError);
  eRequestAlreadyQueued = rb_define_class_under(mError, "RequestAlreadyQueued", eCouchbaseError);
  eNetworkRequestCanceled =
    rb_define_class_under(mError, "NetworkRequestCanceled", eCouchbaseError);
  eBucketClosed = rb_define_class_under(mError, "BucketClosed", eCouchbaseError);
}

[[nodiscard]] auto
cb_map_error_code(std::error_code ec, const std::string& message, bool include_error_code) -> VALUE
{
  std::string what = message;
  if (include_error_code) {
    what += fmt::format(": {}", ec.message());
  }

  if (ec.category() == core::impl::common_category()) {
    switch (static_cast<errc::common>(ec.value())) {
      case errc::common::unambiguous_timeout:
        return rb_exc_new_cstr(eUnambiguousTimeout, what.c_str());

      case errc::common::ambiguous_timeout:
        return rb_exc_new_cstr(eAmbiguousTimeout, what.c_str());

      case errc::common::request_canceled:
        return rb_exc_new_cstr(eRequestCanceled, what.c_str());

      case errc::common::invalid_argument:
        return rb_exc_new_cstr(eInvalidArgument, what.c_str());

      case errc::common::service_not_available:
        return rb_exc_new_cstr(eServiceNotAvailable, what.c_str());

      case errc::common::internal_server_failure:
        return rb_exc_new_cstr(eInternalServerFailure, what.c_str());

      case errc::common::authentication_failure:
        return rb_exc_new_cstr(eAuthenticationFailure, what.c_str());

      case errc::common::temporary_failure:
        return rb_exc_new_cstr(eTemporaryFailure, what.c_str());

      case errc::common::parsing_failure:
        return rb_exc_new_cstr(eParsingFailure, what.c_str());

      case errc::common::cas_mismatch:
        return rb_exc_new_cstr(eCasMismatch, what.c_str());

      case errc::common::bucket_not_found:
        return rb_exc_new_cstr(eBucketNotFound, what.c_str());

      case errc::common::scope_not_found:
        return rb_exc_new_cstr(eScopeNotFound, what.c_str());

      case errc::common::collection_not_found:
        return rb_exc_new_cstr(eCollectionNotFound, what.c_str());

      case errc::common::unsupported_operation:
        return rb_exc_new_cstr(eUnsupportedOperation, what.c_str());

      case errc::common::feature_not_available:
        return rb_exc_new_cstr(eFeatureNotAvailable, what.c_str());

      case errc::common::encoding_failure:
        return rb_exc_new_cstr(eEncodingFailure, what.c_str());

      case errc::common::decoding_failure:
        return rb_exc_new_cstr(eDecodingFailure, what.c_str());

      case errc::common::index_not_found:
        return rb_exc_new_cstr(eIndexNotFound, what.c_str());

      case errc::common::index_exists:
        return rb_exc_new_cstr(eIndexExists, what.c_str());

      case errc::common::rate_limited:
        return rb_exc_new_cstr(eRateLimited, what.c_str());

      case errc::common::quota_limited:
        return rb_exc_new_cstr(eQuotaLimited, what.c_str());
    }
  } else if (ec.category() == core::impl::key_value_category()) {
    switch (static_cast<errc::key_value>(ec.value())) {
      case errc::key_value::document_not_found:
        return rb_exc_new_cstr(eDocumentNotFound, what.c_str());

      case errc::key_value::document_irretrievable:
        return rb_exc_new_cstr(eDocumentIrretrievable, what.c_str());

      case errc::key_value::document_locked:
        return rb_exc_new_cstr(eDocumentLocked, what.c_str());

      case errc::key_value::document_not_locked:
        return rb_exc_new_cstr(eDocumentNotLocked, what.c_str());

      case errc::key_value::value_too_large:
        return rb_exc_new_cstr(eValueTooLarge, what.c_str());

      case errc::key_value::document_exists:
        return rb_exc_new_cstr(eDocumentExists, what.c_str());

      case errc::key_value::durability_level_not_available:
        return rb_exc_new_cstr(eDurabilityLevelNotAvailable, what.c_str());

      case errc::key_value::durability_impossible:
        return rb_exc_new_cstr(eDurabilityImpossible, what.c_str());

      case errc::key_value::durability_ambiguous:
        return rb_exc_new_cstr(eDurabilityAmbiguous, what.c_str());

      case errc::key_value::durable_write_in_progress:
        return rb_exc_new_cstr(eDurableWriteInProgress, what.c_str());

      case errc::key_value::durable_write_re_commit_in_progress:
        return rb_exc_new_cstr(eDurableWriteReCommitInProgress, what.c_str());

      case errc::key_value::mutation_token_outdated:
        return rb_exc_new_cstr(eMutationTokenOutdated, what.c_str());

      case errc::key_value::path_not_found:
        return rb_exc_new_cstr(ePathNotFound, what.c_str());

      case errc::key_value::path_mismatch:
        return rb_exc_new_cstr(ePathMismatch, what.c_str());

      case errc::key_value::path_invalid:
        return rb_exc_new_cstr(ePathInvalid, what.c_str());

      case errc::key_value::path_too_big:
        return rb_exc_new_cstr(ePathTooBig, what.c_str());

      case errc::key_value::path_too_deep:
        return rb_exc_new_cstr(ePathTooDeep, what.c_str());

      case errc::key_value::value_too_deep:
        return rb_exc_new_cstr(eValueTooDeep, what.c_str());

      case errc::key_value::value_invalid:
        return rb_exc_new_cstr(eValueInvalid, what.c_str());

      case errc::key_value::document_not_json:
        return rb_exc_new_cstr(eDocumentNotJson, what.c_str());

      case errc::key_value::number_too_big:
        return rb_exc_new_cstr(eNumberTooBig, what.c_str());

      case errc::key_value::delta_invalid:
        return rb_exc_new_cstr(eDeltaInvalid, what.c_str());

      case errc::key_value::path_exists:
        return rb_exc_new_cstr(ePathExists, what.c_str());

      case errc::key_value::xattr_unknown_macro:
        return rb_exc_new_cstr(eXattrUnknownMacro, what.c_str());

      case errc::key_value::xattr_invalid_key_combo:
        return rb_exc_new_cstr(eXattrInvalidKeyCombo, what.c_str());

      case errc::key_value::xattr_unknown_virtual_attribute:
        return rb_exc_new_cstr(eXattrUnknownVirtualAttribute, what.c_str());

      case errc::key_value::xattr_cannot_modify_virtual_attribute:
        return rb_exc_new_cstr(eXattrCannotModifyVirtualAttribute, what.c_str());

      case errc::key_value::xattr_no_access:
        return rb_exc_new_cstr(eXattrNoAccess, what.c_str());

      case errc::key_value::cannot_revive_living_document:
        return rb_exc_new_cstr(eCannotReviveLivingDocument, what.c_str());

      case errc::key_value::range_scan_completed:
        // Should not be exposed to the Ruby SDK, map it to a BackendError
        return rb_exc_new_cstr(eBackendError, what.c_str());
    }
  } else if (ec.category() == core::impl::query_category()) {
    switch (static_cast<errc::query>(ec.value())) {
      case errc::query::planning_failure:
        return rb_exc_new_cstr(ePlanningFailure, what.c_str());

      case errc::query::index_failure:
        return rb_exc_new_cstr(eIndexFailure, what.c_str());

      case errc::query::prepared_statement_failure:
        return rb_exc_new_cstr(ePreparedStatementFailure, what.c_str());

      case errc::query::dml_failure:
        return rb_exc_new_cstr(eDmlFailure, what.c_str());
    }
  } else if (ec.category() == core::impl::search_category()) {
    switch (static_cast<errc::search>(ec.value())) {
      case errc::search::index_not_ready:
        return rb_exc_new_cstr(eIndexNotReady, what.c_str());
      case errc::search::consistency_mismatch:
        return rb_exc_new_cstr(eConsistencyMismatch, what.c_str());
    }
  } else if (ec.category() == core::impl::view_category()) {
    switch (static_cast<errc::view>(ec.value())) {
      case errc::view::view_not_found:
        return rb_exc_new_cstr(eViewNotFound, what.c_str());

      case errc::view::design_document_not_found:
        return rb_exc_new_cstr(eDesignDocumentNotFound, what.c_str());
    }
  } else if (ec.category() == core::impl::analytics_category()) {
    switch (static_cast<errc::analytics>(ec.value())) {
      case errc::analytics::compilation_failure:
        return rb_exc_new_cstr(eCompilationFailure, what.c_str());

      case errc::analytics::job_queue_full:
        return rb_exc_new_cstr(eJobQueueFull, what.c_str());

      case errc::analytics::dataset_not_found:
        return rb_exc_new_cstr(eDatasetNotFound, what.c_str());

      case errc::analytics::dataverse_not_found:
        return rb_exc_new_cstr(eDataverseNotFound, what.c_str());

      case errc::analytics::dataset_exists:
        return rb_exc_new_cstr(eDatasetExists, what.c_str());

      case errc::analytics::dataverse_exists:
        return rb_exc_new_cstr(eDataverseExists, what.c_str());

      case errc::analytics::link_not_found:
        return rb_exc_new_cstr(eLinkNotFound, what.c_str());

      case errc::analytics::link_exists:
        return rb_exc_new_cstr(eLinkExists, what.c_str());
    }
  } else if (ec.category() == core::impl::management_category()) {
    switch (static_cast<errc::management>(ec.value())) {
      case errc::management::collection_exists:
        return rb_exc_new_cstr(eCollectionExists, what.c_str());

      case errc::management::scope_exists:
        return rb_exc_new_cstr(eScopeExists, what.c_str());

      case errc::management::user_not_found:
        return rb_exc_new_cstr(eUserNotFound, what.c_str());

      case errc::management::group_not_found:
        return rb_exc_new_cstr(eGroupNotFound, what.c_str());

      case errc::management::user_exists:
        return rb_exc_new_cstr(eUserExists, what.c_str());

      case errc::management::bucket_exists:
        return rb_exc_new_cstr(eBucketExists, what.c_str());

      case errc::management::bucket_not_flushable:
        return rb_exc_new_cstr(eBucketNotFlushable, what.c_str());

      case errc::management::eventing_function_not_found:
        return rb_exc_new_cstr(eEventingFunctionNotFound, what.c_str());

      case errc::management::eventing_function_not_deployed:
        return rb_exc_new_cstr(eEventingFunctionNotDeployed, what.c_str());

      case errc::management::eventing_function_compilation_failure:
        return rb_exc_new_cstr(eEventingFunctionCompilationFailure, what.c_str());

      case errc::management::eventing_function_identical_keyspace:
        return rb_exc_new_cstr(eEventingFunctionIdentialKeyspace, what.c_str());

      case errc::management::eventing_function_not_bootstrapped:
        return rb_exc_new_cstr(eEventingFunctionNotBootstrapped, what.c_str());

      case errc::management::eventing_function_deployed:
        return rb_exc_new_cstr(eEventingFunctionDeployed, what.c_str());

      case errc::management::eventing_function_paused:
        return rb_exc_new_cstr(eEventingFunctionPaused, what.c_str());
    }
  } else if (ec.category() == core::impl::network_category()) {
    switch (static_cast<errc::network>(ec.value())) {
      case errc::network::resolve_failure:
        return rb_exc_new_cstr(eResolveFailure, what.c_str());

      case errc::network::no_endpoints_left:
        return rb_exc_new_cstr(eNoEndpointsLeft, what.c_str());

      case errc::network::handshake_failure:
        return rb_exc_new_cstr(eHandshakeFailure, what.c_str());

      case errc::network::protocol_error:
        return rb_exc_new_cstr(eProtocolError, what.c_str());

      case errc::network::configuration_not_available:
        return rb_exc_new_cstr(eConfigurationNotAvailable, what.c_str());

      case errc::network::cluster_closed:
        return rb_exc_new_cstr(eClusterClosed, what.c_str());

      case errc::network::end_of_stream:
        return rb_exc_new_cstr(eEndOfStream, what.c_str());

      case errc::network::need_more_data:
        return rb_exc_new_cstr(eNeedMoreData, what.c_str());

      case errc::network::operation_queue_closed:
        return rb_exc_new_cstr(eOperationQueueClosed, what.c_str());

      case errc::network::operation_queue_full:
        return rb_exc_new_cstr(eOperationQueueFull, what.c_str());

      case errc::network::request_already_queued:
        return rb_exc_new_cstr(eRequestAlreadyQueued, what.c_str());

      case errc::network::request_cancelled:
        return rb_exc_new_cstr(eNetworkRequestCanceled, what.c_str());

      case errc::network::bucket_closed:
        return rb_exc_new_cstr(eBucketClosed, what.c_str());
    }
  }

  return rb_exc_new_cstr(eBackendError, what.c_str());
}

[[noreturn]] void
cb_throw_error_code(std::error_code ec, const std::string& message)
{
  throw ruby_exception(cb_map_error_code(ec, message));
}

auto
exc_feature_not_available() -> VALUE
{
  return eFeatureNotAvailable;
}

auto
exc_couchbase_error() -> VALUE
{
  return eCouchbaseError;
}

auto
exc_cluster_closed() -> VALUE
{
  return eClusterClosed;
}

auto
exc_invalid_argument() -> VALUE
{
  return eInvalidArgument;
}

[[nodiscard]] VALUE
cb_map_error(const core::key_value_error_context& ctx, const std::string& message)
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
    rb_hash_aset(
      error_map_info, rb_id2sym(rb_intern("name")), cb_str_new(ctx.error_map_info()->name()));
    rb_hash_aset(error_map_info,
                 rb_id2sym(rb_intern("desc")),
                 cb_str_new(ctx.error_map_info()->description()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("error_map_info")), error_map_info);
  }
  if (ctx.extended_error_info()) {
    VALUE enhanced_error_info = rb_hash_new();
    rb_hash_aset(enhanced_error_info,
                 rb_id2sym(rb_intern("reference")),
                 cb_str_new(ctx.extended_error_info()->reference()));
    rb_hash_aset(enhanced_error_info,
                 rb_id2sym(rb_intern("context")),
                 cb_str_new(ctx.extended_error_info()->context()));
    rb_hash_aset(error_context, rb_id2sym(rb_intern("extended_error_info")), enhanced_error_info);
  }
  rb_hash_aset(
    error_context, rb_id2sym(rb_intern("retry_attempts")), ULONG2NUM(ctx.retry_attempts()));
  if (!ctx.retry_reasons().empty()) {
    VALUE retry_reasons = rb_ary_new_capa(static_cast<long>(ctx.retry_reasons().size()));
    for (const auto& reason : ctx.retry_reasons()) {
      auto reason_str = fmt::format("{}", reason);
      rb_ary_push(retry_reasons, rb_id2sym(rb_intern(reason_str.c_str())));
    }
    rb_hash_aset(error_context, rb_id2sym(rb_intern("retry_reasons")), retry_reasons);
  }
  if (ctx.last_dispatched_to()) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_to")),
                 cb_str_new(ctx.last_dispatched_to().value()));
  }
  if (ctx.last_dispatched_from()) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_from")),
                 cb_str_new(ctx.last_dispatched_from().value()));
  }
  rb_iv_set(exc, "@context", error_context);
  return exc;
}

namespace
{
[[nodiscard]] VALUE
cb_map_error(const core::error_context::query& ctx, const std::string& message)
{
  VALUE exc = cb_map_error_code(ctx.ec, message);
  VALUE error_context = rb_hash_new();
  std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
  rb_hash_aset(
    error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("statement")), cb_str_new(ctx.statement));
  if (ctx.parameters) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("parameters")), cb_str_new(ctx.parameters.value()));
  }
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
  if (ctx.retry_attempts > 0) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("retry_attempts")), ULONG2NUM(ctx.retry_attempts));
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
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_to")),
                 cb_str_new(ctx.last_dispatched_to.value()));
  }
  if (ctx.last_dispatched_from) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_from")),
                 cb_str_new(ctx.last_dispatched_from.value()));
  }
  rb_iv_set(exc, "@context", error_context);
  return exc;
}

[[nodiscard]] VALUE
cb_map_error(const core::error_context::analytics& ctx, const std::string& message)
{
  VALUE exc = cb_map_error_code(ctx.ec, message);
  VALUE error_context = rb_hash_new();
  std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
  rb_hash_aset(
    error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("statement")), cb_str_new(ctx.statement));
  if (ctx.parameters) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("parameters")), cb_str_new(ctx.parameters.value()));
  }
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
  if (ctx.retry_attempts > 0) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("retry_attempts")), ULONG2NUM(ctx.retry_attempts));
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
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_to")),
                 cb_str_new(ctx.last_dispatched_to.value()));
  }
  if (ctx.last_dispatched_from) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_from")),
                 cb_str_new(ctx.last_dispatched_from.value()));
  }
  rb_iv_set(exc, "@context", error_context);
  return exc;
}

[[nodiscard]] VALUE
cb_map_error(const core::error_context::view& ctx, const std::string& message)
{
  VALUE exc = cb_map_error_code(ctx.ec, message);
  VALUE error_context = rb_hash_new();
  std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
  rb_hash_aset(
    error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
  rb_hash_aset(error_context,
               rb_id2sym(rb_intern("design_document_name")),
               cb_str_new(ctx.design_document_name));
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
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("retry_attempts")), ULONG2NUM(ctx.retry_attempts));
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
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_to")),
                 cb_str_new(ctx.last_dispatched_to.value()));
  }
  if (ctx.last_dispatched_from) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_from")),
                 cb_str_new(ctx.last_dispatched_from.value()));
  }
  rb_iv_set(exc, "@context", error_context);
  return exc;
}

[[nodiscard]] VALUE
cb_map_error(const core::error_context::http& ctx, const std::string& message)
{
  VALUE exc = cb_map_error_code(ctx.ec, message);
  VALUE error_context = rb_hash_new();
  std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
  rb_hash_aset(
    error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("method")), cb_str_new(ctx.method));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("path")), cb_str_new(ctx.path));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
  if (ctx.retry_attempts > 0) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("retry_attempts")), ULONG2NUM(ctx.retry_attempts));
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
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_to")),
                 cb_str_new(ctx.last_dispatched_to.value()));
  }
  if (ctx.last_dispatched_from) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_from")),
                 cb_str_new(ctx.last_dispatched_from.value()));
  }
  rb_iv_set(exc, "@context", error_context);
  return exc;
}

[[nodiscard]] VALUE
cb_map_error(const core::error_context::search& ctx, const std::string& message)
{
  VALUE exc = cb_map_error_code(ctx.ec, message);
  VALUE error_context = rb_hash_new();
  std::string error(fmt::format("{}, {}", ctx.ec.value(), ctx.ec.message()));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("error")), cb_str_new(error));
  rb_hash_aset(
    error_context, rb_id2sym(rb_intern("client_context_id")), cb_str_new(ctx.client_context_id));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("index_name")), cb_str_new(ctx.index_name));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("query")), cb_str_new(ctx.query));
  if (ctx.parameters) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("parameters")), cb_str_new(ctx.parameters.value()));
  }
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_status")), INT2FIX(ctx.http_status));
  rb_hash_aset(error_context, rb_id2sym(rb_intern("http_body")), cb_str_new(ctx.http_body));
  if (ctx.retry_attempts > 0) {
    rb_hash_aset(
      error_context, rb_id2sym(rb_intern("retry_attempts")), ULONG2NUM(ctx.retry_attempts));
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
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_to")),
                 cb_str_new(ctx.last_dispatched_to.value()));
  }
  if (ctx.last_dispatched_from) {
    rb_hash_aset(error_context,
                 rb_id2sym(rb_intern("last_dispatched_from")),
                 cb_str_new(ctx.last_dispatched_from.value()));
  }
  rb_iv_set(exc, "@context", error_context);
  return exc;
}

} // namespace

[[nodiscard]] VALUE
cb_map_error(const error& err, const std::string& message)
{
  VALUE exc = cb_map_error_code(err.ec(), fmt::format("{}: {}", message, err.message()), true);
  static const auto id_context_eq = rb_intern("context=");
  rb_funcall(exc, id_context_eq, 1, cb_str_new(err.ctx().to_json()));
  if (auto cause = err.cause(); cause) {
    rb_iv_set(exc, "@cause", cb_map_error(cause.value(), "Caused by"));
  }
  return exc;
}

void
cb_throw_error(const core::error_context::search& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}

void
cb_throw_error(const core::error_context::http& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}

void
cb_throw_error(const core::error_context::view& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}

void
cb_throw_error(const core::error_context::analytics& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}

void
cb_throw_error(const core::error_context::query& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}

void
cb_throw_error(const core::key_value_error_context& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}

void
cb_throw_error(const error& ctx, const std::string& message)
{
  throw ruby_exception(cb_map_error(ctx, message));
}
} // namespace couchbase::ruby
