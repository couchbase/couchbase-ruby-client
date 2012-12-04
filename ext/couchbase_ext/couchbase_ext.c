/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2011, 2012 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "couchbase_ext.h"

/* Classes */
VALUE cb_cBucket;
VALUE cb_cCouchRequest;
VALUE cb_cResult;
VALUE cb_cTimer;

/* Modules */
VALUE cb_mCouchbase;
VALUE cb_mError;
VALUE cb_mMarshal;
VALUE cb_mMultiJson;
VALUE cb_mURI;

/* Symbols */
ID cb_sym_add;
ID cb_sym_append;
ID cb_sym_assemble_hash;
ID cb_sym_body;
ID cb_sym_bucket;
ID cb_sym_cas;
ID cb_sym_chunked;
ID cb_sym_content_type;
ID cb_sym_create;
ID cb_sym_cluster;
ID cb_sym_decrement;
ID cb_sym_default_arithmetic_init;
ID cb_sym_default_flags;
ID cb_sym_default_format;
ID cb_sym_default_observe_timeout;
ID cb_sym_default_ttl;
ID cb_sym_delete;
ID cb_sym_delta;
ID cb_sym_development;
ID cb_sym_document;
ID cb_sym_environment;
ID cb_sym_extended;
ID cb_sym_flags;
ID cb_sym_format;
ID cb_sym_found;
ID cb_sym_get;
ID cb_sym_hostname;
ID cb_sym_http_request;
ID cb_sym_increment;
ID cb_sym_initial;
ID cb_sym_key_prefix;
ID cb_sym_lock;
ID cb_sym_management;
ID cb_sym_marshal;
ID cb_sym_method;
ID cb_sym_node_list;
ID cb_sym_not_found;
ID cb_sym_num_replicas;
ID cb_sym_observe;
ID cb_sym_password;
ID cb_sym_periodic;
ID cb_sym_persisted;
ID cb_sym_plain;
ID cb_sym_pool;
ID cb_sym_port;
ID cb_sym_post;
ID cb_sym_prepend;
ID cb_sym_production;
ID cb_sym_put;
ID cb_sym_quiet;
ID cb_sym_replace;
ID cb_sym_replica;
ID cb_sym_send_threshold;
ID cb_sym_set;
ID cb_sym_stats;
ID cb_sym_timeout;
ID cb_sym_touch;
ID cb_sym_ttl;
ID cb_sym_type;
ID cb_sym_unlock;
ID cb_sym_username;
ID cb_sym_version;
ID cb_sym_view;
ID cb_id_arity;
ID cb_id_call;
ID cb_id_delete;
ID cb_id_dump;
ID cb_id_dup;
ID cb_id_flatten_bang;
ID cb_id_has_key_p;
ID cb_id_host;
ID cb_id_iv_body;
ID cb_id_iv_cas;
ID cb_id_iv_completed;
ID cb_id_iv_error;
ID cb_id_iv_flags;
ID cb_id_iv_from_master;
ID cb_id_iv_headers;
ID cb_id_iv_inner_exception;
ID cb_id_iv_key;
ID cb_id_iv_node;
ID cb_id_iv_operation;
ID cb_id_iv_status;
ID cb_id_iv_time_to_persist;
ID cb_id_iv_time_to_replicate;
ID cb_id_iv_value;
ID cb_id_load;
ID cb_id_match;
ID cb_id_observe_and_wait;
ID cb_id_parse;
ID cb_id_parse_body_bang;
ID cb_id_password;
ID cb_id_path;
ID cb_id_port;
ID cb_id_scheme;
ID cb_id_sprintf;
ID cb_id_to_s;
ID cb_id_user;
ID cb_id_verify_observe_options;

/* Errors */
VALUE cb_eBaseError;
VALUE cb_eValueFormatError;
VALUE cb_eHTTPError;

                                   /* LCB_SUCCESS = 0x00         */
                                   /* LCB_AUTH_CONTINUE = 0x01   */
VALUE cb_eAuthError;               /* LCB_AUTH_ERROR = 0x02      */
VALUE cb_eDeltaBadvalError;        /* LCB_DELTA_BADVAL = 0x03    */
VALUE cb_eTooBigError;             /* LCB_E2BIG = 0x04           */
VALUE cb_eBusyError;               /* LCB_EBUSY = 0x05           */
VALUE cb_eInternalError;           /* LCB_EINTERNAL = 0x06       */
VALUE cb_eInvalidError;            /* LCB_EINVAL = 0x07          */
VALUE cb_eNoMemoryError;           /* LCB_ENOMEM = 0x08          */
VALUE cb_eRangeError;              /* LCB_ERANGE = 0x09          */
VALUE cb_eLibcouchbaseError;       /* LCB_ERROR = 0x0a           */
VALUE cb_eTmpFailError;            /* LCB_ETMPFAIL = 0x0b        */
VALUE cb_eKeyExistsError;          /* LCB_KEY_EEXISTS = 0x0c     */
VALUE cb_eNotFoundError;           /* LCB_KEY_ENOENT = 0x0d      */
VALUE cb_eDlopenFailedError;       /* LCB_DLOPEN_FAILED = 0x0e   */
VALUE cb_eDlsymFailedError;        /* LCB_DLSYM_FAILED = 0x0f    */
VALUE cb_eNetworkError;            /* LCB_NETWORK_ERROR = 0x10   */
VALUE cb_eNotMyVbucketError;       /* LCB_NOT_MY_VBUCKET = 0x11  */
VALUE cb_eNotStoredError;          /* LCB_NOT_STORED = 0x12      */
VALUE cb_eNotSupportedError;       /* LCB_NOT_SUPPORTED = 0x13   */
VALUE cb_eUnknownCommandError;     /* LCB_UNKNOWN_COMMAND = 0x14 */
VALUE cb_eUnknownHostError;        /* LCB_UNKNOWN_HOST = 0x15    */
VALUE cb_eProtocolError;           /* LCB_PROTOCOL_ERROR = 0x16  */
VALUE cb_eTimeoutError;            /* LCB_ETIMEDOUT = 0x17       */
VALUE cb_eConnectError;            /* LCB_CONNECT_ERROR = 0x18   */
VALUE cb_eBucketNotFoundError;     /* LCB_BUCKET_ENOENT = 0x19   */
VALUE cb_eClientNoMemoryError;     /* LCB_CLIENT_ENOMEM = 0x1a   */
VALUE cb_eClientTmpFailError;      /* LCB_CLIENT_ETMPFAIL = 0x1b */
VALUE cb_eBadHandleError;          /* LCB_EBADHANDLE = 0x1c      */

/* Default Strings */
VALUE cb_vStrDefault;
VALUE cb_vStrEmpty;

/* Ruby Extension initializer */
    void
Init_couchbase_ext(void)
{
    VALUE str;
    cb_mMultiJson = rb_const_get(rb_cObject, rb_intern("MultiJson"));
    cb_mURI = rb_const_get(rb_cObject, rb_intern("URI"));
    cb_mMarshal = rb_const_get(rb_cObject, rb_intern("Marshal"));
    cb_mCouchbase = rb_define_module("Couchbase");

    cb_mError = rb_define_module_under(cb_mCouchbase, "Error");
    /* Document-class: Couchbase::Error::Base
     * The base error class
     *
     * @since 1.0.0
     */
    cb_eBaseError = rb_define_class_under(cb_mError, "Base", rb_eStandardError);
    /* Document-class: Couchbase::Error::Auth
     * Authentication error
     *
     * You provided an invalid username/password combination.
     *
     * @since 1.0.0
     */
    cb_eAuthError = rb_define_class_under(cb_mError, "Auth", cb_eBaseError);
    /* Document-class: Couchbase::Error::BucketNotFound
     * Bucket not found
     *
     * The requested bucket not found in the cluster
     *
     * @since 1.0.0
     */
    cb_eBucketNotFoundError = rb_define_class_under(cb_mError, "BucketNotFound", cb_eBaseError);
    /* Document-class: Couchbase::Error::Busy
     * The cluster is too busy
     *
     * The server is too busy to handle your request right now.
     * please back off and try again at a later time.
     *
     * @since 1.0.0
     */
    cb_eBusyError = rb_define_class_under(cb_mError, "Busy", cb_eBaseError);
    /* Document-class: Couchbase::Error::DeltaBadval
     * The given value is not a number
     *
     * @since 1.0.0
     */
    cb_eDeltaBadvalError = rb_define_class_under(cb_mError, "DeltaBadval", cb_eBaseError);
    /* Document-class: Couchbase::Error::Internal
     * Internal error
     *
     * Internal error inside the library. You would have
     * to destroy the instance and create a new one to recover.
     *
     * @since 1.0.0
     */
    cb_eInternalError = rb_define_class_under(cb_mError, "Internal", cb_eBaseError);
    /* Document-class: Couchbase::Error::Invalid
     * Invalid arguments
     *
     * @since 1.0.0
     */
    cb_eInvalidError = rb_define_class_under(cb_mError, "Invalid", cb_eBaseError);
    /* Document-class: Couchbase::Error::KeyExists
     * Key already exists
     *
     * The key already exists (with another CAS value)
     *
     * @since 1.0.0
     */
    cb_eKeyExistsError = rb_define_class_under(cb_mError, "KeyExists", cb_eBaseError);
    /* Document-class: Couchbase::Error::Libcouchbase
     * Generic error
     *
     * @since 1.0.0
     */
    cb_eLibcouchbaseError = rb_define_class_under(cb_mError, "Libcouchbase", cb_eBaseError);
    /* Document-class: Couchbase::Error::Network
     * Network error
     *
     * A network related problem occured (name lookup, read/write/connect
     * etc)
     *
     * @since 1.0.0
     */
    cb_eNetworkError = rb_define_class_under(cb_mError, "Network", cb_eBaseError);
    /* Document-class: Couchbase::Error::NoMemory
     * Out of memory error (on Server)
     *
     * The client ran out of memory
     *
     * @since 1.0.0
     */
    cb_eNoMemoryError = rb_define_class_under(cb_mError, "NoMemory", cb_eBaseError);
    /* Document-class: Couchbase::Error::ClientNoMemory
     * Out of memory error (on Client)
     *
     * @since 1.2.0.dp6
     */
    cb_eClientNoMemoryError = rb_define_class_under(cb_mError, "ClientNoMemory", cb_eBaseError);
    /* Document-class: Couchbase::Error::NotFound
     * No such key
     *
     * @since 1.0.0
     */
    cb_eNotFoundError = rb_define_class_under(cb_mError, "NotFound", cb_eBaseError);
    /* Document-class: Couchbase::Error::NotMyVbucket
     * The vbucket is not located on this server
     *
     * The server who received the request is not responsible for the
     * object anymore. (This happens during changes in the cluster
     * topology)
     *
     * @since 1.0.0
     */
    cb_eNotMyVbucketError = rb_define_class_under(cb_mError, "NotMyVbucket", cb_eBaseError);
    /* Document-class: Couchbase::Error::NotStored
     * Not stored
     *
     * The object was not stored on the server
     *
     * @since 1.0.0
     */
    cb_eNotStoredError = rb_define_class_under(cb_mError, "NotStored", cb_eBaseError);
    /* Document-class: Couchbase::Error::NotSupported
     * Not supported
     *
     * The server doesn't support the requested command. This error differs
     * from {Couchbase::Error::UnknownCommand} by that the server knows
     * about the command, but for some reason decided to not support it.
     *
     * @since 1.0.0
     */
    cb_eNotSupportedError = rb_define_class_under(cb_mError, "NotSupported", cb_eBaseError);
    /* Document-class: Couchbase::Error::Range
     * Invalid range
     *
     * An invalid range specified
     *
     * @since 1.0.0
     */
    cb_eRangeError = rb_define_class_under(cb_mError, "Range", cb_eBaseError);
    /* Document-class: Couchbase::Error::TemporaryFail
     * Temporary failure
     *
     * The server tried to perform the requested operation, but failed
     * due to a temporary constraint. Retrying the operation may work.
     *
     * @since 1.0.0
     */
    cb_eTmpFailError = rb_define_class_under(cb_mError, "TemporaryFail", cb_eBaseError);
    /* Document-class: Couchbase::Error::ClientTemporaryFail
     * Temporary failure (on Client)
     *
     * The client encountered a temporary error (retry might resolve
     * the problem)
     *
     * @since 1.2.0
     */
    cb_eClientTmpFailError = rb_define_class_under(cb_mError, "ClientTemporaryFail", cb_eBaseError);
    /* Document-class: Couchbase::Error::TooBig
     * Object too big
     *
     * The sever reported that this object is too big
     *
     * @since 1.0.0
     */
    cb_eTooBigError = rb_define_class_under(cb_mError, "TooBig", cb_eBaseError);
    /* Document-class: Couchbase::Error::UnknownCommand
     * Unknown command
     *
     * The server doesn't know what that command is.
     *
     * @since 1.0.0
     */
    cb_eUnknownCommandError = rb_define_class_under(cb_mError, "UnknownCommand", cb_eBaseError);
    /* Document-class: Couchbase::Error::UnknownHost
     * Unknown host
     *
     * The server failed to resolve the requested hostname
     *
     * @since 1.0.0
     */
    cb_eUnknownHostError = rb_define_class_under(cb_mError, "UnknownHost", cb_eBaseError);
    /* Document-class: Couchbase::Error::ValueFormat
     * Failed to decode or encode value
     *
     * @since 1.0.0
     */
    cb_eValueFormatError = rb_define_class_under(cb_mError, "ValueFormat", cb_eBaseError);
    /* Document-class: Couchbase::Error::Protocol
     * Protocol error
     *
     * There is something wrong with the datastream received from
     * the server
     *
     * @since 1.0.0
     */
    cb_eProtocolError = rb_define_class_under(cb_mError, "Protocol", cb_eBaseError);
    /* Document-class: Couchbase::Error::Timeout
     * Timeout error
     *
     * The operation timed out
     *
     * @since 1.1.0
     */
    cb_eTimeoutError = rb_define_class_under(cb_mError, "Timeout", cb_eBaseError);
    /* Document-class: Couchbase::Error::Connect
     * Connect error
     *
     * @since 1.1.0
     */
    cb_eConnectError = rb_define_class_under(cb_mError, "Connect", cb_eBaseError);
    /* Document-class: Couchbase::Error::BadHandle
     * Invalid handle type.
     *
     * The requested operation isn't allowed for given type.
     *
     * @since 1.2.0
     */
    cb_eBadHandleError = rb_define_class_under(cb_mError, "BadHandle", cb_eBaseError);

    /* Document-class: Couchbase::Error::DlopenFailed
     * dlopen() failed
     *
     * Failed to open shared object
     *
     * @since 1.2.0
     */
    cb_eDlopenFailedError = rb_define_class_under(cb_mError, "DlopenFailed", cb_eBaseError);

    /* Document-class: Couchbase::Error::DlsymFailed
     * dlsym() failed
     *
     * Failed to locate the requested cb_symbol in the shared object
     *
     * @since 1.2.0
     */
    cb_eDlsymFailedError = rb_define_class_under(cb_mError, "DlsymFailed", cb_eBaseError);

    /* Document-class: Couchbase::Error::HTTP
     * HTTP error with status code
     *
     * @since 1.2.0
     */
    cb_eHTTPError = rb_define_class_under(cb_mError, "HTTP", cb_eBaseError);
    cb_id_iv_body = rb_intern("@body");
    /* Document-method: error
     *
     * The underlying libcouchbase library could return one of the following
     * error codes. The ruby client will wrap these errors into appropriate
     * exception class, derived from {Couchbase::Error::Base}.
     *
     * 0x00 :: LCB_SUCCESS (Success)
     * 0x01 :: LCB_AUTH_CONTINUE (Continue authentication)
     * 0x02 :: LCB_AUTH_ERROR (Authentication error)
     * 0x03 :: LCB_DELTA_BADVAL (Not a number)
     * 0x04 :: LCB_E2BIG (Object too big)
     * 0x05 :: LCB_EBUSY (Too busy)
     * 0x06 :: LCB_EINTERNAL (Internal error)
     * 0x07 :: LCB_EINVAL (Invalid arguments)
     * 0x08 :: LCB_ENOMEM (Out of memory)
     * 0x09 :: LCB_ERANGE (Invalid range)
     * 0x0a :: LCB_ERROR (Generic error)
     * 0x0b :: LCB_ETMPFAIL (Temporary failure)
     * 0x0c :: LCB_KEY_EEXISTS (Key exists (with a different CAS value))
     * 0x0d :: LCB_KEY_ENOENT (No such key)
     * 0x0e :: LCB_DLOPEN_FAILED (Failed to open shared object)
     * 0x0f :: LCB_DLSYM_FAILED (Failed to locate the requested cb_symbol in shared object)
     * 0x10 :: LCB_NETWORK_ERROR (Network error)
     * 0x11 :: LCB_NOT_MY_VBUCKET (The vbucket is not located on this server)
     * 0x12 :: LCB_NOT_STORED (Not stored)
     * 0x13 :: LCB_NOT_SUPPORTED (Not supported)
     * 0x14 :: LCB_UNKNOWN_COMMAND (Unknown command)
     * 0x15 :: LCB_UNKNOWN_HOST (Unknown host)
     * 0x16 :: LCB_PROTOCOL_ERROR (Protocol error)
     * 0x17 :: LCB_ETIMEDOUT (Operation timed out)
     * 0x18 :: LCB_CONNECT_ERROR (Connection failure)
     * 0x19 :: LCB_BUCKET_ENOENT (No such bucket)
     * 0x1a :: LCB_CLIENT_ENOMEM (Out of memory on the client)
     * 0x1b :: LCB_CLIENT_ETMPFAIL (Temporary failure on the client)
     * 0x1c :: LCB_EBADHANDLE (Invalid handle type)
     *
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the error code from libcouchbase
     */
    rb_define_attr(cb_eBaseError, "error", 1, 0);
    cb_id_iv_error = rb_intern("@error");
    /* Document-method: status
     *
     * @since 1.2.0.beta
     *
     * @return [Fixnum] The HTTP status code */
    rb_define_attr(cb_eBaseError, "status", 1, 0);
    cb_id_iv_status = rb_intern("@status");
    /* Document-method: key
     *
     * @since 1.0.0
     *
     * @return [String] the key which generated error */
    rb_define_attr(cb_eBaseError, "key", 1, 0);
    cb_id_iv_key = rb_intern("@key");
    /* Document-method: cas
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the version of the key (+nil+ unless accessible) */
    rb_define_attr(cb_eBaseError, "cas", 1, 0);
    cb_id_iv_cas = rb_intern("@cas");
    /* Document-method: operation
     *
     * @since 1.0.0
     *
     * @return [Symbol] the operation (+nil+ unless accessible) */
    rb_define_attr(cb_eBaseError, "operation", 1, 0);
    cb_id_iv_operation = rb_intern("@operation");
    /* Document-method: inner_exception
     *
     * @since 1.2.0.beta4
     *
     * @return [Exception] the inner exception or +nil+. Some exceptions like
     *      {Error::ValueFormat} wrap the original exception */
    rb_define_attr(cb_eBaseError, "inner_exception", 1, 0);
    cb_id_iv_inner_exception = rb_intern("@inner_exception");

    /* Document-class: Couchbase::Result
     *
     * The object which yielded to asynchronous callbacks
     *
     * @since 1.0.0
     */
    cb_cResult = rb_define_class_under(cb_mCouchbase, "Result", rb_cObject);
    rb_define_method(cb_cResult, "inspect", cb_result_inspect, 0);
    rb_define_method(cb_cResult, "success?", cb_result_success_p, 0);
    /* Document-method: operation
     *
     * @since 1.0.0
     *
     * @return [Symbol]
     */
    rb_define_attr(cb_cResult, "operation", 1, 0);
    /* Document-method: error
     *
     * @since 1.0.0
     *
     * @return [Couchbase::Error::Base]
     */
    rb_define_attr(cb_cResult, "error", 1, 0);
    /* Document-method: key
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cb_cResult, "key", 1, 0);
    cb_id_iv_key = rb_intern("@key");
    /* Document-method: value
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cb_cResult, "value", 1, 0);
    cb_id_iv_value = rb_intern("@value");
    /* Document-method: cas
     *
     * @since 1.0.0
     *
     * @return [Fixnum]
     */
    rb_define_attr(cb_cResult, "cas", 1, 0);
    cb_id_iv_cas = rb_intern("@cas");
    /* Document-method: flags
     *
     * @since 1.0.0
     *
     * @return [Fixnum]
     */
    rb_define_attr(cb_cResult, "flags", 1, 0);
    cb_id_iv_flags = rb_intern("@flags");
    /* Document-method: node
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cb_cResult, "node", 1, 0);
    cb_id_iv_node = rb_intern("@node");
    /* Document-method: headers
     *
     * @since 1.2.0
     *
     * HTTP headers
     *
     * @return [Hash]
     */
    rb_define_attr(cb_cResult, "headers", 1, 0);
    cb_id_iv_headers = rb_intern("@headers");
    /* Document-method: completed
     * In {Bucket::CouchRequest} operations used to mark the final call
     * @return [Boolean] */
    rb_define_attr(cb_cResult, "completed", 1, 0);
    rb_define_alias(cb_cResult, "completed?", "completed");
    cb_id_iv_completed = rb_intern("@completed");
    /* Document-method: status
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Status of the key. Possible values:
     * +:found+ :: Key found in cache, but not yet persisted
     * +:persisted+ :: Key found and persisted
     * +:not_found+ :: Key not found
     *
     * @return [Symbol]
     */
    rb_define_attr(cb_cResult, "status", 1, 0);
    cb_id_iv_status = rb_intern("@status");
    /* Document-method: from_master
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * True if key stored on master
     * @return [Boolean]
     */
    rb_define_attr(cb_cResult, "from_master", 1, 0);
    rb_define_alias(cb_cResult, "from_master?", "from_master");
    cb_id_iv_from_master = rb_intern("@from_master");
    /* Document-method: time_to_persist
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Average time needed to persist key on the disk (zero if unavailable)
     * @return [Fixnum]
     */
    rb_define_attr(cb_cResult, "time_to_persist", 1, 0);
    rb_define_alias(cb_cResult, "ttp", "time_to_persist");
    cb_id_iv_time_to_persist = rb_intern("@time_to_persist");
    /* Document-method: time_to_persist
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Average time needed to replicate key on the disk (zero if unavailable)
     * @return [Fixnum]
     */
    rb_define_attr(cb_cResult, "time_to_replicate", 1, 0);
    rb_define_alias(cb_cResult, "ttr", "time_to_replicate");
    cb_id_iv_time_to_replicate = rb_intern("@time_to_replicate");

    /* Document-class: Couchbase::Bucket
     *
     * This class in charge of all stuff connected to communication with
     * Couchbase.
     *
     * @since 1.0.0
     */
    cb_cBucket = rb_define_class_under(cb_mCouchbase, "Bucket", rb_cObject);

    /* 0x03: Bitmask for flag bits responsible for format */
    rb_define_const(cb_cBucket, "FMT_MASK", INT2FIX(CB_FMT_MASK));
    /* 0x00: Document format. The (default) format supports most of ruby
     * types which could be mapped to JSON data (hashes, arrays, strings,
     * numbers). Future version will be able to run map/reduce queries on
     * the values in the document form (hashes). */
    rb_define_const(cb_cBucket, "FMT_DOCUMENT", INT2FIX(CB_FMT_DOCUMENT));
    /* 0x01:  Marshal format. The format which supports transparent
     * serialization of ruby objects with standard <tt>Marshal.dump</tt> and
     * <tt>Marhal.load</tt> methods. */
    rb_define_const(cb_cBucket, "FMT_MARSHAL", INT2FIX(CB_FMT_MARSHAL));
    /* 0x02:  Plain format. The format which force client don't apply any
     * conversions to the value, but it should be passed as String. It
     * could be useful for building custom algorithms or formats. For
     * example implement set:
     * http://dustin.github.com/2011/02/17/memcached-set.html */
    rb_define_const(cb_cBucket, "FMT_PLAIN", INT2FIX(CB_FMT_PLAIN));

    rb_define_alloc_func(cb_cBucket, cb_bucket_alloc);
    rb_define_method(cb_cBucket, "initialize", cb_bucket_init, -1);
    rb_define_method(cb_cBucket, "initialize_copy", cb_bucket_init_copy, 1);
    rb_define_method(cb_cBucket, "inspect", cb_bucket_inspect, 0);

    rb_define_method(cb_cBucket, "add", cb_bucket_add, -1);
    rb_define_method(cb_cBucket, "append", cb_bucket_append, -1);
    rb_define_method(cb_cBucket, "prepend", cb_bucket_prepend, -1);
    rb_define_method(cb_cBucket, "replace", cb_bucket_replace, -1);
    rb_define_method(cb_cBucket, "set", cb_bucket_set, -1);
    rb_define_method(cb_cBucket, "get", cb_bucket_get, -1);
    rb_define_method(cb_cBucket, "run", cb_bucket_run, -1);
    rb_define_method(cb_cBucket, "stop", cb_bucket_stop, 0);
    rb_define_method(cb_cBucket, "touch", cb_bucket_touch, -1);
    rb_define_method(cb_cBucket, "delete", cb_bucket_delete, -1);
    rb_define_method(cb_cBucket, "stats", cb_bucket_stats, -1);
    rb_define_method(cb_cBucket, "version", cb_bucket_version, -1);
    rb_define_method(cb_cBucket, "incr", cb_bucket_incr, -1);
    rb_define_method(cb_cBucket, "decr", cb_bucket_decr, -1);
    rb_define_method(cb_cBucket, "unlock", cb_bucket_unlock, -1);
    rb_define_method(cb_cBucket, "disconnect", cb_bucket_disconnect, 0);
    rb_define_method(cb_cBucket, "reconnect", cb_bucket_reconnect, -1);
    rb_define_method(cb_cBucket, "make_http_request", cb_bucket_make_http_request, -1);
    rb_define_method(cb_cBucket, "observe", cb_bucket_observe, -1);

    rb_define_alias(cb_cBucket, "decrement", "decr");
    rb_define_alias(cb_cBucket, "increment", "incr");

    rb_define_alias(cb_cBucket, "[]", "get");
    rb_define_alias(cb_cBucket, "[]=", "set");
    rb_define_method(cb_cBucket, "[]=", cb_bucket_aset, -1);

    rb_define_method(cb_cBucket, "connected?", cb_bucket_connected_p, 0);
    rb_define_method(cb_cBucket, "async?", cb_bucket_async_p, 0);

    /* Document-method: quiet
     * Flag specifying behaviour for operations on missing keys
     *
     * @since 1.0.0
     *
     * If it is +true+, the operations will silently return +nil+ or +false+
     * instead of raising {Couchbase::Error::NotFound}.
     *
     * @example Hiding cache miss (considering "miss" key is not stored)
     *   connection.quiet = true
     *   connection.get("miss")     #=> nil
     *
     * @example Raising errors on miss (considering "miss" key is not stored)
     *   connection.quiet = false
     *   connection.get("miss")     #=> will raise Couchbase::Error::NotFound
     *
     * @return [true, false] */
    /* rb_define_attr(cb_cBucket, "quiet", 1, 1); */
    rb_define_method(cb_cBucket, "quiet", cb_bucket_quiet_get, 0);
    rb_define_method(cb_cBucket, "quiet=", cb_bucket_quiet_set, 1);
    rb_define_alias(cb_cBucket, "quiet?", "quiet");

    /* Document-method: default_flags
     * Default flags for new values.
     *
     * @since 1.0.0
     *
     * The library reserves last two lower bits to store the format of the
     * value. The can be masked via FMT_MASK constant.
     *
     * @example Selecting format bits
     *   connection.default_flags & Couchbase::Bucket::FMT_MASK
     *
     * @example Set user defined bits
     *   connection.default_flags |= 0x6660
     *
     * @note Amending format bit will also change #default_format value
     *
     * @return [Fixnum] the effective flags */
    /* rb_define_attr(cb_cBucket, "default_flags", 1, 1); */
    rb_define_method(cb_cBucket, "default_flags", cb_bucket_default_flags_get, 0);
    rb_define_method(cb_cBucket, "default_flags=", cb_bucket_default_flags_set, 1);

    /* Document-method: default_format
     * Default format for new values.
     *
     * @since 1.0.0
     *
     * @see http://couchbase.com/docs/couchbase-manual-2.0/couchbase-views-datastore.html
     *
     * It uses flags field to store the format. It accepts either the Symbol
     * (+:document+, +:marshal+, +:plain+) or Fixnum (use constants
     * FMT_DOCUMENT, FMT_MARSHAL, FMT_PLAIN) and silently ignores all
     * other value.
     *
     * Here is some notes regarding how to choose the format:
     *
     * * <tt>:document</tt> (default) format supports most of ruby types
     *   which could be mapped to JSON data (hashes, arrays, strings,
     *   numbers). Future version will be able to run map/reduce queries on
     *   the values in the document form (hashes).
     *
     * * <tt>:plain</tt> format if you no need any conversions to be applied
     *   to your data, but your data should be passed as String. It could be
     *   useful for building custom algorithms or formats. For example
     *   implement set: http://dustin.github.com/2011/02/17/memcached-set.html
     *
     * * <tt>:marshal</tt> format if you'd like to transparently serialize
     *   your ruby object with standard <tt>Marshal.dump</tt> and
     *   <tt>Marhal.load</tt> methods.
     *
     * @example Selecting plain format using cb_symbol
     *   connection.format = :document
     *
     * @example Selecting plain format using Fixnum constant
     *   connection.format = Couchbase::Bucket::FMT_PLAIN
     *
     * @note Amending default_format will also change #default_flags value
     *
     * @return [Symbol] the effective format */
    /* rb_define_attr(cb_cBucket, "default_format", 1, 1); */
    rb_define_method(cb_cBucket, "default_format", cb_bucket_default_format_get, 0);
    rb_define_method(cb_cBucket, "default_format=", cb_bucket_default_format_set, 1);

    /* Document-method: timeout
     *
     * @since 1.1.0
     *
     * @return [Fixnum] The timeout for the operations in microseconds. The
     *   client will raise {Couchbase::Error::Timeout} exception for all
     *   commands which weren't completed in given timeslot. */
    /* rb_define_attr(cb_cBucket, "timeout", 1, 1); */
    rb_define_method(cb_cBucket, "timeout", cb_bucket_timeout_get, 0);
    rb_define_method(cb_cBucket, "timeout=", cb_bucket_timeout_set, 1);

    /* Document-method: default_arithmetic_init
     *
     * @since 1.2.0
     *
     * @return [Fixnum, true] The initial value for arithmetic operations
     *   {Bucket#incr} and {Bucket#decr}. Setting this attribute will force
     *   aforementioned operations create keys unless they exists in the
     *   bucket and will use given value. You can also just specify +true+
     *   if you'd like just force key creation with zero default value.
     */
    /* rb_define_attr(cb_cBucket, "default_arithmetic_init", 1, 1); */
    rb_define_method(cb_cBucket, "default_arithmetic_init", cb_bucket_default_arithmetic_init_get, 0);
    rb_define_method(cb_cBucket, "default_arithmetic_init=", cb_bucket_default_arithmetic_init_set, 1);

    /* Document-method: key_prefix
     *
     * @since 1.2.0.dp5
     *
     * @return [String] The library will prepend +key_prefix+ to each key to
     *   provide simple namespacing. */
    /* rb_define_attr(cb_cBucket, "key_prefix", 1, 1); */
    rb_define_method(cb_cBucket, "key_prefix", cb_bucket_key_prefix_get, 0);
    rb_define_method(cb_cBucket, "key_prefix=", cb_bucket_key_prefix_set, 1);

    /* Document-method: on_error
     * Error callback for asynchronous mode.
     *
     * @since 1.0.0
     *
     * This callback is using to deliver exceptions in asynchronous mode.
     *
     * @yieldparam [Symbol] op The operation caused the error
     * @yieldparam [String] key The key which cause the error or +nil+
     * @yieldparam [Exception] exc The exception instance
     *
     * @example Using lambda syntax
     *   connection = Couchbase.new(:async => true)
     *   connection.on_error = lambda {|op, key, exc| ... }
     *   connection.run do |conn|
     *     conn.set("foo", "bar")
     *   end
     *
     * @example Using block syntax
     *   connection = Couchbase.new(:async => true)
     *   connection.on_error {|op, key, exc| ... }
     *   ...
     *
     * @return [Proc] the effective callback */
    /* rb_define_attr(cb_cBucket, "on_error", 1, 1); */
    rb_define_method(cb_cBucket, "on_error", cb_bucket_on_error_get, 0);
    rb_define_method(cb_cBucket, "on_error=", cb_bucket_on_error_set, 1);

    /* Document-method: url
     *
     * The config url for this connection.
     *
     * Generally it is the bootstrap URL, but it could be different after
     * cluster upgrade. This url is used to fetch the cluster
     * configuration.
     *
     * @since 1.0.0
     *
     * @return [String] the address of the cluster management interface
     */
    /* rb_define_attr(cb_cBucket, "url", 1, 0); */
    rb_define_method(cb_cBucket, "url", cb_bucket_url_get, 0);
    /* Document-method: hostname
     *
     * The hostname of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     *
     * @return [String] the host name of the management interface (default: "localhost")
     */
    /* rb_define_attr(cb_cBucket, "hostname", 1, 0); */
    rb_define_method(cb_cBucket, "hostname", cb_bucket_hostname_get, 0);
    /* Document-method: port
     *
     * The port of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the port number of the management interface (default: 8091)
     */
    /* rb_define_attr(cb_cBucket, "port", 1, 0); */
    rb_define_method(cb_cBucket, "port", cb_bucket_port_get, 0);
    /* Document-method: authority
     *
     * The authority ("hostname:port") of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     *
     * @return [String] host with port
     */
    /* rb_define_attr(cb_cBucket, "authority", 1, 0); */
    rb_define_method(cb_cBucket, "authority", cb_bucket_authority_get, 0);
    /* Document-method: bucket
     *
     * The bucket name of the current connection
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     *
     * @return [String] the bucket name
     */
    /* rb_define_attr(cb_cBucket, "bucket", 1, 0); */
    rb_define_method(cb_cBucket, "bucket", cb_bucket_bucket_get, 0);
    rb_define_alias(cb_cBucket, "name", "bucket");
    /* Document-method: pool
     *
     * The pool name of the current connection
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     *
     * @return [String] the pool name (usually "default")
     */
    /* rb_define_attr(cb_cBucket, "pool", 1, 0); */
    rb_define_method(cb_cBucket, "pool", cb_bucket_pool_get, 0);
    /* Document-method: username
     *
     * The user name used to connect to the cluster
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     *
     * @return [String] the username for protected buckets (usually matches
     *   the bucket name)
     */
    /* rb_define_attr(cb_cBucket, "username", 1, 0); */
    rb_define_method(cb_cBucket, "username", cb_bucket_username_get, 0);
    /* Document-method: password
     *
     * The password used to connect to the cluster
     *
     * @since 1.0.0
     *
     * @return [String] the password for protected buckets
     */
    /* rb_define_attr(cb_cBucket, "password", 1, 0); */
    rb_define_method(cb_cBucket, "password", cb_bucket_password_get, 0);
    /* Document-method: environment
     *
     * The environment of the connection (+:development+ or +:production+)
     *
     * @since 1.2.0
     *
     * @return [Symbol]
     */
    /* rb_define_attr(cb_cBucket, "environment", 1, 0); */
    rb_define_method(cb_cBucket, "environment", cb_bucket_environment_get, 0);
    /* Document-method: num_replicas
     *
     * @since 1.2.0.dp6
     *
     * The numbers of the replicas for each node in the cluster
     *
     * @return [Fixnum]
     */
    /* rb_define_attr(cb_cBucket, "num_replicas", 1, 0); */
    rb_define_method(cb_cBucket, "num_replicas", cb_bucket_num_replicas_get, 0);
    /* Document-method: default_observe_timeout
     *
     * @since 1.2.0.dp6
     *
     * The default timeout value for {Bucket#observe_and_wait} operation in
     * microseconds
     *
     * @return [Fixnum]
     */
    /* rb_define_attr(cb_cBucket, "default_observe_timeout", 1, 1); */
    rb_define_method(cb_cBucket, "default_observe_timeout", cb_bucket_default_observe_timeout_get, 0);
    rb_define_method(cb_cBucket, "default_observe_timeout=", cb_bucket_default_observe_timeout_set, 1);

    cb_cCouchRequest = rb_define_class_under(cb_cBucket, "CouchRequest", rb_cObject);
    rb_define_alloc_func(cb_cCouchRequest, cb_http_request_alloc);

    rb_define_method(cb_cCouchRequest, "initialize", cb_http_request_init, -1);
    rb_define_method(cb_cCouchRequest, "inspect", cb_http_request_inspect, 0);
    rb_define_method(cb_cCouchRequest, "on_body", cb_http_request_on_body, 0);
    rb_define_method(cb_cCouchRequest, "perform", cb_http_request_perform, 0);
    rb_define_method(cb_cCouchRequest, "pause", cb_http_request_pause, 0);
    rb_define_method(cb_cCouchRequest, "continue", cb_http_request_continue, 0);

    /* rb_define_attr(cb_cCouchRequest, "path", 1, 0); */
    rb_define_method(cb_cCouchRequest, "path", cb_http_request_path_get, 0);
    /* rb_define_attr(cb_cCouchRequest, "extended", 1, 0); */
    rb_define_method(cb_cCouchRequest, "extended", cb_http_request_extended_get, 0);
    rb_define_alias(cb_cCouchRequest, "extended?", "extended");
    /* rb_define_attr(cb_cCouchRequest, "chunked", 1, 0); */
    rb_define_method(cb_cCouchRequest, "chunked", cb_http_request_chunked_get, 0);
    rb_define_alias(cb_cCouchRequest, "chunked?", "chunked");

    cb_cTimer = rb_define_class_under(cb_mCouchbase, "Timer", rb_cObject);
    rb_define_alloc_func(cb_cTimer, cb_timer_alloc);
    rb_define_method(cb_cTimer, "initialize", cb_timer_init, -1);
    rb_define_method(cb_cTimer, "inspect", cb_timer_inspect, 0);
    rb_define_method(cb_cTimer, "cancel", cb_timer_cancel, 0);

    /* Define cb_symbols */
    cb_id_arity = rb_intern("arity");
    cb_id_call = rb_intern("call");
    cb_id_delete = rb_intern("delete");
    cb_id_dump = rb_intern("dump");
    cb_id_dup = rb_intern("dup");
    cb_id_flatten_bang = rb_intern("flatten!");
    cb_id_has_key_p = rb_intern("has_key?");
    cb_id_host = rb_intern("host");
    cb_id_load = rb_intern("load");
    cb_id_match = rb_intern("match");
    cb_id_observe_and_wait = rb_intern("observe_and_wait");
    cb_id_parse = rb_intern("parse");
    cb_id_parse_body_bang = rb_intern("parse_body!");
    cb_id_password = rb_intern("password");
    cb_id_path = rb_intern("path");
    cb_id_port = rb_intern("port");
    cb_id_scheme = rb_intern("scheme");
    cb_id_sprintf = rb_intern("sprintf");
    cb_id_to_s = rb_intern("to_s");
    cb_id_user = rb_intern("user");
    cb_id_verify_observe_options = rb_intern("verify_observe_options");

    cb_sym_add = ID2SYM(rb_intern("add"));
    cb_sym_append = ID2SYM(rb_intern("append"));
    cb_sym_assemble_hash = ID2SYM(rb_intern("assemble_hash"));
    cb_sym_body = ID2SYM(rb_intern("body"));
    cb_sym_bucket = ID2SYM(rb_intern("bucket"));
    cb_sym_cas = ID2SYM(rb_intern("cas"));
    cb_sym_chunked = ID2SYM(rb_intern("chunked"));
    cb_sym_cluster = ID2SYM(rb_intern("cluster"));
    cb_sym_content_type = ID2SYM(rb_intern("content_type"));
    cb_sym_create = ID2SYM(rb_intern("create"));
    cb_sym_decrement = ID2SYM(rb_intern("decrement"));
    cb_sym_default_arithmetic_init = ID2SYM(rb_intern("default_arithmetic_init"));
    cb_sym_default_flags = ID2SYM(rb_intern("default_flags"));
    cb_sym_default_format = ID2SYM(rb_intern("default_format"));
    cb_sym_default_ttl = ID2SYM(rb_intern("default_ttl"));
    cb_sym_delete = ID2SYM(rb_intern("delete"));
    cb_sym_delta = ID2SYM(rb_intern("delta"));
    cb_sym_development = ID2SYM(rb_intern("development"));
    cb_sym_document = ID2SYM(rb_intern("document"));
    cb_sym_environment = ID2SYM(rb_intern("environment"));
    cb_sym_extended = ID2SYM(rb_intern("extended"));
    cb_sym_flags = ID2SYM(rb_intern("flags"));
    cb_sym_format = ID2SYM(rb_intern("format"));
    cb_sym_found = ID2SYM(rb_intern("found"));
    cb_sym_get = ID2SYM(rb_intern("get"));
    cb_sym_hostname = ID2SYM(rb_intern("hostname"));
    cb_sym_http_request = ID2SYM(rb_intern("http_request"));
    cb_sym_increment = ID2SYM(rb_intern("increment"));
    cb_sym_initial = ID2SYM(rb_intern("initial"));
    cb_sym_key_prefix = ID2SYM(rb_intern("key_prefix"));
    cb_sym_lock = ID2SYM(rb_intern("lock"));
    cb_sym_management = ID2SYM(rb_intern("management"));
    cb_sym_marshal = ID2SYM(rb_intern("marshal"));
    cb_sym_method = ID2SYM(rb_intern("method"));
    cb_sym_node_list = ID2SYM(rb_intern("node_list"));
    cb_sym_not_found = ID2SYM(rb_intern("not_found"));
    cb_sym_num_replicas = ID2SYM(rb_intern("num_replicas"));
    cb_sym_observe = ID2SYM(rb_intern("observe"));
    cb_sym_password = ID2SYM(rb_intern("password"));
    cb_sym_periodic = ID2SYM(rb_intern("periodic"));
    cb_sym_persisted = ID2SYM(rb_intern("persisted"));
    cb_sym_plain = ID2SYM(rb_intern("plain"));
    cb_sym_pool = ID2SYM(rb_intern("pool"));
    cb_sym_port = ID2SYM(rb_intern("port"));
    cb_sym_post = ID2SYM(rb_intern("post"));
    cb_sym_prepend = ID2SYM(rb_intern("prepend"));
    cb_sym_production = ID2SYM(rb_intern("production"));
    cb_sym_put = ID2SYM(rb_intern("put"));
    cb_sym_quiet = ID2SYM(rb_intern("quiet"));
    cb_sym_replace = ID2SYM(rb_intern("replace"));
    cb_sym_replica = ID2SYM(rb_intern("replica"));
    cb_sym_send_threshold = ID2SYM(rb_intern("send_threshold"));
    cb_sym_set = ID2SYM(rb_intern("set"));
    cb_sym_stats = ID2SYM(rb_intern("stats"));
    cb_sym_timeout = ID2SYM(rb_intern("timeout"));
    cb_sym_touch = ID2SYM(rb_intern("touch"));
    cb_sym_ttl = ID2SYM(rb_intern("ttl"));
    cb_sym_type = ID2SYM(rb_intern("type"));
    cb_sym_unlock = ID2SYM(rb_intern("unlock"));
    cb_sym_username = ID2SYM(rb_intern("username"));
    cb_sym_version = ID2SYM(rb_intern("version"));
    cb_sym_view = ID2SYM(rb_intern("view"));

    cb_vStrDefault = str = STR_NEW_CSTR("default");
    rb_str_freeze(cb_vStrDefault);
    rb_const_set(cb_mCouchbase, rb_intern("_STR_DEFAULT"), str);
    cb_vStrEmpty = str = STR_NEW_CSTR("");
    rb_str_freeze(cb_vStrEmpty);
    rb_const_set(cb_mCouchbase, rb_intern("_STR_EMPTY"), str);
}
