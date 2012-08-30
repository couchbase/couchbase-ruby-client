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
VALUE cBucket;
VALUE cCouchRequest;
VALUE cResult;
VALUE cTimer;

/* Modules */
VALUE mCouchbase;
VALUE mError;
VALUE mMarshal;
VALUE mMultiJson;
VALUE mURI;

/* Symbols */
ID sym_add;
ID sym_append;
ID sym_assemble_hash;
ID sym_body;
ID sym_bucket;
ID sym_cas;
ID sym_chunked;
ID sym_content_type;
ID sym_create;
ID sym_decrement;
ID sym_default_flags;
ID sym_default_format;
ID sym_default_observe_timeout;
ID sym_default_ttl;
ID sym_delete;
ID sym_delta;
ID sym_development;
ID sym_document;
ID sym_environment;
ID sym_extended;
ID sym_flags;
ID sym_flush;
ID sym_format;
ID sym_found;
ID sym_get;
ID sym_hostname;
ID sym_http_request;
ID sym_increment;
ID sym_initial;
ID sym_key_prefix;
ID sym_lock;
ID sym_management;
ID sym_marshal;
ID sym_method;
ID sym_node_list;
ID sym_not_found;
ID sym_num_replicas;
ID sym_observe;
ID sym_password;
ID sym_periodic;
ID sym_persisted;
ID sym_plain;
ID sym_pool;
ID sym_port;
ID sym_post;
ID sym_prepend;
ID sym_production;
ID sym_put;
ID sym_quiet;
ID sym_replace;
ID sym_replica;
ID sym_send_threshold;
ID sym_set;
ID sym_stats;
ID sym_timeout;
ID sym_touch;
ID sym_ttl;
ID sym_type;
ID sym_username;
ID sym_version;
ID sym_view;
ID id_arity;
ID id_call;
ID id_delete;
ID id_dump;
ID id_dup;
ID id_flatten_bang;
ID id_has_key_p;
ID id_host;
ID id_iv_cas;
ID id_iv_completed;
ID id_iv_error;
ID id_iv_flags;
ID id_iv_from_master;
ID id_iv_headers;
ID id_iv_key;
ID id_iv_node;
ID id_iv_operation;
ID id_iv_status;
ID id_iv_status;
ID id_iv_time_to_persist;
ID id_iv_time_to_replicate;
ID id_iv_value;
ID id_load;
ID id_match;
ID id_observe_and_wait;
ID id_parse;
ID id_password;
ID id_path;
ID id_port;
ID id_scheme;
ID id_to_s;
ID id_user;
ID id_verify_observe_options;

/* Errors */
VALUE eBaseError;
VALUE eValueFormatError;
                                /* LCB_SUCCESS = 0x00         */
                                /* LCB_AUTH_CONTINUE = 0x01   */
VALUE eAuthError;               /* LCB_AUTH_ERROR = 0x02      */
VALUE eDeltaBadvalError;        /* LCB_DELTA_BADVAL = 0x03    */
VALUE eTooBigError;             /* LCB_E2BIG = 0x04           */
VALUE eBusyError;               /* LCB_EBUSY = 0x05           */
VALUE eInternalError;           /* LCB_EINTERNAL = 0x06       */
VALUE eInvalidError;            /* LCB_EINVAL = 0x07          */
VALUE eNoMemoryError;           /* LCB_ENOMEM = 0x08          */
VALUE eRangeError;              /* LCB_ERANGE = 0x09          */
VALUE eLibcouchbaseError;       /* LCB_ERROR = 0x0a           */
VALUE eTmpFailError;            /* LCB_ETMPFAIL = 0x0b        */
VALUE eKeyExistsError;          /* LCB_KEY_EEXISTS = 0x0c     */
VALUE eNotFoundError;           /* LCB_KEY_ENOENT = 0x0d      */
VALUE eLibeventError;           /* LCB_LIBEVENT_ERROR = 0x0e  */
VALUE eNetworkError;            /* LCB_NETWORK_ERROR = 0x0f   */
VALUE eNotMyVbucketError;       /* LCB_NOT_MY_VBUCKET = 0x10  */
VALUE eNotStoredError;          /* LCB_NOT_STORED = 0x11      */
VALUE eNotSupportedError;       /* LCB_NOT_SUPPORTED = 0x12   */
VALUE eUnknownCommandError;     /* LCB_UNKNOWN_COMMAND = 0x13 */
VALUE eUnknownHostError;        /* LCB_UNKNOWN_HOST = 0x14    */
VALUE eProtocolError;           /* LCB_PROTOCOL_ERROR = 0x15  */
VALUE eTimeoutError;            /* LCB_ETIMEDOUT = 0x16       */
VALUE eConnectError;            /* LCB_CONNECT_ERROR = 0x17   */
VALUE eBucketNotFoundError;     /* LCB_BUCKET_ENOENT = 0x18   */
VALUE eClientNoMemoryError;     /* LCB_CLIENT_ENOMEM = 0x19   */


/* Ruby Extension initializer */
    void
Init_couchbase_ext(void)
{
    mMultiJson = rb_const_get(rb_cObject, rb_intern("MultiJson"));
    mURI = rb_const_get(rb_cObject, rb_intern("URI"));
    mMarshal = rb_const_get(rb_cObject, rb_intern("Marshal"));
    mCouchbase = rb_define_module("Couchbase");

    mError = rb_define_module_under(mCouchbase, "Error");
    /* Document-class: Couchbase::Error::Base
     * The base error class
     *
     * @since 1.0.0
     */
    eBaseError = rb_define_class_under(mError, "Base", rb_eStandardError);
    /* Document-class: Couchbase::Error::Auth
     * Authentication error
     *
     * @since 1.0.0
     */
    eAuthError = rb_define_class_under(mError, "Auth", eBaseError);
    /* Document-class: Couchbase::Error::BucketNotFound
     * The given bucket not found in the cluster
     *
     * @since 1.0.0
     */
    eBucketNotFoundError = rb_define_class_under(mError, "BucketNotFound", eBaseError);
    /* Document-class: Couchbase::Error::Busy
     * The cluster is too busy now. Try again later
     *
     * @since 1.0.0
     */
    eBusyError = rb_define_class_under(mError, "Busy", eBaseError);
    /* Document-class: Couchbase::Error::DeltaBadval
     * The given value is not a number
     *
     * @since 1.0.0
     */
    eDeltaBadvalError = rb_define_class_under(mError, "DeltaBadval", eBaseError);
    /* Document-class: Couchbase::Error::Internal
     * Internal error
     *
     * @since 1.0.0
     */
    eInternalError = rb_define_class_under(mError, "Internal", eBaseError);
    /* Document-class: Couchbase::Error::Invalid
     * Invalid arguments
     *
     * @since 1.0.0
     */
    eInvalidError = rb_define_class_under(mError, "Invalid", eBaseError);
    /* Document-class: Couchbase::Error::KeyExists
     * Key already exists
     *
     * @since 1.0.0
     */
    eKeyExistsError = rb_define_class_under(mError, "KeyExists", eBaseError);
    /* Document-class: Couchbase::Error::Libcouchbase
     * Generic error
     *
     * @since 1.0.0
     */
    eLibcouchbaseError = rb_define_class_under(mError, "Libcouchbase", eBaseError);
    /* Document-class: Couchbase::Error::Libevent
     * Problem using libevent
     *
     * @since 1.0.0
     */
    eLibeventError = rb_define_class_under(mError, "Libevent", eBaseError);
    /* Document-class: Couchbase::Error::Network
     * Network error
     *
     * @since 1.0.0
     */
    eNetworkError = rb_define_class_under(mError, "Network", eBaseError);
    /* Document-class: Couchbase::Error::NoMemory
     * Out of memory error (on Server)
     *
     * @since 1.0.0
     */
    eNoMemoryError = rb_define_class_under(mError, "NoMemory", eBaseError);
    /* Document-class: Couchbase::Error::ClientNoMemory
     * Out of memory error (on Client)
     *
     * @since 1.2.0.dp6
     */
    eClientNoMemoryError = rb_define_class_under(mError, "ClientNoMemory", eBaseError);
    /* Document-class: Couchbase::Error::NotFound
     * No such key
     *
     * @since 1.0.0
     */
    eNotFoundError = rb_define_class_under(mError, "NotFound", eBaseError);
    /* Document-class: Couchbase::Error::NotMyVbucket
     * The vbucket is not located on this server
     *
     * @since 1.0.0
     */
    eNotMyVbucketError = rb_define_class_under(mError, "NotMyVbucket", eBaseError);
    /* Document-class: Couchbase::Error::NotStored
     * Not stored
     *
     * @since 1.0.0
     */
    eNotStoredError = rb_define_class_under(mError, "NotStored", eBaseError);
    /* Document-class: Couchbase::Error::NotSupported
     * Not supported
     *
     * @since 1.0.0
     */
    eNotSupportedError = rb_define_class_under(mError, "NotSupported", eBaseError);
    /* Document-class: Couchbase::Error::Range
     * Invalid range
     *
     * @since 1.0.0
     */
    eRangeError = rb_define_class_under(mError, "Range", eBaseError);
    /* Document-class: Couchbase::Error::TemporaryFail
     * Temporary failure. Try again later
     *
     * @since 1.0.0
     */
    eTmpFailError = rb_define_class_under(mError, "TemporaryFail", eBaseError);
    /* Document-class: Couchbase::Error::TooBig
     * Object too big
     *
     * @since 1.0.0
     */
    eTooBigError = rb_define_class_under(mError, "TooBig", eBaseError);
    /* Document-class: Couchbase::Error::UnknownCommand
     * Unknown command
     *
     * @since 1.0.0
     */
    eUnknownCommandError = rb_define_class_under(mError, "UnknownCommand", eBaseError);
    /* Document-class: Couchbase::Error::UnknownHost
     * Unknown host
     *
     * @since 1.0.0
     */
    eUnknownHostError = rb_define_class_under(mError, "UnknownHost", eBaseError);
    /* Document-class: Couchbase::Error::ValueFormat
     * Failed to decode or encode value
     *
     * @since 1.0.0
     */
    eValueFormatError = rb_define_class_under(mError, "ValueFormat", eBaseError);
    /* Document-class: Couchbase::Error::Protocol
     * Protocol error
     *
     * @since 1.0.0
     */
    eProtocolError = rb_define_class_under(mError, "Protocol", eBaseError);
    /* Document-class: Couchbase::Error::Timeout
     * Timeout error
     *
     * @since 1.1.0
     */
    eTimeoutError = rb_define_class_under(mError, "Timeout", eBaseError);
    /* Document-class: Couchbase::Error::Connect
     * Connect error
     *
     * @since 1.1.0
     */
    eConnectError = rb_define_class_under(mError, "Connect", eBaseError);

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
     * 0x05 :: LCB_EBUSY (Too busy. Try again later)
     * 0x06 :: LCB_EINTERNAL (Internal error)
     * 0x07 :: LCB_EINVAL (Invalid arguments)
     * 0x08 :: LCB_ENOMEM (Out of memory)
     * 0x09 :: LCB_ERANGE (Invalid range)
     * 0x0a :: LCB_ERROR (Generic error)
     * 0x0b :: LCB_ETMPFAIL (Temporary failure. Try again later)
     * 0x0c :: LCB_KEY_EEXISTS (Key exists (with a different CAS value))
     * 0x0d :: LCB_KEY_ENOENT (No such key)
     * 0x0e :: LCB_LIBEVENT_ERROR (Problem using libevent)
     * 0x0f :: LCB_NETWORK_ERROR (Network error)
     * 0x10 :: LCB_NOT_MY_VBUCKET (The vbucket is not located on this server)
     * 0x11 :: LCB_NOT_STORED (Not stored)
     * 0x12 :: LCB_NOT_SUPPORTED (Not supported)
     * 0x13 :: LCB_UNKNOWN_COMMAND (Unknown command)
     * 0x14 :: LCB_UNKNOWN_HOST (Unknown host)
     * 0x15 :: LCB_PROTOCOL_ERROR (Protocol error)
     * 0x16 :: LCB_ETIMEDOUT (Operation timed out)
     * 0x17 :: LCB_CONNECT_ERROR (Connection failure)
     * 0x18 :: LCB_BUCKET_ENOENT (No such bucket)
     * 0x18 :: LCB_CLIENT_ENOMEM (Out of memory on the client)
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the error code from libcouchbase
     */
    rb_define_attr(eBaseError, "error", 1, 0);
    id_iv_error = rb_intern("@error");
    /* Document-method: key
     *
     * @since 1.0.0
     *
     * @return [String] the key which generated error */
    rb_define_attr(eBaseError, "key", 1, 0);
    id_iv_key = rb_intern("@key");
    /* Document-method: cas
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the version of the key (+nil+ unless accessible) */
    rb_define_attr(eBaseError, "cas", 1, 0);
    id_iv_cas = rb_intern("@cas");
    /* Document-method: operation
     *
     * @since 1.0.0
     *
     * @return [Symbol] the operation (+nil+ unless accessible) */
    rb_define_attr(eBaseError, "operation", 1, 0);
    id_iv_operation = rb_intern("@operation");

    /* Document-class: Couchbase::Result
     *
     * The object which yielded to asynchronous callbacks
     *
     * @since 1.0.0
     */
    cResult = rb_define_class_under(mCouchbase, "Result", rb_cObject);
    rb_define_method(cResult, "inspect", cb_result_inspect, 0);
    rb_define_method(cResult, "success?", cb_result_success_p, 0);
    /* Document-method: operation
     *
     * @since 1.0.0
     *
     * @return [Symbol]
     */
    rb_define_attr(cResult, "operation", 1, 0);
    /* Document-method: error
     *
     * @since 1.0.0
     *
     * @return [Couchbase::Error::Base]
     */
    rb_define_attr(cResult, "error", 1, 0);
    /* Document-method: key
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cResult, "key", 1, 0);
    id_iv_key = rb_intern("@key");
    /* Document-method: value
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cResult, "value", 1, 0);
    id_iv_value = rb_intern("@value");
    /* Document-method: cas
     *
     * @since 1.0.0
     *
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "cas", 1, 0);
    id_iv_cas = rb_intern("@cas");
    /* Document-method: flags
     *
     * @since 1.0.0
     *
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "flags", 1, 0);
    id_iv_flags = rb_intern("@flags");
    /* Document-method: node
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cResult, "node", 1, 0);
    id_iv_node = rb_intern("@node");
    /* Document-method: headers
     *
     * @since 1.2.0
     *
     * HTTP headers
     *
     * @return [Hash]
     */
    rb_define_attr(cResult, "headers", 1, 0);
    id_iv_headers = rb_intern("@headers");
    /* Document-method: completed
     * In {Bucket::CouchRequest} operations used to mark the final call
     * @return [Boolean] */
    rb_define_attr(cResult, "completed", 1, 0);
    rb_define_alias(cResult, "completed?", "completed");
    id_iv_completed = rb_intern("@completed");
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
    rb_define_attr(cResult, "status", 1, 0);
    id_iv_status = rb_intern("@status");
    /* Document-method: from_master
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * True if key stored on master
     * @return [Boolean]
     */
    rb_define_attr(cResult, "from_master", 1, 0);
    rb_define_alias(cResult, "from_master?", "from_master");
    id_iv_from_master = rb_intern("@from_master");
    /* Document-method: time_to_persist
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Average time needed to persist key on the disk (zero if unavailable)
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "time_to_persist", 1, 0);
    rb_define_alias(cResult, "ttp", "time_to_persist");
    id_iv_time_to_persist = rb_intern("@time_to_persist");
    /* Document-method: time_to_persist
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Average time needed to replicate key on the disk (zero if unavailable)
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "time_to_replicate", 1, 0);
    rb_define_alias(cResult, "ttr", "time_to_replicate");
    id_iv_time_to_replicate = rb_intern("@time_to_replicate");

    /* Document-class: Couchbase::Bucket
     *
     * This class in charge of all stuff connected to communication with
     * Couchbase.
     *
     * @since 1.0.0
     */
    cBucket = rb_define_class_under(mCouchbase, "Bucket", rb_cObject);

    /* 0x03: Bitmask for flag bits responsible for format */
    rb_define_const(cBucket, "FMT_MASK", INT2FIX(FMT_MASK));
    /* 0x00: Document format. The (default) format supports most of ruby
     * types which could be mapped to JSON data (hashes, arrays, strings,
     * numbers). Future version will be able to run map/reduce queries on
     * the values in the document form (hashes). */
    rb_define_const(cBucket, "FMT_DOCUMENT", INT2FIX(FMT_DOCUMENT));
    /* 0x01:  Marshal format. The format which supports transparent
     * serialization of ruby objects with standard <tt>Marshal.dump</tt> and
     * <tt>Marhal.load</tt> methods. */
    rb_define_const(cBucket, "FMT_MARSHAL", INT2FIX(FMT_MARSHAL));
    /* 0x02:  Plain format. The format which force client don't apply any
     * conversions to the value, but it should be passed as String. It
     * could be useful for building custom algorithms or formats. For
     * example implement set:
     * http://dustin.github.com/2011/02/17/memcached-set.html */
    rb_define_const(cBucket, "FMT_PLAIN", INT2FIX(FMT_PLAIN));

    rb_define_alloc_func(cBucket, cb_bucket_alloc);
    rb_define_method(cBucket, "initialize", cb_bucket_init, -1);
    rb_define_method(cBucket, "initialize_copy", cb_bucket_init_copy, 1);
    rb_define_method(cBucket, "inspect", cb_bucket_inspect, 0);

    rb_define_method(cBucket, "add", cb_bucket_add, -1);
    rb_define_method(cBucket, "append", cb_bucket_append, -1);
    rb_define_method(cBucket, "prepend", cb_bucket_prepend, -1);
    rb_define_method(cBucket, "replace", cb_bucket_replace, -1);
    rb_define_method(cBucket, "set", cb_bucket_set, -1);
    rb_define_method(cBucket, "get", cb_bucket_get, -1);
    rb_define_method(cBucket, "run", cb_bucket_run, -1);
    rb_define_method(cBucket, "stop", cb_bucket_stop, 0);
    rb_define_method(cBucket, "touch", cb_bucket_touch, -1);
    rb_define_method(cBucket, "delete", cb_bucket_delete, -1);
    rb_define_method(cBucket, "stats", cb_bucket_stats, -1);
    rb_define_method(cBucket, "flush", cb_bucket_flush, -1);
    rb_define_method(cBucket, "version", cb_bucket_version, -1);
    rb_define_method(cBucket, "incr", cb_bucket_incr, -1);
    rb_define_method(cBucket, "decr", cb_bucket_decr, -1);
    rb_define_method(cBucket, "disconnect", cb_bucket_disconnect, 0);
    rb_define_method(cBucket, "reconnect", cb_bucket_reconnect, -1);
    rb_define_method(cBucket, "make_http_request", cb_bucket_make_http_request, -1);
    rb_define_method(cBucket, "observe", cb_bucket_observe, -1);

    rb_define_alias(cBucket, "decrement", "decr");
    rb_define_alias(cBucket, "increment", "incr");

    rb_define_alias(cBucket, "[]", "get");
    rb_define_alias(cBucket, "[]=", "set");
    rb_define_method(cBucket, "[]=", cb_bucket_aset, -1);

    rb_define_method(cBucket, "connected?", cb_bucket_connected_p, 0);
    rb_define_method(cBucket, "async?", cb_bucket_async_p, 0);

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
    /* rb_define_attr(cBucket, "quiet", 1, 1); */
    rb_define_method(cBucket, "quiet", cb_bucket_quiet_get, 0);
    rb_define_method(cBucket, "quiet=", cb_bucket_quiet_set, 1);
    rb_define_alias(cBucket, "quiet?", "quiet");

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
    /* rb_define_attr(cBucket, "default_flags", 1, 1); */
    rb_define_method(cBucket, "default_flags", cb_bucket_default_flags_get, 0);
    rb_define_method(cBucket, "default_flags=", cb_bucket_default_flags_set, 1);

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
     * @example Selecting plain format using symbol
     *   connection.format = :document
     *
     * @example Selecting plain format using Fixnum constant
     *   connection.format = Couchbase::Bucket::FMT_PLAIN
     *
     * @note Amending default_format will also change #default_flags value
     *
     * @return [Symbol] the effective format */
    /* rb_define_attr(cBucket, "default_format", 1, 1); */
    rb_define_method(cBucket, "default_format", cb_bucket_default_format_get, 0);
    rb_define_method(cBucket, "default_format=", cb_bucket_default_format_set, 1);

    /* Document-method: timeout
     *
     * @since 1.1.0
     *
     * @return [Fixnum] The timeout for the operations in microseconds. The
     *   client will raise {Couchbase::Error::Timeout} exception for all
     *   commands which weren't completed in given timeslot. */
    /* rb_define_attr(cBucket, "timeout", 1, 1); */
    rb_define_method(cBucket, "timeout", cb_bucket_timeout_get, 0);
    rb_define_method(cBucket, "timeout=", cb_bucket_timeout_set, 1);

    /* Document-method: key_prefix
     *
     * @since 1.2.0.dp5
     *
     * @return [String] The library will prepend +key_prefix+ to each key to
     *   provide simple namespacing. */
    /* rb_define_attr(cBucket, "key_prefix", 1, 1); */
    rb_define_method(cBucket, "key_prefix", cb_bucket_key_prefix_get, 0);
    rb_define_method(cBucket, "key_prefix=", cb_bucket_key_prefix_set, 1);

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
    /* rb_define_attr(cBucket, "on_error", 1, 1); */
    rb_define_method(cBucket, "on_error", cb_bucket_on_error_get, 0);
    rb_define_method(cBucket, "on_error=", cb_bucket_on_error_set, 1);

    /* Document-method: url
     *
     * The config url for this connection.
     *
     * Generally it is the bootstrap URL, but it could be different after
     * cluster upgrade. This url is used to fetch the cluster
     * configuration.
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "url", 1, 0); */
    rb_define_method(cBucket, "url", cb_bucket_url_get, 0);
    /* Document-method: hostname
     *
     * The hostname of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "hostname", 1, 0); */
    rb_define_method(cBucket, "hostname", cb_bucket_hostname_get, 0);
    /* Document-method: port
     *
     * The port of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "port", 1, 0); */
    rb_define_method(cBucket, "port", cb_bucket_port_get, 0);
    /* Document-method: authority
     *
     * The authority ("hostname:port") of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "authority", 1, 0); */
    rb_define_method(cBucket, "authority", cb_bucket_authority_get, 0);
    /* Document-method: bucket
     *
     * The bucket name of the current connection
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "bucket", 1, 0); */
    rb_define_method(cBucket, "bucket", cb_bucket_bucket_get, 0);
    rb_define_alias(cBucket, "name", "bucket");
    /* Document-method: pool
     *
     * The pool name of the current connection
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "pool", 1, 0); */
    rb_define_method(cBucket, "pool", cb_bucket_pool_get, 0);
    /* Document-method: username
     *
     * The user name used to connect to the cluster
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "username", 1, 0); */
    rb_define_method(cBucket, "username", cb_bucket_username_get, 0);
    /* Document-method: password
     *
     * The password used to connect to the cluster
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "password", 1, 0); */
    rb_define_method(cBucket, "password", cb_bucket_password_get, 0);
    /* Document-method: environment
     *
     * The environment of the connection (+:development+ or +:production+)
     *
     * @since 1.2.0
     *
     * @returns [Symbol]
     */
    /* rb_define_attr(cBucket, "environment", 1, 0); */
    rb_define_method(cBucket, "environment", cb_bucket_environment_get, 0);
    /* Document-method: num_replicas
     *
     * @since 1.2.0.dp6
     *
     * The numbers of the replicas for each node in the cluster
     *
     * @returns [Fixnum]
     */
    /* rb_define_attr(cBucket, "num_replicas", 1, 0); */
    rb_define_method(cBucket, "num_replicas", cb_bucket_num_replicas_get, 0);
    /* Document-method: default_observe_timeout
     *
     * @since 1.2.0.dp6
     *
     * The default timeout value for {Bucket#observe_and_wait} operation in
     * microseconds
     *
     * @returns [Fixnum]
     */
    /* rb_define_attr(cBucket, "default_observe_timeout", 1, 1); */
    rb_define_method(cBucket, "default_observe_timeout", cb_bucket_default_observe_timeout_get, 0);
    rb_define_method(cBucket, "default_observe_timeout=", cb_bucket_default_observe_timeout_set, 1);

    cCouchRequest = rb_define_class_under(cBucket, "CouchRequest", rb_cObject);
    rb_define_alloc_func(cCouchRequest, cb_http_request_alloc);

    rb_define_method(cCouchRequest, "initialize", cb_http_request_init, -1);
    rb_define_method(cCouchRequest, "inspect", cb_http_request_inspect, 0);
    rb_define_method(cCouchRequest, "on_body", cb_http_request_on_body, 0);
    rb_define_method(cCouchRequest, "perform", cb_http_request_perform, 0);
    rb_define_method(cCouchRequest, "pause", cb_http_request_pause, 0);
    rb_define_method(cCouchRequest, "continue", cb_http_request_continue, 0);

    /* rb_define_attr(cCouchRequest, "path", 1, 0); */
    rb_define_method(cCouchRequest, "path", cb_http_request_path_get, 0);
    /* rb_define_attr(cCouchRequest, "extended", 1, 0); */
    rb_define_method(cCouchRequest, "extended", cb_http_request_extended_get, 0);
    rb_define_alias(cCouchRequest, "extended?", "extended");
    /* rb_define_attr(cCouchRequest, "chunked", 1, 0); */
    rb_define_method(cCouchRequest, "chunked", cb_http_request_chunked_get, 0);
    rb_define_alias(cCouchRequest, "chunked?", "chunked");

    cTimer = rb_define_class_under(mCouchbase, "Timer", rb_cObject);
    rb_define_alloc_func(cTimer, cb_timer_alloc);
    rb_define_method(cTimer, "initialize", cb_timer_init, -1);
    rb_define_method(cTimer, "inspect", cb_timer_inspect, 0);
    rb_define_method(cTimer, "cancel", cb_timer_cancel, 0);

    /* Define symbols */
    id_arity = rb_intern("arity");
    id_call = rb_intern("call");
    id_delete = rb_intern("delete");
    id_dump = rb_intern("dump");
    id_dup = rb_intern("dup");
    id_flatten_bang = rb_intern("flatten!");
    id_has_key_p = rb_intern("has_key?");
    id_host = rb_intern("host");
    id_load = rb_intern("load");
    id_match = rb_intern("match");
    id_observe_and_wait = rb_intern("observe_and_wait");
    id_parse = rb_intern("parse");
    id_password = rb_intern("password");
    id_path = rb_intern("path");
    id_port = rb_intern("port");
    id_scheme = rb_intern("scheme");
    id_to_s = rb_intern("to_s");
    id_user = rb_intern("user");
    id_verify_observe_options = rb_intern("verify_observe_options");

    sym_add = ID2SYM(rb_intern("add"));
    sym_append = ID2SYM(rb_intern("append"));
    sym_assemble_hash = ID2SYM(rb_intern("assemble_hash"));
    sym_body = ID2SYM(rb_intern("body"));
    sym_bucket = ID2SYM(rb_intern("bucket"));
    sym_cas = ID2SYM(rb_intern("cas"));
    sym_chunked = ID2SYM(rb_intern("chunked"));
    sym_content_type = ID2SYM(rb_intern("content_type"));
    sym_create = ID2SYM(rb_intern("create"));
    sym_decrement = ID2SYM(rb_intern("decrement"));
    sym_default_flags = ID2SYM(rb_intern("default_flags"));
    sym_default_format = ID2SYM(rb_intern("default_format"));
    sym_default_ttl = ID2SYM(rb_intern("default_ttl"));
    sym_delete = ID2SYM(rb_intern("delete"));
    sym_delta = ID2SYM(rb_intern("delta"));
    sym_development = ID2SYM(rb_intern("development"));
    sym_document = ID2SYM(rb_intern("document"));
    sym_environment = ID2SYM(rb_intern("environment"));
    sym_extended = ID2SYM(rb_intern("extended"));
    sym_flags = ID2SYM(rb_intern("flags"));
    sym_flush = ID2SYM(rb_intern("flush"));
    sym_format = ID2SYM(rb_intern("format"));
    sym_found = ID2SYM(rb_intern("found"));
    sym_get = ID2SYM(rb_intern("get"));
    sym_hostname = ID2SYM(rb_intern("hostname"));
    sym_http_request = ID2SYM(rb_intern("http_request"));
    sym_increment = ID2SYM(rb_intern("increment"));
    sym_initial = ID2SYM(rb_intern("initial"));
    sym_key_prefix = ID2SYM(rb_intern("key_prefix"));
    sym_lock = ID2SYM(rb_intern("lock"));
    sym_management = ID2SYM(rb_intern("management"));
    sym_marshal = ID2SYM(rb_intern("marshal"));
    sym_method = ID2SYM(rb_intern("method"));
    sym_node_list = ID2SYM(rb_intern("node_list"));
    sym_not_found = ID2SYM(rb_intern("not_found"));
    sym_num_replicas = ID2SYM(rb_intern("num_replicas"));
    sym_observe = ID2SYM(rb_intern("observe"));
    sym_password = ID2SYM(rb_intern("password"));
    sym_periodic = ID2SYM(rb_intern("periodic"));
    sym_persisted = ID2SYM(rb_intern("persisted"));
    sym_plain = ID2SYM(rb_intern("plain"));
    sym_pool = ID2SYM(rb_intern("pool"));
    sym_port = ID2SYM(rb_intern("port"));
    sym_post = ID2SYM(rb_intern("post"));
    sym_prepend = ID2SYM(rb_intern("prepend"));
    sym_production = ID2SYM(rb_intern("production"));
    sym_put = ID2SYM(rb_intern("put"));
    sym_quiet = ID2SYM(rb_intern("quiet"));
    sym_replace = ID2SYM(rb_intern("replace"));
    sym_replica = ID2SYM(rb_intern("replica"));
    sym_send_threshold = ID2SYM(rb_intern("send_threshold"));
    sym_set = ID2SYM(rb_intern("set"));
    sym_stats = ID2SYM(rb_intern("stats"));
    sym_timeout = ID2SYM(rb_intern("timeout"));
    sym_touch = ID2SYM(rb_intern("touch"));
    sym_ttl = ID2SYM(rb_intern("ttl"));
    sym_type = ID2SYM(rb_intern("type"));
    sym_username = ID2SYM(rb_intern("username"));
    sym_version = ID2SYM(rb_intern("version"));
    sym_view = ID2SYM(rb_intern("view"));
}
